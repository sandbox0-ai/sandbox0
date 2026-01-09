package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	"github.com/sandbox0-ai/infra/manager/pkg/db"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// SandboxService handles sandbox operations
type SandboxService struct {
	k8sClient                   kubernetes.Interface
	podLister                   corelisters.PodLister
	templateLister              controller.TemplateLister
	SandboxNetworkPolicyService *SandboxNetworkPolicyService
	procdClient                 *ProcdClient
	tokenGenerator              TokenGenerator
	logger                      *zap.Logger
}

// TokenGenerator generates internal tokens for procd authentication.
type TokenGenerator interface {
	GenerateToken(teamID, sandboxID string) (string, error)
}

// NewSandboxService creates a new SandboxService
func NewSandboxService(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	templateLister controller.TemplateLister,
	SandboxNetworkPolicyService *SandboxNetworkPolicyService,
	tokenGenerator TokenGenerator,
	logger *zap.Logger,
) *SandboxService {
	return &SandboxService{
		k8sClient:                   k8sClient,
		podLister:                   podLister,
		templateLister:              templateLister,
		SandboxNetworkPolicyService: SandboxNetworkPolicyService,
		procdClient:                 NewProcdClient(),
		tokenGenerator:              tokenGenerator,
		logger:                      logger,
	}
}

// ClaimRequest represents a sandbox claim request
type ClaimRequest struct {
	Namespace  string         `json:"namespace"`
	TemplateID string         `json:"template_id"`
	TeamID     string         `json:"team_id"`
	UserID     string         `json:"user_id"`
	SandboxID  string         `json:"sandbox_id"`
	Config     *SandboxConfig `json:"config,omitempty"`
}

// SandboxConfig represents sandbox configuration
type SandboxConfig struct {
	EnvVars map[string]string                 `json:"env_vars,omitempty"`
	TTL     int32                             `json:"ttl,omitempty"` // Time-to-live in seconds
	Network *v1alpha1.TplSandboxNetworkPolicy `json:"network,omitempty"`
}

// ClaimResponse represents a sandbox claim response
type ClaimResponse struct {
	SandboxID    string `json:"sandbox_id"`
	TemplateID   string `json:"template_id"`
	Status       string `json:"status"`
	ProcdAddress string `json:"procd_address"`
	PodName      string `json:"pod_name"`
	Namespace    string `json:"namespace"`
}

// ClaimSandbox claims a sandbox from the idle pool or creates a new one
func (s *SandboxService) ClaimSandbox(ctx context.Context, req *ClaimRequest) (*ClaimResponse, error) {
	s.logger.Info("Claiming sandbox",
		zap.String("templateID", req.TemplateID),
		zap.String("sandboxID", req.SandboxID),
		zap.String("teamID", req.TeamID),
	)

	// Get the template
	template, err := s.templateLister.Get(req.Namespace, req.TemplateID)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("template %s not found in namespace %s", req.TemplateID, req.Namespace)
		}
		return nil, fmt.Errorf("get template: %w", err)
	}

	// Try to claim an idle pod first
	pod, err := s.claimIdlePod(ctx, template, req)
	if err != nil {
		return nil, fmt.Errorf("claim idle pod: %w", err)
	}

	// If no idle pod available, create a new one (cold start)
	if pod == nil {
		s.logger.Info("No idle pod available, creating new pod",
			zap.String("templateID", req.TemplateID),
		)
		pod, err = s.createNewPod(ctx, template, req)
		if err != nil {
			return nil, fmt.Errorf("create new pod: %w", err)
		}
	}

	// Create network and bandwidth policies for the sandbox
	if s.SandboxNetworkPolicyService != nil {
		// Create network policy
		var requestNetwork *v1alpha1.TplSandboxNetworkPolicy
		if req.Config != nil {
			requestNetwork = req.Config.Network
		}

		if err := s.SandboxNetworkPolicyService.CreateOrUpdateSandboxNetworkPolicy(ctx, &CreateSandboxNetworkPolicyRequest{
			SandboxID:    req.SandboxID,
			TeamID:       req.TeamID,
			Namespace:    pod.Namespace,
			TemplateSpec: template.Spec.Network,
			RequestSpec:  requestNetwork,
		}); err != nil {
			s.logger.Error("Failed to create network policy",
				zap.String("sandboxID", req.SandboxID),
				zap.Error(err),
			)
			// Don't fail the claim, but log the error
		}

		// Create bandwidth policy with defaults
		if err := s.SandboxNetworkPolicyService.CreateOrUpdateBandwidthPolicy(ctx, &CreateBandwidthPolicyRequest{
			SandboxID:         req.SandboxID,
			TeamID:            req.TeamID,
			Namespace:         pod.Namespace,
			EgressRateBps:     100 * 1000 * 1000, // 100 Mbps
			IngressRateBps:    100 * 1000 * 1000, // 100 Mbps
			AccountingEnabled: true,
		}); err != nil {
			s.logger.Error("Failed to create bandwidth policy",
				zap.String("sandboxID", req.SandboxID),
				zap.Error(err),
			)
			// Don't fail the claim, but log the error
		}
	}

	return &ClaimResponse{
		SandboxID:    req.SandboxID,
		TemplateID:   req.TemplateID,
		Status:       "starting",
		ProcdAddress: s.prodAddress(pod.Name, pod.Namespace),
		PodName:      pod.Name,
		Namespace:    pod.Namespace,
	}, nil
}

// claimIdlePod claims an idle pod from the pool
func (s *SandboxService) claimIdlePod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	// Get all idle pods for this template
	pods, err := s.podLister.Pods(template.ObjectMeta.Namespace).List(labels.SelectorFromSet(map[string]string{
		controller.LabelTemplateID: template.ObjectMeta.Name,
		controller.LabelPoolType:   controller.PoolTypeIdle,
	}))
	if err != nil {
		return nil, err
	}

	// Filter running pods
	var runningPods []*corev1.Pod
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning {
			runningPods = append(runningPods, pod)
		}
	}

	if len(runningPods) == 0 {
		return nil, nil // No idle pod available
	}

	// Claim the first available pod
	pod := runningPods[0]

	s.logger.Info("Claiming idle pod",
		zap.String("pod", pod.Name),
		zap.String("sandboxID", req.SandboxID),
	)

	// Update pod labels and annotations
	pod = pod.DeepCopy()

	// Change pool type from idle to active
	pod.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	pod.Labels[controller.LabelSandboxID] = req.SandboxID

	// Remove owner reference (so it's no longer managed by ReplicaSet)
	pod.OwnerReferences = nil

	// Add annotations
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[controller.AnnotationTeamID] = req.TeamID
	pod.Annotations[controller.AnnotationUserID] = req.UserID
	pod.Annotations[controller.AnnotationClaimedAt] = time.Now().Format(time.RFC3339)

	// Set expiration time
	ttl := int32(3600) // Default 1 hour
	if req.Config != nil && req.Config.TTL > 0 {
		ttl = req.Config.TTL
	}
	expiresAt := time.Now().Add(time.Duration(ttl) * time.Second)
	pod.Annotations[controller.AnnotationExpiresAt] = expiresAt.Format(time.RFC3339)

	// Serialize config
	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return nil, fmt.Errorf("marshal config: %w", err)
		}
		pod.Annotations[controller.AnnotationConfig] = string(configJSON)
	}

	// Update the pod
	updatedPod, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update pod: %w", err)
	}

	s.logger.Info("Successfully claimed idle pod",
		zap.String("pod", updatedPod.Name),
		zap.String("sandboxID", req.SandboxID),
		zap.Time("expiresAt", expiresAt),
	)

	return updatedPod, nil
}

// createNewPod creates a new pod for cold start
func (s *SandboxService) createNewPod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	suffix := req.SandboxID
	if len(suffix) >= 8 {
		suffix = suffix[:8]
	}
	podName := fmt.Sprintf("%s-%s", req.TemplateID, suffix)

	// Build pod spec from template
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: template.ObjectMeta.Namespace,
			Labels: map[string]string{
				controller.LabelTemplateID: template.ObjectMeta.Name,
				controller.LabelPoolType:   controller.PoolTypeActive,
				controller.LabelSandboxID:  req.SandboxID,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:    req.TeamID,
				controller.AnnotationUserID:    req.UserID,
				controller.AnnotationClaimedAt: time.Now().Format(time.RFC3339),
			},
		},
		Spec: s.buildPodSpec(template),
	}

	// Set expiration time
	ttl := int32(300) // Default 5 minutes
	if req.Config != nil && req.Config.TTL > 0 {
		ttl = req.Config.TTL
	}
	expiresAt := time.Now().Add(time.Duration(ttl) * time.Second)
	pod.Annotations[controller.AnnotationExpiresAt] = expiresAt.Format(time.RFC3339)

	// Serialize config
	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return nil, fmt.Errorf("marshal config: %w", err)
		}
		pod.Annotations[controller.AnnotationConfig] = string(configJSON)
	}

	// Create the pod
	createdPod, err := s.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}

	s.logger.Info("Created new pod for cold start",
		zap.String("pod", createdPod.Name),
		zap.String("sandboxID", req.SandboxID),
		zap.Time("expiresAt", expiresAt),
	)

	return createdPod, nil
}

// buildPodSpec builds a pod spec from a template
func (s *SandboxService) buildPodSpec(template *v1alpha1.SandboxTemplate) corev1.PodSpec {
	spec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    s.buildContainers(template),
	}

	// Apply runtime class if specified
	if template.Spec.RuntimeClassName != nil {
		spec.RuntimeClassName = template.Spec.RuntimeClassName
	}

	// Apply pod-level overrides
	if template.Spec.Pod != nil {
		if template.Spec.Pod.NodeSelector != nil {
			spec.NodeSelector = template.Spec.Pod.NodeSelector
		}
		if template.Spec.Pod.ServiceAccountName != "" {
			spec.ServiceAccountName = template.Spec.Pod.ServiceAccountName
		}
	}

	return spec
}

// buildContainers builds containers from template
func (s *SandboxService) buildContainers(template *v1alpha1.SandboxTemplate) []corev1.Container {
	containers := []corev1.Container{
		s.buildContainer(&template.Spec.MainContainer, template, true),
	}

	for i := range template.Spec.Sidecars {
		containers = append(containers, s.buildContainer(&template.Spec.Sidecars[i], template, false))
	}

	return containers
}

// buildContainer builds a single container
func (s *SandboxService) buildContainer(spec *v1alpha1.ContainerSpec, template *v1alpha1.SandboxTemplate, isMain bool) corev1.Container {
	name := "procd"
	if !isMain {
		name = fmt.Sprintf("sidecar-%s", spec.Image)
	}

	container := corev1.Container{
		Name:            name,
		Image:           spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         spec.Command,
		Args:            spec.Args,
	}

	if spec.ImagePullPolicy != "" {
		container.ImagePullPolicy = corev1.PullPolicy(spec.ImagePullPolicy)
	}

	// Environment variables
	var envVars []corev1.EnvVar
	for k, v := range template.Spec.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}
	for _, ev := range spec.Env {
		envVars = append(envVars, corev1.EnvVar{Name: ev.Name, Value: ev.Value})
	}
	container.Env = envVars

	// Security context
	if spec.SecurityContext != nil {
		container.SecurityContext = &corev1.SecurityContext{}
		if spec.SecurityContext.RunAsUser != nil {
			container.SecurityContext.RunAsUser = spec.SecurityContext.RunAsUser
		}
		if spec.SecurityContext.RunAsGroup != nil {
			container.SecurityContext.RunAsGroup = spec.SecurityContext.RunAsGroup
		}
		if spec.SecurityContext.Capabilities != nil {
			container.SecurityContext.Capabilities = &corev1.Capabilities{
				Drop: convertCapabilities(spec.SecurityContext.Capabilities.Drop),
			}
		}
	}

	return container
}

func convertCapabilities(caps []string) []corev1.Capability {
	if caps == nil {
		return nil
	}
	result := make([]corev1.Capability, len(caps))
	for i, cap := range caps {
		result[i] = corev1.Capability(cap)
	}
	return result
}

// TerminateSandbox terminates a sandbox
func (s *SandboxService) TerminateSandbox(ctx context.Context, sandboxID string) error {
	s.logger.Info("Terminating sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pods, err := s.podLister.Pods("").List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}

	if len(pods) == 0 {
		return fmt.Errorf("sandbox not found: %s", sandboxID)
	}

	pod := pods[0]

	// Delete network and bandwidth policies
	if s.SandboxNetworkPolicyService != nil {
		if err := s.SandboxNetworkPolicyService.DeleteSandboxNetworkPolicy(ctx, pod.Namespace, sandboxID); err != nil {
			s.logger.Warn("Failed to delete network policy",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		}

		if err := s.SandboxNetworkPolicyService.DeleteBandwidthPolicy(ctx, pod.Namespace, sandboxID); err != nil {
			s.logger.Warn("Failed to delete bandwidth policy",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		}
	}

	// Delete the pod
	err = s.k8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete pod: %w", err)
	}

	s.logger.Info("Sandbox terminated", zap.String("sandboxID", sandboxID), zap.String("pod", pod.Name))

	return nil
}

// GetSandbox gets a sandbox by ID
func (s *SandboxService) GetSandbox(ctx context.Context, sandboxID string) (*db.Sandbox, error) {
	// Find the pod by sandbox ID
	pods, err := s.podLister.Pods("").List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	if len(pods) == 0 {
		return nil, fmt.Errorf("sandbox not found: %s", sandboxID)
	}

	pod := pods[0]
	return s.podToSandbox(pod, sandboxID), nil
}

// ListSandboxes lists all sandboxes for a team
func (s *SandboxService) ListSandboxes(ctx context.Context, teamID string) ([]*db.Sandbox, error) {
	// List all pods with active pool type (claimed sandboxes)
	pods, err := s.podLister.Pods("").List(labels.SelectorFromSet(map[string]string{
		controller.LabelPoolType: controller.PoolTypeActive,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	var sandboxes []*db.Sandbox
	for _, pod := range pods {
		// Filter by team ID if specified
		if teamID != "" && pod.Annotations[controller.AnnotationTeamID] != teamID {
			continue
		}

		sandboxID := pod.Labels[controller.LabelSandboxID]
		if sandboxID != "" {
			sandboxes = append(sandboxes, s.podToSandbox(pod, sandboxID))
		}
	}

	return sandboxes, nil
}

// podToSandbox converts a pod to a sandbox object
func (s *SandboxService) podToSandbox(pod *corev1.Pod, sandboxID string) *db.Sandbox {
	status := s.podPhaseToSandboxStatus(pod.Status.Phase)

	// Parse timestamps
	var claimedAt, expiresAt, createdAt time.Time
	if claimedAtStr := pod.Annotations[controller.AnnotationClaimedAt]; claimedAtStr != "" {
		claimedAt, _ = time.Parse(time.RFC3339, claimedAtStr)
	}
	if expiresAtStr := pod.Annotations[controller.AnnotationExpiresAt]; expiresAtStr != "" {
		expiresAt, _ = time.Parse(time.RFC3339, expiresAtStr)
	}
	createdAt = pod.CreationTimestamp.Time

	return &db.Sandbox{
		ID:           sandboxID,
		TemplateID:   pod.Labels[controller.LabelTemplateID],
		TeamID:       pod.Annotations[controller.AnnotationTeamID],
		UserID:       pod.Annotations[controller.AnnotationUserID],
		ProcdAddress: s.prodAddress(pod.Name, pod.Namespace),
		Status:       status,
		PodName:      pod.Name,
		Namespace:    pod.Namespace,
		ExpiresAt:    expiresAt,
		ClaimedAt:    claimedAt,
		CreatedAt:    createdAt,
	}
}

func (s *SandboxService) prodAddress(name, namespace string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:49983", name, namespace)
}

// podPhaseToSandboxStatus converts pod phase to sandbox status
func (s *SandboxService) podPhaseToSandboxStatus(phase corev1.PodPhase) string {
	switch phase {
	case corev1.PodPending:
		return db.SandboxStatusStarting
	case corev1.PodRunning:
		return db.SandboxStatusRunning
	case corev1.PodSucceeded:
		return db.SandboxStatusCompleted
	case corev1.PodFailed:
		return db.SandboxStatusFailed
	default:
		return db.SandboxStatusPending
	}
}

// GetSandboxStatus gets the status of a sandbox
func (s *SandboxService) GetSandboxStatus(ctx context.Context, sandboxID string) (map[string]interface{}, error) {
	sandbox, err := s.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}

	status := map[string]interface{}{
		"sandbox_id":    sandbox.ID,
		"template_id":   sandbox.TemplateID,
		"team_id":       sandbox.TeamID,
		"user_id":       sandbox.UserID,
		"pod_name":      sandbox.PodName,
		"namespace":     sandbox.Namespace,
		"status":        sandbox.Status,
		"procd_address": sandbox.ProcdAddress,
		"claimed_at":    sandbox.ClaimedAt.Format(time.RFC3339),
		"expires_at":    sandbox.ExpiresAt.Format(time.RFC3339),
		"created_at":    sandbox.CreatedAt.Format(time.RFC3339),
	}

	return status, nil
}

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                `json:"sandbox_id"`
	Paused        bool                  `json:"paused"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                `json:"updated_memory,omitempty"`
}

// ResumeSandboxResponse represents the response from resuming a sandbox.
type ResumeSandboxResponse struct {
	SandboxID      string `json:"sandbox_id"`
	Resumed        bool   `json:"resumed"`
	RestoredMemory string `json:"restored_memory,omitempty"`
}

// OriginalResources stores original pod resources before pause.
type OriginalResources struct {
	Containers map[string]ContainerResources `json:"containers"`
}

// ContainerResources stores resource requests/limits for a container.
type ContainerResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

// PauseSandbox pauses a sandbox and reduces pod resources based on actual usage.
// This uses Kubernetes 1.35+ in-place pod update feature.
func (s *SandboxService) PauseSandbox(ctx context.Context, sandboxID string) (*PauseSandboxResponse, error) {
	s.logger.Info("Pausing sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pods, err := s.podLister.Pods("").List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	if len(pods) == 0 {
		return nil, fmt.Errorf("sandbox not found: %s", sandboxID)
	}

	pod := pods[0]

	// Check if already paused
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return nil, fmt.Errorf("sandbox is already paused")
	}

	// Generate internal token for procd authentication
	if s.tokenGenerator == nil {
		return nil, fmt.Errorf("token generator not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	token, err := s.tokenGenerator.GenerateToken(teamID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	// Call procd pause API
	procdAddress := s.prodAddress(pod.Name, pod.Namespace)
	pauseResp, err := s.procdClient.Pause(ctx, procdAddress, token)
	if err != nil {
		return nil, fmt.Errorf("call procd pause: %w", err)
	}

	if !pauseResp.Paused {
		return nil, fmt.Errorf("procd pause failed: %s", pauseResp.Error)
	}

	// Save original resources before updating
	originalResources := s.extractOriginalResources(pod)
	originalResourcesJSON, err := json.Marshal(originalResources)
	if err != nil {
		return nil, fmt.Errorf("marshal original resources: %w", err)
	}

	// Calculate new memory request based on working set + buffer
	var newMemory resource.Quantity
	if pauseResp.ResourceUsage != nil && pauseResp.ResourceUsage.ContainerMemoryWorkingSet > 0 {
		// Add 20% buffer to working set for safety
		workingSet := pauseResp.ResourceUsage.ContainerMemoryWorkingSet
		newMemoryBytes := int64(float64(workingSet) * 1.2)
		// Minimum 64Mi to avoid too aggressive scaling
		if newMemoryBytes < 64*1024*1024 {
			newMemoryBytes = 64 * 1024 * 1024
		}
		newMemory = *resource.NewQuantity(newMemoryBytes, resource.BinarySI)
	}

	// Update pod with reduced resources (in-place update)
	podCopy := pod.DeepCopy()

	// Update annotations
	if podCopy.Annotations == nil {
		podCopy.Annotations = make(map[string]string)
	}
	podCopy.Annotations[controller.AnnotationPaused] = "true"
	podCopy.Annotations[controller.AnnotationPausedAt] = time.Now().Format(time.RFC3339)
	podCopy.Annotations[controller.AnnotationOriginalResources] = string(originalResourcesJSON)

	// Update container resources if we have a valid new memory size
	if !newMemory.IsZero() {
		for i := range podCopy.Spec.Containers {
			container := &podCopy.Spec.Containers[i]
			if container.Resources.Requests == nil {
				container.Resources.Requests = make(corev1.ResourceList)
			}
			container.Resources.Requests[corev1.ResourceMemory] = newMemory
			// Also update limits if set
			if container.Resources.Limits != nil {
				if _, hasMemLimit := container.Resources.Limits[corev1.ResourceMemory]; hasMemLimit {
					container.Resources.Limits[corev1.ResourceMemory] = newMemory
				}
			}
		}
	}

	// Apply the update
	_, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, podCopy, metav1.UpdateOptions{})
	if err != nil {
		s.logger.Error("Failed to update pod resources after pause",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		// Don't fail the pause operation, just log the error
		// The sandbox is still paused in procd
	}

	s.logger.Info("Sandbox paused successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("newMemory", newMemory.String()),
		zap.Int64("workingSet", pauseResp.ResourceUsage.ContainerMemoryWorkingSet),
	)

	return &PauseSandboxResponse{
		SandboxID:     sandboxID,
		Paused:        true,
		ResourceUsage: pauseResp.ResourceUsage,
		UpdatedMemory: newMemory.String(),
	}, nil
}

// ResumeSandbox resumes a paused sandbox and restores original pod resources.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	s.logger.Info("Resuming sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pods, err := s.podLister.Pods("").List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	if len(pods) == 0 {
		return nil, fmt.Errorf("sandbox not found: %s", sandboxID)
	}

	pod := pods[0]

	// Check if paused
	if pod.Annotations[controller.AnnotationPaused] != "true" {
		return nil, fmt.Errorf("sandbox is not paused")
	}

	// Restore original resources first (before resuming processes)
	var restoredMemory string
	originalResourcesJSON := pod.Annotations[controller.AnnotationOriginalResources]
	if originalResourcesJSON != "" {
		var originalResources OriginalResources
		if err := json.Unmarshal([]byte(originalResourcesJSON), &originalResources); err == nil {
			podCopy := pod.DeepCopy()

			for i := range podCopy.Spec.Containers {
				container := &podCopy.Spec.Containers[i]
				if orig, ok := originalResources.Containers[container.Name]; ok {
					container.Resources.Requests = orig.Requests
					container.Resources.Limits = orig.Limits
					if memReq, ok := orig.Requests[corev1.ResourceMemory]; ok {
						restoredMemory = memReq.String()
					}
				}
			}

			// Remove pause annotations
			delete(podCopy.Annotations, controller.AnnotationPaused)
			delete(podCopy.Annotations, controller.AnnotationPausedAt)
			delete(podCopy.Annotations, controller.AnnotationOriginalResources)

			// Apply the update
			_, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, podCopy, metav1.UpdateOptions{})
			if err != nil {
				s.logger.Error("Failed to restore pod resources before resume",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				// Continue with resume anyway
			}
		}
	}

	// Generate internal token for procd authentication
	if s.tokenGenerator == nil {
		return nil, fmt.Errorf("token generator not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	token, err := s.tokenGenerator.GenerateToken(teamID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	// Call procd resume API
	procdAddress := s.prodAddress(pod.Name, pod.Namespace)
	resumeResp, err := s.procdClient.Resume(ctx, procdAddress, token)
	if err != nil {
		return nil, fmt.Errorf("call procd resume: %w", err)
	}

	if !resumeResp.Resumed {
		return nil, fmt.Errorf("procd resume failed: %s", resumeResp.Error)
	}

	s.logger.Info("Sandbox resumed successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("restoredMemory", restoredMemory),
	)

	return &ResumeSandboxResponse{
		SandboxID:      sandboxID,
		Resumed:        true,
		RestoredMemory: restoredMemory,
	}, nil
}

// GetSandboxResourceUsage gets the resource usage of a sandbox.
func (s *SandboxService) GetSandboxResourceUsage(ctx context.Context, sandboxID string) (*SandboxResourceUsage, error) {
	// Find the pod by sandbox ID
	pods, err := s.podLister.Pods("").List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	if len(pods) == 0 {
		return nil, fmt.Errorf("sandbox not found: %s", sandboxID)
	}

	pod := pods[0]

	// Generate internal token for procd authentication
	if s.tokenGenerator == nil {
		return nil, fmt.Errorf("token generator not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	token, err := s.tokenGenerator.GenerateToken(teamID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	// Call procd stats API
	procdAddress := s.prodAddress(pod.Name, pod.Namespace)
	statsResp, err := s.procdClient.Stats(ctx, procdAddress, token)
	if err != nil {
		return nil, fmt.Errorf("call procd stats: %w", err)
	}

	return &statsResp.SandboxResourceUsage, nil
}

// extractOriginalResources extracts current resources from pod containers.
func (s *SandboxService) extractOriginalResources(pod *corev1.Pod) OriginalResources {
	original := OriginalResources{
		Containers: make(map[string]ContainerResources),
	}

	for _, container := range pod.Spec.Containers {
		original.Containers[container.Name] = ContainerResources{
			Requests: container.Resources.Requests.DeepCopy(),
			Limits:   container.Resources.Limits.DeepCopy(),
		}
	}

	return original
}
