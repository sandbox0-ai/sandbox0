package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	"github.com/sandbox0-ai/infra/manager/pkg/network"
	"github.com/sandbox0-ai/infra/pkg/naming"
	obsmetrics "github.com/sandbox0-ai/infra/pkg/observability/metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// Sandbox represents a sandbox instance
type Sandbox struct {
	ID           string    `json:"id"`
	TemplateID   string    `json:"template_id"`
	TeamID       string    `json:"team_id"`
	UserID       string    `json:"user_id"`
	InternalAddr string    `json:"internal_addr"`
	Status       string    `json:"status"`
	PodName      string    `json:"pod_name"`
	ExpiresAt    time.Time `json:"expires_at"`
	ClaimedAt    time.Time `json:"claimed_at"`
	CreatedAt    time.Time `json:"created_at"`
}

// SandboxStatus represents possible sandbox statuses
const (
	SandboxStatusPending   = "pending"
	SandboxStatusStarting  = "starting"
	SandboxStatusRunning   = "running"
	SandboxStatusFailed    = "failed"
	SandboxStatusCompleted = "completed"
)

// SandboxServiceConfig handles configuration for SandboxService
type SandboxServiceConfig struct {
	DefaultTTL             time.Duration
	PauseMinMemoryRequest  string
	PauseMinMemoryLimit    string
	PauseMemoryBufferRatio float64
	PauseMinCPU            string
	ProcdPort              int
	ProcdClientTimeout     time.Duration
	ProcdInitTimeout       time.Duration
}

// SandboxService handles sandbox operations
type SandboxService struct {
	k8sClient              kubernetes.Interface
	podLister              corelisters.PodLister
	sandboxIndex           *SandboxIndex
	secretLister           corelisters.SecretLister
	templateLister         controller.TemplateLister
	NetworkPolicyService   *NetworkPolicyService
	networkProvider        network.Provider
	procdClient            *ProcdClient
	internalTokenGenerator TokenGenerator
	procdTokenGenerator    TokenGenerator
	clock                  TimeProvider
	config                 SandboxServiceConfig
	logger                 *zap.Logger
	metrics                *obsmetrics.ManagerMetrics
}

// TimeProvider provides time functions, allowing for synchronized time across clusters
type TimeProvider interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Until(t time.Time) time.Duration
}

// systemTime is the default implementation using system time
type systemTime struct{}

func (systemTime) Now() time.Time                  { return time.Now() }
func (systemTime) Since(t time.Time) time.Duration { return time.Since(t) }
func (systemTime) Until(t time.Time) time.Duration { return time.Until(t) }

// TokenGenerator generates internal tokens for procd authentication.
type TokenGenerator interface {
	GenerateToken(teamID, userID, sandboxID string) (string, error)
}

// NewSandboxService creates a new SandboxService
func NewSandboxService(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	sandboxIndex *SandboxIndex,
	secretLister corelisters.SecretLister,
	templateLister controller.TemplateLister,
	networkPolicyService *NetworkPolicyService,
	networkProvider network.Provider,
	internalTokenGenerator TokenGenerator,
	procdTokenGenerator TokenGenerator,
	clock TimeProvider,
	config SandboxServiceConfig,
	logger *zap.Logger,
	metrics *obsmetrics.ManagerMetrics,
) *SandboxService {
	// Use system time as fallback if clock is nil
	if clock == nil {
		clock = systemTime{}
	}
	if networkProvider == nil {
		networkProvider = network.NewNoopProvider()
	}
	return &SandboxService{
		k8sClient:              k8sClient,
		podLister:              podLister,
		sandboxIndex:           sandboxIndex,
		secretLister:           secretLister,
		templateLister:         templateLister,
		NetworkPolicyService:   networkPolicyService,
		networkProvider:        networkProvider,
		procdClient:            NewProcdClient(ProcdClientConfig{Timeout: config.ProcdClientTimeout}),
		internalTokenGenerator: internalTokenGenerator,
		procdTokenGenerator:    procdTokenGenerator,
		clock:                  clock,
		config:                 config,
		logger:                 logger,
		metrics:                metrics,
	}
}

// SetProcdClient overrides the procd client (used by tests).
func (s *SandboxService) SetProcdClient(client *ProcdClient) {
	if client == nil {
		return
	}
	s.procdClient = client
}

// ClaimRequest represents a sandbox claim request
type ClaimRequest struct {
	TeamID   string
	UserID   string
	Template string         `json:"template"`
	Config   *SandboxConfig `json:"config,omitempty"`
}

// SandboxConfig represents sandbox configuration
type SandboxConfig struct {
	EnvVars map[string]string                 `json:"env_vars,omitempty"`
	TTL     int32                             `json:"ttl,omitempty"`      // Time-to-live in seconds
	HardTTL int32                             `json:"hard_ttl,omitempty"` // Hard time-to-live in seconds (0 disables)
	Network *v1alpha1.TplSandboxNetworkPolicy `json:"network,omitempty"`
	Webhook *WebhookConfig                    `json:"webhook,omitempty"`
}

// WebhookConfig represents outbound webhook configuration.
type WebhookConfig struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// ClaimResponse represents a sandbox claim response
type ClaimResponse struct {
	SandboxID    string  `json:"sandbox_id"`
	Status       string  `json:"status"`
	ProcdAddress string  `json:"procd_address"`
	PodName      string  `json:"pod_name"`
	Template     string  `json:"template"`
	ClusterId    *string `json:"cluster_id,omitempty"`
}

// ClaimSandbox claims a sandbox from the idle pool or creates a new one
func (s *SandboxService) ClaimSandbox(ctx context.Context, req *ClaimRequest) (*ClaimResponse, error) {
	start := time.Now()
	metrics := s.metrics
	s.logger.Info("Claiming sandbox",
		zap.String("template", req.Template),
		zap.String("teamID", req.TeamID),
	)

	// Resolve tenant template name:
	// prefer team-scoped template, fall back to public, and always enforce ownership checks.
	resolvedName := req.Template
	var template *v1alpha1.SandboxTemplate
	var err error

	if req.TeamID != "" {
		privateName := naming.TemplateNameForCluster(naming.ScopeTeam, req.TeamID, req.Template)
		privateNamespace, nsErr := naming.TemplateNamespaceFromName(privateName)
		if nsErr != nil {
			return nil, fmt.Errorf("resolve template namespace for %s: %w", privateName, nsErr)
		}
		t, getErr := s.templateLister.Get(privateNamespace, privateName)
		if getErr == nil {
			template = t
			resolvedName = privateName
		}
	}

	if template == nil {
		publicNamespace, nsErr := naming.TemplateNamespaceFromName(req.Template)
		if nsErr != nil {
			return nil, fmt.Errorf("resolve template namespace for %s: %w", req.Template, nsErr)
		}
		template, err = s.templateLister.Get(publicNamespace, req.Template)
		if err != nil {
			if errors.IsNotFound(err) {
				return nil, fmt.Errorf("template %s not found in namespace %s", req.Template, publicNamespace)
			}
			return nil, fmt.Errorf("get template: %w", err)
		}
	}

	// Enforce tenant isolation (best-effort based on scheduler-projected metadata).
	if template.Labels != nil && template.Labels["sandbox0.ai/template-scope"] == naming.ScopeTeam {
		teamID := ""
		if template.Annotations != nil {
			teamID = template.Annotations["sandbox0.ai/template-team-id"]
		}
		if teamID != "" && teamID != req.TeamID {
			return nil, fmt.Errorf("forbidden: template belongs to a different team")
		}
	}

	_ = resolvedName // reserved for audit/debugging (name used is template.ObjectMeta.Name)

	// Try to claim an idle pod first
	var pod *corev1.Pod
	claimType := "hot"
	for i := 0; i < 2; i++ {
		pod, err = s.claimIdlePod(ctx, template, req)
		if err != nil && !errors.IsConflict(err) {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("claim idle pod: %w", err)
		}
		if errors.IsConflict(err) {
			s.logger.Info("Idle pod is already claimed, trying again",
				zap.String("namespace", template.Namespace),
				zap.String("template", template.Name),
				zap.String("teamID", req.TeamID),
				zap.Error(err),
			)
			continue
		}
		if err == nil {
			break
		}
	}

	// If no idle pod available, create a new one (cold start)
	if pod == nil {
		claimType = "cold"
		s.logger.Info("No idle pod available, creating new pod",
			zap.String("template", req.Template),
		)
		pod, err = s.createNewPod(ctx, template, req)
		if err != nil {
			if metrics != nil {
				metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "error").Inc()
			}
			return nil, fmt.Errorf("create new pod: %w", err)
		}
	}

	if metrics != nil {
		metrics.SandboxClaimsTotal.WithLabelValues(req.Template, "success").Inc()
		metrics.SandboxClaimDuration.WithLabelValues(req.Template, claimType).Observe(time.Since(start).Seconds())
	}

	// Note: Network policies are stored in pod annotations
	// They are set in claimIdlePod() and createNewPod() methods

	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	if err := s.initializeProcd(ctx, pod, req, procdAddress); err != nil {
		return nil, fmt.Errorf("initialize procd: %w", err)
	}

	return &ClaimResponse{
		SandboxID:    pod.Name,
		Status:       "starting",
		ProcdAddress: procdAddress,
		PodName:      pod.Name,
		Template:     req.Template,
		ClusterId:    template.Spec.ClusterId,
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

	// Claim an available pod
	pod := runningPods[rand.Intn(len(runningPods))]

	s.logger.Info("Claiming idle pod",
		zap.String("pod", pod.Name),
		zap.String("sandboxID", pod.Name),
	)

	// Update pod labels and annotations
	pod = pod.DeepCopy()

	// Change pool type from idle to active
	pod.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	pod.Labels[controller.LabelSandboxID] = pod.Name

	// Remove owner reference (so it's no longer managed by ReplicaSet)
	pod.OwnerReferences = nil

	// Add annotations
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[controller.AnnotationSandboxID] = pod.Name
	pod.Annotations[controller.AnnotationTeamID] = req.TeamID
	pod.Annotations[controller.AnnotationUserID] = req.UserID
	pod.Annotations[controller.AnnotationClaimedAt] = s.clock.Now().Format(time.RFC3339)
	pod.Annotations[controller.AnnotationClaimType] = "hot"

	// Set expiration time
	ttl := int32(s.config.DefaultTTL.Seconds())
	if req.Config != nil && req.Config.TTL > 0 {
		ttl = req.Config.TTL
	}
	expiresAt := s.clock.Now().Add(time.Duration(ttl) * time.Second)
	pod.Annotations[controller.AnnotationExpiresAt] = expiresAt.Format(time.RFC3339)
	if req.Config != nil && req.Config.HardTTL > 0 {
		hardExpiresAt := s.clock.Now().Add(time.Duration(req.Config.HardTTL) * time.Second)
		pod.Annotations[controller.AnnotationHardExpiresAt] = hardExpiresAt.Format(time.RFC3339)
	}

	// Serialize config
	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return nil, fmt.Errorf("marshal config: %w", err)
		}
		pod.Annotations[controller.AnnotationConfig] = string(configJSON)
	}

	// Build and add network policy annotation
	networkSpec, err := s.applyPoliciesForPod(ctx, pod, template, req)
	if err != nil {
		return nil, err
	}

	// Update the pod
	updatedPod, err := s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update pod: %w", err)
	}

	if err := s.applyNetworkProvider(ctx, updatedPod, req.TeamID, networkSpec); err != nil {
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	s.logger.Info("Successfully claimed idle pod",
		zap.String("pod", updatedPod.Name),
		zap.String("sandboxID", updatedPod.Name),
		zap.Time("expiresAt", expiresAt),
	)

	return updatedPod, nil
}

// createNewPod creates a new pod for cold start
func (s *SandboxService) createNewPod(ctx context.Context, template *v1alpha1.SandboxTemplate, req *ClaimRequest) (*corev1.Pod, error) {
	// Simulate K8s pod name generation: rs-name + "-" + 5 random chars
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	podName, err := naming.SandboxName(clusterID, template.Name, utilrand.String(5))
	if err != nil {
		return nil, fmt.Errorf("generate sandbox name: %w", err)
	}
	if err := controller.EnsureProcdConfigSecret(ctx, s.k8sClient, s.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}

	// Build pod spec from template
	spec := v1alpha1.BuildPodSpec(template, false)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: template.ObjectMeta.Namespace,
			Labels: map[string]string{
				controller.LabelTemplateID: template.ObjectMeta.Name,
				controller.LabelPoolType:   controller.PoolTypeActive,
				controller.LabelSandboxID:  podName,
			},
			Annotations: map[string]string{
				controller.AnnotationSandboxID: podName,
				controller.AnnotationTeamID:    req.TeamID,
				controller.AnnotationUserID:    req.UserID,
				controller.AnnotationClaimedAt: s.clock.Now().Format(time.RFC3339),
				controller.AnnotationClaimType: "cold",
			},
		},
		Spec: spec,
	}

	// Set expiration time
	ttl := int32(s.config.DefaultTTL.Seconds())
	if req.Config != nil && req.Config.TTL > 0 {
		ttl = req.Config.TTL
	}
	expiresAt := s.clock.Now().Add(time.Duration(ttl) * time.Second)
	pod.Annotations[controller.AnnotationExpiresAt] = expiresAt.Format(time.RFC3339)
	if req.Config != nil && req.Config.HardTTL > 0 {
		hardExpiresAt := s.clock.Now().Add(time.Duration(req.Config.HardTTL) * time.Second)
		pod.Annotations[controller.AnnotationHardExpiresAt] = hardExpiresAt.Format(time.RFC3339)
	}

	// Serialize config
	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return nil, fmt.Errorf("marshal config: %w", err)
		}
		pod.Annotations[controller.AnnotationConfig] = string(configJSON)
	}

	// Build and add network policy annotation
	networkSpec, err := s.applyPoliciesForPod(ctx, pod, template, req)
	if err != nil {
		return nil, err
	}

	// Create the pod
	createdPod, err := s.k8sClient.CoreV1().Pods(template.ObjectMeta.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}

	if err := s.applyNetworkProvider(ctx, createdPod, req.TeamID, networkSpec); err != nil {
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	s.logger.Info("Created new pod for cold start",
		zap.String("pod", createdPod.Name),
		zap.String("sandboxID", createdPod.Name),
		zap.Time("expiresAt", expiresAt),
	)

	return createdPod, nil
}

type webhookInfo struct {
	URL      string
	Secret   string
	WatchDir string
}

func (s *SandboxService) getWebhookInfo(req *ClaimRequest) *webhookInfo {
	if req == nil || req.Config == nil || req.Config.Webhook == nil {
		return nil
	}
	urlValue := strings.TrimSpace(req.Config.Webhook.URL)
	if urlValue == "" {
		return nil
	}
	return &webhookInfo{
		URL:      urlValue,
		Secret:   strings.TrimSpace(req.Config.Webhook.Secret),
		WatchDir: strings.TrimSpace(req.Config.Webhook.WatchDir),
	}
}

func (s *SandboxService) appendWebhookNetworkPolicy(
	requestNetwork *v1alpha1.TplSandboxNetworkPolicy,
	webhookURL string,
) *v1alpha1.TplSandboxNetworkPolicy {
	if webhookURL == "" {
		return requestNetwork
	}
	parsed, err := url.Parse(webhookURL)
	if err != nil {
		s.logger.Warn("Failed to parse webhook URL",
			zap.String("webhook_url", webhookURL),
			zap.Error(err),
		)
		return requestNetwork
	}
	host := parsed.Hostname()
	if host == "" {
		s.logger.Warn("Webhook URL missing hostname",
			zap.String("webhook_url", webhookURL),
		)
		return requestNetwork
	}
	if requestNetwork == nil {
		requestNetwork = &v1alpha1.TplSandboxNetworkPolicy{}
	}
	if requestNetwork.Egress == nil {
		requestNetwork.Egress = &v1alpha1.NetworkEgressPolicy{}
	}
	if ip := net.ParseIP(host); ip != nil {
		requestNetwork.Egress.AllowedCIDRs = append(requestNetwork.Egress.AllowedCIDRs, formatCIDRForIP(ip))
		return requestNetwork
	}
	requestNetwork.Egress.AllowedDomains = append(requestNetwork.Egress.AllowedDomains, host)
	return requestNetwork
}

func formatCIDRForIP(ip net.IP) string {
	if ip.To4() != nil {
		return ip.String() + "/32"
	}
	return ip.String() + "/128"
}

func (s *SandboxService) applyPoliciesForPod(
	ctx context.Context,
	pod *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
) (*v1alpha1.NetworkPolicySpec, error) {
	if s.NetworkPolicyService == nil || pod == nil || template == nil || req == nil {
		return nil, nil
	}

	var requestNetwork *v1alpha1.TplSandboxNetworkPolicy
	if req.Config != nil {
		requestNetwork = req.Config.Network
	}
	webhookInfo := s.getWebhookInfo(req)
	if webhookInfo != nil {
		requestNetwork = s.appendWebhookNetworkPolicy(requestNetwork, webhookInfo.URL)
	}

	networkSpec := s.NetworkPolicyService.BuildNetworkPolicySpec(&BuildNetworkPolicyRequest{
		SandboxID:    pod.Name,
		TeamID:       req.TeamID,
		TemplateSpec: template.Spec.Network,
		RequestSpec:  requestNetwork,
	})
	if networkSpec != nil {
		if _, err := s.setNetworkPolicyAnnotations(pod, networkSpec); err != nil {
			return nil, err
		}
	}

	return networkSpec, nil
}

func (s *SandboxService) setNetworkPolicyAnnotations(pod *corev1.Pod, spec *v1alpha1.NetworkPolicySpec) (string, error) {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(spec)
	if err != nil {
		return "", fmt.Errorf("serialize network policy: %w", err)
	}
	pod.Annotations[controller.AnnotationNetworkPolicy] = annotation
	newHash := policyAnnotationHash(annotation)
	oldHash := pod.Annotations[controller.AnnotationNetworkPolicyHash]
	if newHash != "" {
		pod.Annotations[controller.AnnotationNetworkPolicyHash] = newHash
	} else {
		delete(pod.Annotations, controller.AnnotationNetworkPolicyHash)
	}
	if oldHash != newHash {
		delete(pod.Annotations, controller.AnnotationNetworkPolicyAppliedHash)
	}
	return newHash, nil
}

func (s *SandboxService) applyNetworkProvider(
	ctx context.Context,
	pod *corev1.Pod,
	teamID string,
	networkSpec *v1alpha1.NetworkPolicySpec,
) error {
	if s.networkProvider == nil || pod == nil || networkSpec == nil {
		return nil
	}

	input := network.SandboxPolicyInput{
		SandboxID:     pod.Name,
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		TeamID:        teamID,
		PodLabels:     pod.Labels,
		NetworkPolicy: networkSpec,
	}
	if err := s.networkProvider.ApplySandboxPolicy(ctx, input); err != nil {
		return err
	}
	return nil
}

func (s *SandboxService) initializeProcd(
	ctx context.Context,
	pod *corev1.Pod,
	req *ClaimRequest,
	procdAddress string,
) error {
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	if pod == nil || req == nil {
		return fmt.Errorf("missing sandbox context")
	}

	teamID := req.TeamID
	userID := req.UserID
	sandboxID := pod.Name

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return fmt.Errorf("generate procd token: %w", err)
	}

	webhookInfo := s.getWebhookInfo(req)
	var webhookConfig *InitializeWebhook
	if webhookInfo != nil {
		webhookConfig = &InitializeWebhook{
			URL:      webhookInfo.URL,
			Secret:   webhookInfo.Secret,
			WatchDir: webhookInfo.WatchDir,
		}
	}

	initReq := InitializeRequest{
		SandboxID: sandboxID,
		TeamID:    teamID,
		Webhook:   webhookConfig,
	}

	var initErr error
	timeout := s.config.ProcdInitTimeout
	if timeout == 0 {
		timeout = 6 * time.Second
	}

	initCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		_, initErr = s.procdClient.Initialize(initCtx, procdAddress, initReq, internalToken, procdToken)
		if initErr == nil {
			return nil
		}

		select {
		case <-initCtx.Done():
			return fmt.Errorf("initialize procd timed out after %s: %w", timeout, initErr)
		case <-ticker.C:
			continue
		}
	}
}

// TerminateSandbox terminates a sandbox
func (s *SandboxService) TerminateSandbox(ctx context.Context, sandboxID string) error {
	s.logger.Info("Terminating sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("get pod: %w", err)
	}

	// Note: Network policies are stored in pod annotations
	// They are automatically deleted when the pod is deleted
	if s.networkProvider != nil {
		if err := s.networkProvider.RemoveSandboxPolicy(ctx, pod.Namespace, pod.Name); err != nil {
			s.logger.Warn("Network provider remove failed",
				zap.String("provider", s.networkProvider.Name()),
				zap.String("sandboxID", pod.Name),
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
func (s *SandboxService) GetSandbox(ctx context.Context, sandboxID string) (*Sandbox, error) {
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	return s.podToSandbox(ctx, pod, sandboxID), nil
}

// GetNetworkPolicy gets the effective network policy for a sandbox.
func (s *SandboxService) GetNetworkPolicy(ctx context.Context, sandboxID string) (*v1alpha1.TplSandboxNetworkPolicy, error) {
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	annotation := ""
	if pod.Annotations != nil {
		annotation = pod.Annotations[controller.AnnotationNetworkPolicy]
	}
	spec, err := v1alpha1.ParseNetworkPolicyFromAnnotation(annotation)
	if err != nil {
		return nil, fmt.Errorf("parse network policy annotation: %w", err)
	}
	if spec != nil {
		return networkPolicyFromSpec(spec), nil
	}

	templateSpec := s.templateNetworkSpec(pod)
	if templateSpec != nil {
		return templateSpec.DeepCopy(), nil
	}

	return &v1alpha1.TplSandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}, nil
}

// UpdateNetworkPolicy updates the network policy for a sandbox.
func (s *SandboxService) UpdateNetworkPolicy(
	ctx context.Context,
	sandboxID string,
	policy *v1alpha1.TplSandboxNetworkPolicy,
) (*v1alpha1.TplSandboxNetworkPolicy, error) {
	if policy == nil {
		return nil, fmt.Errorf("network policy is required")
	}
	if s.NetworkPolicyService == nil {
		return nil, fmt.Errorf("network policy service not configured")
	}

	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	teamID := ""
	if pod.Annotations != nil {
		teamID = pod.Annotations[controller.AnnotationTeamID]
	}
	templateSpec := s.templateNetworkSpec(pod)

	networkSpec := s.NetworkPolicyService.BuildNetworkPolicySpec(&BuildNetworkPolicyRequest{
		SandboxID:    pod.Name,
		TeamID:       teamID,
		TemplateSpec: templateSpec,
		RequestSpec:  policy,
	})

	updatedPod := pod.DeepCopy()
	if updatedPod.Annotations == nil {
		updatedPod.Annotations = make(map[string]string)
	}
	if _, err := s.setNetworkPolicyAnnotations(updatedPod, networkSpec); err != nil {
		return nil, err
	}

	if configJSON := updatedPod.Annotations[controller.AnnotationConfig]; configJSON != "" {
		var config SandboxConfig
		if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
			s.logger.Warn("Failed to parse sandbox config annotation",
				zap.String("sandboxID", sandboxID),
				zap.Error(err),
			)
		} else {
			config.Network = policy
			updatedConfigJSON, err := json.Marshal(config)
			if err != nil {
				return nil, fmt.Errorf("marshal sandbox config: %w", err)
			}
			updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)
		}
	} else {
		config := SandboxConfig{Network: policy}
		updatedConfigJSON, err := json.Marshal(config)
		if err != nil {
			return nil, fmt.Errorf("marshal sandbox config: %w", err)
		}
		updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)
	}

	updatedPod, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, updatedPod, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update pod annotations: %w", err)
	}

	if err := s.applyNetworkProvider(ctx, updatedPod, teamID, networkSpec); err != nil {
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	return networkPolicyFromSpec(networkSpec), nil
}

func (s *SandboxService) getSandboxPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if s.sandboxIndex != nil {
		if namespace, ok := s.sandboxIndex.GetNamespace(sandboxID); ok {
			return s.podLister.Pods(namespace).Get(sandboxID)
		}
	}

	pods, err := s.podLister.List(labels.SelectorFromSet(map[string]string{
		controller.LabelSandboxID: sandboxID,
	}))
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, errors.NewNotFound(schema.GroupResource{Resource: "pod"}, sandboxID)
	}
	return pods[0], nil
}

func (s *SandboxService) templateNetworkSpec(pod *corev1.Pod) *v1alpha1.TplSandboxNetworkPolicy {
	if pod == nil || s.templateLister == nil {
		return nil
	}
	templateID := pod.Labels[controller.LabelTemplateID]
	if templateID == "" {
		return nil
	}
	namespace, err := naming.TemplateNamespaceFromName(templateID)
	if err != nil {
		s.logger.Warn("Failed to resolve template namespace",
			zap.String("templateID", templateID),
			zap.Error(err),
		)
		return nil
	}
	template, err := s.templateLister.Get(namespace, templateID)
	if err != nil {
		s.logger.Warn("Failed to get template for network policy",
			zap.String("templateID", templateID),
			zap.String("namespace", namespace),
			zap.Error(err),
		)
		return nil
	}
	return template.Spec.Network
}

func networkPolicyFromSpec(spec *v1alpha1.NetworkPolicySpec) *v1alpha1.TplSandboxNetworkPolicy {
	if spec == nil {
		return &v1alpha1.TplSandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}

	var (
		egressAllowedCIDRs   []string
		egressDeniedCIDRs    []string
		egressAllowedDomains []string
		egressDeniedDomains  []string
		egressAllowedPorts   []v1alpha1.PortSpec
		egressDeniedPorts    []v1alpha1.PortSpec
	)
	if spec.Egress != nil {
		egressAllowedCIDRs = append(egressAllowedCIDRs, spec.Egress.AllowedCIDRs...)
		egressDeniedCIDRs = append(egressDeniedCIDRs, spec.Egress.DeniedCIDRs...)
		egressAllowedDomains = append(egressAllowedDomains, spec.Egress.AllowedDomains...)
		egressDeniedDomains = append(egressDeniedDomains, spec.Egress.DeniedDomains...)
		egressAllowedPorts = append(egressAllowedPorts, spec.Egress.AllowedPorts...)
		egressDeniedPorts = append(egressDeniedPorts, spec.Egress.DeniedPorts...)
	}

	mode := v1alpha1.NetworkModeAllowAll
	if spec.Mode != "" {
		mode = spec.Mode
	}

	policy := &v1alpha1.TplSandboxNetworkPolicy{
		Mode: mode,
	}
	if len(egressAllowedCIDRs)+len(egressDeniedCIDRs)+len(egressAllowedDomains)+len(egressDeniedDomains)+len(egressAllowedPorts)+len(egressDeniedPorts) > 0 {
		policy.Egress = &v1alpha1.NetworkEgressPolicy{
			AllowedCIDRs:   egressAllowedCIDRs,
			DeniedCIDRs:    egressDeniedCIDRs,
			AllowedDomains: egressAllowedDomains,
			DeniedDomains:  egressDeniedDomains,
			AllowedPorts:   egressAllowedPorts,
			DeniedPorts:    egressDeniedPorts,
		}
	}

	return policy
}

// podToSandbox converts a pod to a sandbox object
func (s *SandboxService) podToSandbox(ctx context.Context, pod *corev1.Pod, sandboxID string) *Sandbox {
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

	internalAddr, err := s.prodAddress(ctx, pod)
	if err != nil {
		s.logger.Error("Failed to get procd address", zap.String("sandboxID", sandboxID), zap.Error(err))
	}

	return &Sandbox{
		ID:           sandboxID,
		TemplateID:   pod.Labels[controller.LabelTemplateID],
		TeamID:       pod.Annotations[controller.AnnotationTeamID],
		UserID:       pod.Annotations[controller.AnnotationUserID],
		InternalAddr: internalAddr,
		Status:       status,
		PodName:      pod.Name,
		ExpiresAt:    expiresAt,
		ClaimedAt:    claimedAt,
		CreatedAt:    createdAt,
	}
}

func (s *SandboxService) prodAddress(ctx context.Context, pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	if podIP := strings.TrimSpace(pod.Status.PodIP); podIP != "" {
		return fmt.Sprintf("http://%s:%d", podIP, s.config.ProcdPort), nil
	}

	podIP, err := s.waitForPodIP(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:%d", podIP, s.config.ProcdPort), nil
}

func (s *SandboxService) waitForPodIP(ctx context.Context, namespace, name string) (string, error) {
	const (
		maxAttempts = 15
		waitDelay   = 200 * time.Millisecond
	)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		pod, err := s.podLister.Pods(namespace).Get(name)
		if err != nil {
			return "", fmt.Errorf("get pod for ip: %w", err)
		}
		if podIP := strings.TrimSpace(pod.Status.PodIP); podIP != "" {
			return podIP, nil
		}

		if attempt == maxAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(waitDelay):
		}
	}

	return "", fmt.Errorf("pod ip not assigned")
}

// podPhaseToSandboxStatus converts pod phase to sandbox status
func (s *SandboxService) podPhaseToSandboxStatus(phase corev1.PodPhase) string {
	switch phase {
	case corev1.PodPending:
		return SandboxStatusStarting
	case corev1.PodRunning:
		return SandboxStatusRunning
	case corev1.PodSucceeded:
		return SandboxStatusCompleted
	case corev1.PodFailed:
		return SandboxStatusFailed
	default:
		return SandboxStatusPending
	}
}

// GetSandboxStatus gets the status of a sandbox
func (s *SandboxService) GetSandboxStatus(ctx context.Context, sandboxID string) (map[string]any, error) {
	sandbox, err := s.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}

	status := map[string]any{
		"sandbox_id":  sandbox.ID,
		"template_id": sandbox.TemplateID,
		"team_id":     sandbox.TeamID,
		"user_id":     sandbox.UserID,
		"pod_name":    sandbox.PodName,
		"status":      sandbox.Status,
		"claimed_at":  sandbox.ClaimedAt.Format(time.RFC3339),
		"expires_at":  sandbox.ExpiresAt.Format(time.RFC3339),
		"created_at":  sandbox.CreatedAt.Format(time.RFC3339),
	}

	return status, nil
}

// PauseSandboxResponse represents the response from pausing a sandbox.
type PauseSandboxResponse struct {
	SandboxID     string                `json:"sandbox_id"`
	Paused        bool                  `json:"paused"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
	UpdatedMemory string                `json:"updated_memory,omitempty"`
	UpdatedCPU    string                `json:"updated_cpu,omitempty"`
}

// ResumeSandboxResponse represents the response from resuming a sandbox.
type ResumeSandboxResponse struct {
	SandboxID      string `json:"sandbox_id"`
	Resumed        bool   `json:"resumed"`
	RestoredMemory string `json:"restored_memory,omitempty"`
}

// PausedState stores the sandbox state before pause for restoration on resume.
type PausedState struct {
	// Resources stores original pod resources before pause.
	Resources map[string]ContainerResources `json:"resources"`
	// OriginalTTL stores the original TTL (in seconds) set by user or default.
	// On resume, this TTL is reused to reset the countdown.
	OriginalTTL int32 `json:"original_ttl,omitempty"`
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
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Check if already paused
	if pod.Annotations[controller.AnnotationPaused] == "true" {
		return nil, fmt.Errorf("sandbox is already paused")
	}

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}

	// Call procd pause API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	pauseResp, err := s.procdClient.Pause(ctx, procdAddress, internalToken, procdToken)
	if err != nil {
		return nil, fmt.Errorf("call procd pause: %w", err)
	}

	if !pauseResp.Paused {
		return nil, fmt.Errorf("procd pause failed: %s", pauseResp.Error)
	}

	// Build paused state to save resources and original TTL
	pausedState := PausedState{
		Resources: s.extractOriginalResources(pod),
	}

	// Extract original TTL from config annotation
	if configJSON := pod.Annotations[controller.AnnotationConfig]; configJSON != "" {
		var config SandboxConfig
		if err := json.Unmarshal([]byte(configJSON), &config); err == nil && config.TTL > 0 {
			pausedState.OriginalTTL = config.TTL
		}
	}

	pausedStateJSON, err := json.Marshal(pausedState)
	if err != nil {
		return nil, fmt.Errorf("marshal paused state: %w", err)
	}

	// Calculate new memory request and limit
	// Request: Actual usage from procd (WorkingSet)
	// Limit: Usage + Buffer (current buffer algorithm: 10% buffer, min 32Mi)
	var newRequestMemory resource.Quantity
	var newLimitMemory resource.Quantity

	if pauseResp.ResourceUsage != nil && pauseResp.ResourceUsage.ContainerMemoryWorkingSet > 0 {
		workingSet := pauseResp.ResourceUsage.ContainerMemoryWorkingSet

		// Request = Actual Usage
		// Ensure a minimum safety baseline (e.g. 10Mi) to prevent container crash/instability
		// slightly lower than the buffer minimum
		reqBytes := int64(workingSet)
		minReq, err := resource.ParseQuantity(s.config.PauseMinMemoryRequest)
		if err == nil && reqBytes < minReq.Value() {
			reqBytes = minReq.Value()
		}
		newRequestMemory = *resource.NewQuantity(reqBytes, resource.BinarySI)

		// Limit = Usage * bufferRatio
		// Minimum limit to avoid too aggressive scaling
		limitBytes := int64(float64(workingSet) * s.config.PauseMemoryBufferRatio)
		minLimit, err := resource.ParseQuantity(s.config.PauseMinMemoryLimit)
		if err == nil && limitBytes < minLimit.Value() {
			limitBytes = minLimit.Value()
		}
		newLimitMemory = *resource.NewQuantity(limitBytes, resource.BinarySI)
	}

	// Minimal CPU resources for paused state
	// Since processes are SIGSTOP'ed, they consume negligible CPU.
	// We reduce requests to release node capacity for other workloads.
	// K8s doesn't allow 0 CPU, so we use a minimal value (e.g., 10m).
	minCPU := resource.MustParse(s.config.PauseMinCPU)

	// Update pod annotations (metadata update)
	annotatedPod := pod.DeepCopy()
	if annotatedPod.Annotations == nil {
		annotatedPod.Annotations = make(map[string]string)
	}
	annotatedPod.Annotations[controller.AnnotationPaused] = "true"
	annotatedPod.Annotations[controller.AnnotationPausedAt] = s.clock.Now().Format(time.RFC3339)
	annotatedPod.Annotations[controller.AnnotationPausedState] = string(pausedStateJSON)
	// Remove expires-at annotation to stop TTL countdown during pause
	delete(annotatedPod.Annotations, controller.AnnotationExpiresAt)

	updatedPod, updateErr := s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, annotatedPod, metav1.UpdateOptions{})
	if updateErr != nil {
		s.logger.Error("Failed to update pod annotations after pause",
			zap.String("sandboxID", sandboxID),
			zap.Error(updateErr),
		)
		// Continue; the sandbox is still paused in procd
	} else {
		pod = updatedPod
	}

	// Update container resources using the resize subresource (in-place)
	if !newLimitMemory.IsZero() || !minCPU.IsZero() {
		resizePod := pod.DeepCopy()
		found := false
		for i := range resizePod.Spec.Containers {
			container := &resizePod.Spec.Containers[i]
			if container.Name != "procd" {
				continue
			}
			found = true

			if container.Resources.Requests == nil {
				container.Resources.Requests = make(corev1.ResourceList)
			}
			if !newRequestMemory.IsZero() {
				container.Resources.Requests[corev1.ResourceMemory] = newRequestMemory
			}
			container.Resources.Requests[corev1.ResourceCPU] = minCPU

			if container.Resources.Limits == nil {
				container.Resources.Limits = make(corev1.ResourceList)
			}
			if !newLimitMemory.IsZero() {
				container.Resources.Limits[corev1.ResourceMemory] = newLimitMemory
			}
			container.Resources.Limits[corev1.ResourceCPU] = minCPU
		}

		if !found {
			s.logger.Warn("Main container 'procd' not found during pause resource update",
				zap.String("sandboxID", sandboxID))
		} else {
			if _, err = s.k8sClient.CoreV1().Pods(pod.Namespace).UpdateResize(ctx, pod.Name, resizePod, metav1.UpdateOptions{}); err != nil {
				s.logger.Error("Failed to update pod resources after pause",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				// Don't fail the pause operation; procd is already paused
			}
		}
	}

	s.logger.Info("Sandbox paused successfully",
		zap.String("sandboxID", sandboxID),
		zap.String("newRequest", newRequestMemory.String()),
		zap.String("newLimit", newLimitMemory.String()),
		zap.Int64("workingSet", pauseResp.ResourceUsage.ContainerMemoryWorkingSet),
	)

	return &PauseSandboxResponse{
		SandboxID:     sandboxID,
		Paused:        true,
		ResourceUsage: pauseResp.ResourceUsage,
		UpdatedMemory: newLimitMemory.String(),
		UpdatedCPU:    minCPU.String(),
	}, nil
}

// ResumeSandbox resumes a paused sandbox and restores original pod resources.
func (s *SandboxService) ResumeSandbox(ctx context.Context, sandboxID string) (*ResumeSandboxResponse, error) {
	s.logger.Info("Resuming sandbox", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pods: %w", err)
	}

	// Check if paused
	if pod.Annotations[controller.AnnotationPaused] != "true" {
		return nil, fmt.Errorf("sandbox is not paused")
	}

	// Restore original resources and TTL first (before resuming processes)
	var restoredMemory string
	pausedStateJSON := pod.Annotations[controller.AnnotationPausedState]
	if pausedStateJSON != "" {
		var pausedState PausedState
		if err := json.Unmarshal([]byte(pausedStateJSON), &pausedState); err == nil {
			annotationPod := pod.DeepCopy()
			if annotationPod.Annotations == nil {
				annotationPod.Annotations = make(map[string]string)
			}

			// Reset TTL using original TTL (not remaining time)
			if pausedState.OriginalTTL > 0 {
				newExpiresAt := s.clock.Now().Add(time.Duration(pausedState.OriginalTTL) * time.Second)
				annotationPod.Annotations[controller.AnnotationExpiresAt] = newExpiresAt.Format(time.RFC3339)
			} else {
				newExpiresAt := s.clock.Now().Add(s.config.DefaultTTL)
				annotationPod.Annotations[controller.AnnotationExpiresAt] = newExpiresAt.Format(time.RFC3339)
			}

			// Remove pause annotations
			delete(annotationPod.Annotations, controller.AnnotationPaused)
			delete(annotationPod.Annotations, controller.AnnotationPausedAt)
			delete(annotationPod.Annotations, controller.AnnotationPausedState)

			updatedPod, updateErr := s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, annotationPod, metav1.UpdateOptions{})
			if updateErr != nil {
				s.logger.Error("Failed to restore pod annotations before resume",
					zap.String("sandboxID", sandboxID),
					zap.Error(updateErr),
				)
			} else {
				pod = updatedPod
			}

			// Restore container resources using resize subresource
			resizePod := pod.DeepCopy()
			for i := range resizePod.Spec.Containers {
				container := &resizePod.Spec.Containers[i]
				if orig, ok := pausedState.Resources[container.Name]; ok {
					container.Resources.Requests = orig.Requests
					container.Resources.Limits = orig.Limits
					if memReq, ok := orig.Requests[corev1.ResourceMemory]; ok {
						restoredMemory = memReq.String()
					}
				}
			}
			if _, err = s.k8sClient.CoreV1().Pods(pod.Namespace).UpdateResize(ctx, pod.Name, resizePod, metav1.UpdateOptions{}); err != nil {
				s.logger.Error("Failed to restore pod resources before resume",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
				// Continue with resume anyway
			}
		}
	}

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}

	// Call procd resume API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	resumeResp, err := s.procdClient.Resume(ctx, procdAddress, internalToken, procdToken)
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

// PauseSandboxByID implements the SandboxPauser interface from controller package.
// It wraps PauseSandbox and returns only the error.
func (s *SandboxService) PauseSandboxByID(ctx context.Context, sandboxID string) error {
	_, err := s.PauseSandbox(ctx, sandboxID)
	return err
}

// TerminateSandboxByID implements the SandboxTerminator interface from controller package.
// It wraps TerminateSandbox and returns only the error.
func (s *SandboxService) TerminateSandboxByID(ctx context.Context, sandboxID string) error {
	return s.TerminateSandbox(ctx, sandboxID)
}

// GetSandboxResourceUsage gets the resource usage of a sandbox.
func (s *SandboxService) GetSandboxResourceUsage(ctx context.Context, sandboxID string) (*SandboxResourceUsage, error) {
	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	// Generate internal token for procd authentication
	if s.internalTokenGenerator == nil || s.procdTokenGenerator == nil {
		return nil, fmt.Errorf("token generators not configured, cannot authenticate with procd")
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]

	internalToken, err := s.internalTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate internal token: %w", err)
	}

	procdToken, err := s.procdTokenGenerator.GenerateToken(teamID, userID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}

	// Call procd stats API
	procdAddress, err := s.prodAddress(ctx, pod)
	if err != nil {
		return nil, fmt.Errorf("get procd address: %w", err)
	}
	statsResp, err := s.procdClient.Stats(ctx, procdAddress, internalToken, procdToken)
	if err != nil {
		return nil, fmt.Errorf("call procd stats: %w", err)
	}

	return &statsResp.SandboxResourceUsage, nil
}

// RefreshRequest represents a sandbox refresh request
type RefreshRequest struct {
	Duration int32 `json:"duration,omitempty"` // Duration to extend in seconds (optional, defaults to original TTL)
}

// RefreshResponse represents a sandbox refresh response
type RefreshResponse struct {
	SandboxID string    `json:"sandbox_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// RefreshSandbox refreshes the TTL of a sandbox
func (s *SandboxService) RefreshSandbox(ctx context.Context, sandboxID string, req *RefreshRequest) (*RefreshResponse, error) {
	s.logger.Info("Refreshing sandbox TTL", zap.String("sandboxID", sandboxID))

	// Find the pod by sandbox ID
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	// Determine the TTL duration
	var ttlDuration time.Duration
	if req != nil && req.Duration > 0 {
		ttlDuration = time.Duration(req.Duration) * time.Second
	} else {
		// Try to get original TTL from config annotation
		ttlDuration = s.config.DefaultTTL
		if configJSON := pod.Annotations[controller.AnnotationConfig]; configJSON != "" {
			var config SandboxConfig
			if err := json.Unmarshal([]byte(configJSON), &config); err == nil && config.TTL > 0 {
				ttlDuration = time.Duration(config.TTL) * time.Second
			}
		}
	}

	// Calculate new expiration time
	newExpiresAt := s.clock.Now().Add(ttlDuration)

	// Update pod annotation
	podCopy := pod.DeepCopy()
	if podCopy.Annotations == nil {
		podCopy.Annotations = make(map[string]string)
	}
	podCopy.Annotations[controller.AnnotationExpiresAt] = newExpiresAt.Format(time.RFC3339)

	// Apply the update
	_, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, podCopy, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update pod: %w", err)
	}

	s.logger.Info("Sandbox TTL refreshed successfully",
		zap.String("sandboxID", sandboxID),
		zap.Time("newExpiresAt", newExpiresAt),
		zap.Duration("ttlDuration", ttlDuration),
	)

	return &RefreshResponse{
		SandboxID: sandboxID,
		ExpiresAt: newExpiresAt,
	}, nil
}

// extractOriginalResources extracts current resources from pod containers.
func (s *SandboxService) extractOriginalResources(pod *corev1.Pod) map[string]ContainerResources {
	resources := make(map[string]ContainerResources)

	for _, container := range pod.Spec.Containers {
		resources[container.Name] = ContainerResources{
			Requests: container.Resources.Requests.DeepCopy(),
			Limits:   container.Resources.Limits.DeepCopy(),
		}
	}

	return resources
}
