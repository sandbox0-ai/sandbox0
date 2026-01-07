package controller

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
)

const (
	// Labels
	LabelTemplateID = "sandbox0.ai/template-id"
	LabelPoolType   = "sandbox0.ai/pool-type"
	LabelSandboxID  = "sandbox0.ai/sandbox-id"

	// Pool types
	PoolTypeIdle   = "idle"
	PoolTypeActive = "active"

	// Annotations
	AnnotationTeamID    = "sandbox0.ai/team-id"
	AnnotationUserID    = "sandbox0.ai/user-id"
	AnnotationClaimedAt = "sandbox0.ai/claimed-at"
	AnnotationExpiresAt = "sandbox0.ai/expires-at"
	AnnotationConfig    = "sandbox0.ai/config"
)

// PoolManager manages the idle pool (ReplicaSet)
type PoolManager struct {
	k8sClient kubernetes.Interface
	podLister corelisters.PodLister
	recorder  record.EventRecorder
	logger    *zap.Logger
}

// NewPoolManager creates a new PoolManager
func NewPoolManager(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	recorder record.EventRecorder,
	logger *zap.Logger,
) *PoolManager {
	return &PoolManager{
		k8sClient: k8sClient,
		podLister: podLister,
		recorder:  recorder,
		logger:    logger,
	}
}

// ReconcilePool reconciles the idle pool for a template
func (pm *PoolManager) ReconcilePool(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	pm.logger.Info("Reconciling pool",
		zap.String("template", template.ObjectMeta.Name),
		zap.String("namespace", template.ObjectMeta.Namespace),
		zap.Int32("minIdle", template.Spec.Pool.MinIdle),
	)

	// 1. Ensure ReplicaSet exists and is configured correctly
	rs, err := pm.getOrCreateReplicaSet(ctx, template)
	if err != nil {
		return fmt.Errorf("get or create replicaset: %w", err)
	}

	// 2. Check if replicas match minIdle
	if rs.Spec.Replicas == nil || *rs.Spec.Replicas != template.Spec.Pool.MinIdle {
		pm.logger.Info("Updating ReplicaSet replicas",
			zap.String("template", template.ObjectMeta.Name),
			zap.Int32("current", getInt32Value(rs.Spec.Replicas)),
			zap.Int32("desired", template.Spec.Pool.MinIdle),
		)

		rs.Spec.Replicas = &template.Spec.Pool.MinIdle
		_, err = pm.k8sClient.AppsV1().ReplicaSets(template.ObjectMeta.Namespace).Update(ctx, rs, metav1.UpdateOptions{})
		if err != nil {
			pm.recorder.Eventf(template, corev1.EventTypeWarning, "ReplicaSetUpdateFailed",
				"Failed to update ReplicaSet: %v", err)
			return fmt.Errorf("update replicaset: %w", err)
		}

		pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetUpdated",
			"Updated ReplicaSet replicas to %d", template.Spec.Pool.MinIdle)
	}

	return nil
}

// getOrCreateReplicaSet gets or creates the ReplicaSet for a template
func (pm *PoolManager) getOrCreateReplicaSet(ctx context.Context, template *v1alpha1.SandboxTemplate) (*appsv1.ReplicaSet, error) {
	rsName := fmt.Sprintf("%s-pool", template.ObjectMeta.Name)

	// Try to get existing ReplicaSet
	rs, err := pm.k8sClient.AppsV1().ReplicaSets(template.ObjectMeta.Namespace).Get(ctx, rsName, metav1.GetOptions{})
	if err == nil {
		return rs, nil
	}

	if !errors.IsNotFound(err) {
		return nil, err
	}

	// Create new ReplicaSet
	pm.logger.Info("Creating new ReplicaSet", zap.String("name", rsName))

	rs = &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.ObjectMeta.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.ObjectMeta.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(template, v1alpha1.SchemeGroupVersion.WithKind("SandboxTemplate")),
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &template.Spec.Pool.MinIdle,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					LabelTemplateID: template.ObjectMeta.Name,
					LabelPoolType:   PoolTypeIdle,
				},
			},
			Template: pm.buildPodTemplate(template),
		},
	}

	rs, err = pm.k8sClient.AppsV1().ReplicaSets(template.ObjectMeta.Namespace).Create(ctx, rs, metav1.CreateOptions{})
	if err != nil {
		pm.recorder.Eventf(template, corev1.EventTypeWarning, "ReplicaSetCreateFailed",
			"Failed to create ReplicaSet: %v", err)
		return nil, fmt.Errorf("create replicaset: %w", err)
	}

	pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetCreated",
		"Created ReplicaSet with %d replicas", template.Spec.Pool.MinIdle)

	return rs, nil
}

// buildPodTemplate builds the pod template for a template
func (pm *PoolManager) buildPodTemplate(template *v1alpha1.SandboxTemplate) corev1.PodTemplateSpec {
	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				LabelTemplateID: template.ObjectMeta.Name,
				LabelPoolType:   PoolTypeIdle,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    pm.buildContainers(template),
		},
	}

	// Apply runtime class if specified
	if template.Spec.RuntimeClassName != nil {
		podTemplate.Spec.RuntimeClassName = template.Spec.RuntimeClassName
	}

	// Apply pod-level overrides
	if template.Spec.Pod != nil {
		if template.Spec.Pod.NodeSelector != nil {
			podTemplate.Spec.NodeSelector = template.Spec.Pod.NodeSelector
		}
		if template.Spec.Pod.ServiceAccountName != "" {
			podTemplate.Spec.ServiceAccountName = template.Spec.Pod.ServiceAccountName
		}
		if template.Spec.Pod.Tolerations != nil {
			podTemplate.Spec.Tolerations = convertTolerations(template.Spec.Pod.Tolerations)
		}
		// Note: Affinity conversion is complex, skip for now or implement separately
	}

	return podTemplate
}

// buildContainers builds the containers for a pod
func (pm *PoolManager) buildContainers(template *v1alpha1.SandboxTemplate) []corev1.Container {
	containers := []corev1.Container{
		pm.buildContainer(&template.Spec.MainContainer, template, true),
	}

	for i := range template.Spec.Sidecars {
		containers = append(containers, pm.buildContainer(&template.Spec.Sidecars[i], template, false))
	}

	return containers
}

// buildContainer builds a single container
func (pm *PoolManager) buildContainer(spec *v1alpha1.ContainerSpec, template *v1alpha1.SandboxTemplate, isMain bool) corev1.Container {
	name := "procd"
	if !isMain {
		// For sidecars, generate unique name
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

	// Add global env vars
	for k, v := range template.Spec.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}

	// Add container-specific env vars
	for _, ev := range spec.Env {
		envVars = append(envVars, corev1.EnvVar{Name: ev.Name, Value: ev.Value})
	}

	container.Env = envVars

	// Resources
	if len(spec.Resources.Limits) > 0 || len(spec.Resources.Requests) > 0 {
		container.Resources = corev1.ResourceRequirements{
			Limits:   convertResourceList(spec.Resources.Limits),
			Requests: convertResourceList(spec.Resources.Requests),
		}
	}

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

// Helper functions

func getInt32Value(val *int32) int32 {
	if val == nil {
		return 0
	}
	return *val
}

func convertResourceList(resources map[string]string) corev1.ResourceList {
	if resources == nil {
		return nil
	}

	result := make(corev1.ResourceList)
	for k, v := range resources {
		result[corev1.ResourceName(k)] = resource.MustParse(v)
	}
	return result
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

func convertTolerations(tolerations []v1alpha1.Toleration) []corev1.Toleration {
	if tolerations == nil {
		return nil
	}

	result := make([]corev1.Toleration, len(tolerations))
	for i, t := range tolerations {
		result[i] = corev1.Toleration{
			Key:      t.Key,
			Operator: corev1.TolerationOperator(t.Operator),
			Value:    t.Value,
			Effect:   corev1.TaintEffect(t.Effect),
		}
	}
	return result
}
