package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
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
	AnnotationTeamID                       = "sandbox0.ai/team-id"
	AnnotationUserID                       = "sandbox0.ai/user-id"
	AnnotationClaimedAt                    = "sandbox0.ai/claimed-at"
	AnnotationClaimType                    = "sandbox0.ai/claim-type" // "hot" or "cold"
	AnnotationExpiresAt                    = "sandbox0.ai/expires-at"
	AnnotationHardExpiresAt                = "sandbox0.ai/hard-expires-at"
	AnnotationConfig                       = "sandbox0.ai/config"
	AnnotationPaused                       = "sandbox0.ai/paused"
	AnnotationPausedAt                     = "sandbox0.ai/paused-at"
	AnnotationPausedState                  = "sandbox0.ai/paused-state"
	AnnotationPowerStateDesired            = "sandbox0.ai/power-state-desired"
	AnnotationPowerStateDesiredGeneration  = "sandbox0.ai/power-state-desired-generation"
	AnnotationPowerStateObserved           = "sandbox0.ai/power-state-observed"
	AnnotationPowerStateObservedGeneration = "sandbox0.ai/power-state-observed-generation"
	AnnotationPowerStatePhase              = "sandbox0.ai/power-state-phase"
	AnnotationNetworkPolicy                = "sandbox0.ai/network-policy" // JSON serialized network policy spec
	AnnotationNetworkPolicyHash            = "sandbox0.ai/network-policy-hash"
	AnnotationNetworkPolicyAppliedHash     = "sandbox0.ai/network-policy-applied-hash"
	AnnotationSandboxID                    = "sandbox0.ai/sandbox-id"
	AnnotationWebhookStateVolumeID         = "sandbox0.ai/webhook-state-volume-id"
	AnnotationTemplateSpecHash             = "sandbox0.ai/template-spec-hash"
	AnnotationClusterAutoscalerSafeToEvict = "cluster-autoscaler.kubernetes.io/safe-to-evict"

	unhealthyIdlePodRepairGracePeriod = 2 * time.Minute
)

// ClaimedSandboxPodAnnotations returns manager-owned metadata for active sandbox
// pods. Idle pool pods intentionally do not carry these annotations.
func ClaimedSandboxPodAnnotations(extra map[string]string) map[string]string {
	annotations := make(map[string]string, len(extra)+1)
	for key, value := range extra {
		annotations[key] = value
	}
	annotations[AnnotationClusterAutoscalerSafeToEvict] = "false"
	return annotations
}

// PoolManager manages the idle pool (ReplicaSet)
type PoolManager struct {
	k8sClient        kubernetes.Interface
	podLister        corelisters.PodLister
	replicaSetLister appslisters.ReplicaSetLister
	secretLister     corelisters.SecretLister
	recorder         record.EventRecorder
	logger           *zap.Logger
}

// NewPoolManager creates a new PoolManager
func NewPoolManager(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	replicaSetLister appslisters.ReplicaSetLister,
	secretLister corelisters.SecretLister,
	recorder record.EventRecorder,
	logger *zap.Logger,
) *PoolManager {
	return &PoolManager{
		k8sClient:        k8sClient,
		podLister:        podLister,
		replicaSetLister: replicaSetLister,
		secretLister:     secretLister,
		recorder:         recorder,
		logger:           logger,
	}
}

// ReconcilePool reconciles the idle pool for a template
func (pm *PoolManager) ReconcilePool(ctx context.Context, template *v1alpha1.SandboxTemplate) error {
	pm.logger.Info("Reconciling pool",
		zap.String("template", template.Name),
		zap.String("namespace", template.Namespace),
		zap.Int32("minIdle", template.Spec.Pool.MinIdle),
	)

	desiredTemplateHash, err := TemplateSpecHash(template)
	if err != nil {
		return fmt.Errorf("compute template hash: %w", err)
	}

	// 1. Ensure ReplicaSet exists and is configured correctly
	rs, err := pm.getOrCreateReplicaSet(ctx, template)
	if err != nil {
		return fmt.Errorf("get or create replicaset: %w", err)
	}

	// 2. Ensure newly created pods use the latest template spec hash.
	rs, err = pm.reconcileReplicaSetTemplate(ctx, template, rs, desiredTemplateHash)
	if err != nil {
		return fmt.Errorf("reconcile replicaset template: %w", err)
	}

	// 3. Drain stale idle pods atomically with delete preconditions.
	if err := pm.drainStaleIdlePods(ctx, template, desiredTemplateHash); err != nil {
		return fmt.Errorf("drain stale idle pods: %w", err)
	}

	// 4. Repair current-hash idle pods that are stuck and will keep the
	// ReplicaSet from creating replacements.
	if err := pm.repairUnhealthyIdlePods(ctx, template, desiredTemplateHash); err != nil {
		return fmt.Errorf("repair unhealthy idle pods: %w", err)
	}

	// 5. Check if replicas match minIdle
	if rs.Spec.Replicas == nil || *rs.Spec.Replicas != template.Spec.Pool.MinIdle {
		pm.logger.Info("Updating ReplicaSet replicas",
			zap.String("template", template.Name),
			zap.Int32("current", getInt32Value(rs.Spec.Replicas)),
			zap.Int32("desired", template.Spec.Pool.MinIdle),
		)

		rs.Spec.Replicas = &template.Spec.Pool.MinIdle
		_, err = pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Update(ctx, rs, metav1.UpdateOptions{})
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
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return nil, fmt.Errorf("generate replicaset name: %w", err)
	}
	if err := EnsureProcdConfigSecret(ctx, pm.k8sClient, pm.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}
	if err := EnsureNetdMITMCASecret(ctx, pm.k8sClient, template.Namespace); err != nil {
		return nil, fmt.Errorf("ensure netd MITM CA secret: %w", err)
	}
	// Try to get existing ReplicaSet
	rs, err := pm.replicaSetLister.ReplicaSets(template.Namespace).Get(rsName)
	if err == nil {
		return pm.reconcileReplicaSetMetadata(ctx, template, rs)
	}

	if !errors.IsNotFound(err) {
		return nil, err
	}

	// Create new ReplicaSet
	pm.logger.Info("Creating new ReplicaSet", zap.String("name", rsName))
	hash, err := TemplateSpecHash(template)
	if err != nil {
		return nil, fmt.Errorf("compute template hash: %w", err)
	}
	podTemplate, err := pm.buildPodTemplate(template, hash)
	if err != nil {
		return nil, fmt.Errorf("build pod template: %w", err)
	}

	rs = &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(template, v1alpha1.SchemeGroupVersion.WithKind("SandboxTemplate")),
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &template.Spec.Pool.MinIdle,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					LabelTemplateID: template.Name,
					LabelPoolType:   PoolTypeIdle,
				},
			},
			Template: podTemplate,
		},
	}

	rs, err = pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Create(ctx, rs, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			existing, getErr := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
			if getErr != nil {
				return nil, fmt.Errorf("get replicaset after already exists: %w", getErr)
			}
			return pm.reconcileReplicaSetMetadata(ctx, template, existing)
		}
		pm.recorder.Eventf(template, corev1.EventTypeWarning, "ReplicaSetCreateFailed",
			"Failed to create ReplicaSet: %v", err)
		return nil, fmt.Errorf("create replicaset: %w", err)
	}

	pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetCreated",
		"Created ReplicaSet with %d replicas", template.Spec.Pool.MinIdle)

	return rs, nil
}

func (pm *PoolManager) reconcileReplicaSetMetadata(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	rs *appsv1.ReplicaSet,
) (*appsv1.ReplicaSet, error) {
	if rs == nil {
		return nil, fmt.Errorf("replicaset is required")
	}
	if rs.DeletionTimestamp != nil {
		return rs, nil
	}

	desiredLabels := map[string]string{
		LabelTemplateID: template.Name,
	}
	desiredOwnerRefs := []metav1.OwnerReference{
		*metav1.NewControllerRef(template, v1alpha1.SchemeGroupVersion.WithKind("SandboxTemplate")),
	}
	if reflect.DeepEqual(rs.Labels, desiredLabels) && reflect.DeepEqual(rs.OwnerReferences, desiredOwnerRefs) {
		return rs, nil
	}

	var updatedRS *appsv1.ReplicaSet
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := pm.k8sClient.AppsV1().ReplicaSets(rs.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.DeletionTimestamp != nil {
			updatedRS = current
			return nil
		}
		updated := current.DeepCopy()
		updated.Labels = desiredLabels
		updated.OwnerReferences = desiredOwnerRefs
		if reflect.DeepEqual(current.Labels, updated.Labels) &&
			reflect.DeepEqual(current.OwnerReferences, updated.OwnerReferences) {
			updatedRS = current
			return nil
		}
		updatedRS, err = pm.k8sClient.AppsV1().ReplicaSets(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile replicaset metadata: %w", err)
	}
	pm.logger.Info("Reconciled ReplicaSet metadata",
		zap.String("template", template.Name),
		zap.String("replicaset", rs.Name),
	)
	return updatedRS, nil
}

// buildPodTemplate builds the pod template for a template
func (pm *PoolManager) buildPodTemplate(template *v1alpha1.SandboxTemplate, specHash string) (corev1.PodTemplateSpec, error) {
	spec := v1alpha1.BuildPodSpec(template)
	annotations := map[string]string{
		AnnotationTemplateSpecHash: specHash,
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			},
			Annotations: annotations,
		},
		Spec: spec,
	}, nil
}

func (pm *PoolManager) reconcileReplicaSetTemplate(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	rs *appsv1.ReplicaSet,
	desiredTemplateHash string,
) (*appsv1.ReplicaSet, error) {
	currentTemplateHash := rs.Spec.Template.Annotations[AnnotationTemplateSpecHash]
	if currentTemplateHash == desiredTemplateHash {
		return rs, nil
	}

	newTemplate, err := pm.buildPodTemplate(template, desiredTemplateHash)
	if err != nil {
		return nil, fmt.Errorf("build pod template: %w", err)
	}

	updated := rs.DeepCopy()
	updated.Spec.Template = newTemplate
	updatedRS, err := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetTemplateUpdated",
		"Updated ReplicaSet pod template hash to %s", desiredTemplateHash)
	pm.logger.Info("Updated ReplicaSet pod template hash",
		zap.String("template", template.Name),
		zap.String("hash", desiredTemplateHash),
	)
	return updatedRS, nil
}

func (pm *PoolManager) drainStaleIdlePods(ctx context.Context, template *v1alpha1.SandboxTemplate, desiredTemplateHash string) error {
	pods, err := pm.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return err
	}

	drained := 0
	for _, pod := range pods {
		if pod.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			continue
		}
		deleted, err := pm.deleteStaleIdlePodWithRetry(ctx, template.Namespace, pod.Name, desiredTemplateHash)
		if err != nil {
			return err
		}
		if deleted {
			drained++
		}
	}

	if drained > 0 {
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "StaleIdlePodsDrained",
			"Drained %d stale idle pod(s) with outdated template hash", drained)
		pm.logger.Info("Drained stale idle pods",
			zap.String("template", template.Name),
			zap.Int("count", drained),
			zap.String("desiredHash", desiredTemplateHash),
		)
	}
	return nil
}

func (pm *PoolManager) repairUnhealthyIdlePods(ctx context.Context, template *v1alpha1.SandboxTemplate, desiredTemplateHash string) error {
	pods, err := pm.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return err
	}

	now := time.Now()
	repaired := 0
	for _, pod := range pods {
		if pod.Annotations[AnnotationTemplateSpecHash] != desiredTemplateHash {
			continue
		}
		if !shouldRepairUnhealthyIdlePod(pod, now) {
			continue
		}
		deleted, err := pm.deleteUnhealthyIdlePodWithRetry(ctx, template.Namespace, pod.Name, desiredTemplateHash)
		if err != nil {
			return err
		}
		if deleted {
			repaired++
		}
	}

	if repaired > 0 {
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "UnhealthyIdlePodsRepaired",
			"Deleted %d unhealthy idle pod(s) so the ReplicaSet can recreate them", repaired)
		pm.logger.Info("Repaired unhealthy idle pods",
			zap.String("template", template.Name),
			zap.Int("count", repaired),
			zap.String("desiredHash", desiredTemplateHash),
		)
	}
	return nil
}

func shouldRepairUnhealthyIdlePod(pod *corev1.Pod, now time.Time) bool {
	if pod == nil || pod.DeletionTimestamp != nil || IsPodReady(pod) {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return true
	}
	if pod.CreationTimestamp.IsZero() {
		return false
	}
	return now.Sub(pod.CreationTimestamp.Time) >= unhealthyIdlePodRepairGracePeriod
}

func (pm *PoolManager) deleteUnhealthyIdlePodWithRetry(ctx context.Context, namespace, podName, desiredTemplateHash string) (bool, error) {
	deleted := false
	retryErr := retry.OnError(retry.DefaultRetry, func(err error) bool {
		return errors.IsConflict(err) || errors.IsInvalid(err)
	}, func() error {
		pod, err := pm.k8sClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}

		if pod.Labels[LabelPoolType] != PoolTypeIdle ||
			pod.Annotations[AnnotationTemplateSpecHash] != desiredTemplateHash ||
			!shouldRepairUnhealthyIdlePod(pod, time.Now()) {
			return nil
		}

		uid := pod.UID
		resourceVersion := pod.ResourceVersion
		err = pm.k8sClient.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &uid,
				ResourceVersion: &resourceVersion,
			},
		})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		deleted = err == nil
		return nil
	})
	if retryErr != nil {
		return false, retryErr
	}
	return deleted, nil
}

func (pm *PoolManager) deleteStaleIdlePodWithRetry(ctx context.Context, namespace, podName, desiredTemplateHash string) (bool, error) {
	deleted := false
	// Retry small transient races while still validating the pod is stale+idle.
	retryErr := retry.OnError(retry.DefaultRetry, func(err error) bool {
		return errors.IsConflict(err) || errors.IsInvalid(err)
	}, func() error {
		pod, err := pm.k8sClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}

		// If pod has been claimed or already updated to latest hash, skip delete.
		if pod.Labels[LabelPoolType] != PoolTypeIdle || pod.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			return nil
		}

		uid := pod.UID
		resourceVersion := pod.ResourceVersion
		err = pm.k8sClient.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &uid,
				ResourceVersion: &resourceVersion,
			},
		})
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		deleted = err == nil
		return nil
	})
	if retryErr != nil {
		return false, retryErr
	}
	return deleted, nil
}

// TemplateSpecHash returns the pod spec hash used to identify current idle pods.
func TemplateSpecHash(template *v1alpha1.SandboxTemplate) (string, error) {
	podSpec := v1alpha1.BuildPodSpec(template)
	payload := struct {
		PodSpec corev1.PodSpec `json:"podSpec"`
	}{
		PodSpec: podSpec,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Helper functions
func getInt32Value(val *int32) int32 {
	if val == nil {
		return 0
	}
	return *val
}
