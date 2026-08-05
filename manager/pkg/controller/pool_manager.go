package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	config "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	managernaming "github.com/sandbox0-ai/sandbox0/manager/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
)

const (
	// Labels
	LabelTemplateID        = sandboxpod.LabelTemplateID
	LabelTemplateLogicalID = sandboxpod.LabelTemplateLogicalID
	LabelTemplateScope     = sandboxpod.LabelTemplateScope
	LabelPoolType          = sandboxpod.LabelPoolType
	LabelSandboxID         = sandboxpod.LabelSandboxID
	LabelOwnerKind         = sandboxpod.LabelOwnerKind

	// Pool types
	PoolTypeIdle   = sandboxpod.PoolTypeIdle
	PoolTypeActive = sandboxpod.PoolTypeActive

	// Annotations
	AnnotationTeamID                       = sandboxpod.AnnotationTeamID
	AnnotationUserID                       = sandboxpod.AnnotationUserID
	AnnotationClaimedAt                    = sandboxpod.AnnotationClaimedAt
	AnnotationClaimType                    = sandboxpod.AnnotationClaimType
	AnnotationExpiresAt                    = sandboxpod.AnnotationExpiresAt
	AnnotationHardExpiresAt                = sandboxpod.AnnotationHardExpiresAt
	AnnotationConfig                       = sandboxpod.AnnotationConfig
	AnnotationMounts                       = sandboxpod.AnnotationMounts
	AnnotationPaused                       = sandboxpod.AnnotationPaused
	AnnotationPausedAt                     = sandboxpod.AnnotationPausedAt
	AnnotationPausedState                  = sandboxpod.AnnotationPausedState
	AnnotationPowerStateDesired            = sandboxpod.AnnotationPowerStateDesired
	AnnotationPowerStateDesiredGeneration  = sandboxpod.AnnotationPowerStateDesiredGeneration
	AnnotationPowerStateObserved           = sandboxpod.AnnotationPowerStateObserved
	AnnotationPowerStateObservedGeneration = sandboxpod.AnnotationPowerStateObservedGeneration
	AnnotationPowerStatePhase              = sandboxpod.AnnotationPowerStatePhase
	AnnotationNetworkPolicy                = sandboxpod.AnnotationNetworkPolicy
	AnnotationNetworkPolicyHash            = sandboxpod.AnnotationNetworkPolicyHash
	AnnotationNetworkPolicyAppliedHash     = sandboxpod.AnnotationNetworkPolicyAppliedHash
	AnnotationSandboxID                    = sandboxpod.AnnotationSandboxID
	AnnotationRuntimeGeneration            = sandboxpod.AnnotationRuntimeGeneration
	AnnotationRootFSSnapshotterInstance    = sandboxpod.AnnotationRootFSSnapshotterInstance
	AnnotationWebhookStateVolumeID         = sandboxpod.AnnotationWebhookStateVolumeID
	AnnotationHotClaimReservation          = sandboxpod.AnnotationHotClaimReservation
	AnnotationHotClaimReservationState     = sandboxpod.AnnotationHotClaimReservationState
	AnnotationHotClaimReservedAt           = sandboxpod.AnnotationHotClaimReservedAt
	AnnotationHotClaimReadyAt              = sandboxpod.AnnotationHotClaimReadyAt
	AnnotationHotClaimCompletionProtocol   = sandboxpod.AnnotationHotClaimCompletionProtocol
	AnnotationTemplateSpecHash             = sandboxpod.AnnotationTemplateSpecHash
	AnnotationTemplateTeamID               = sandboxpod.AnnotationTemplateTeamID
	AnnotationTemplateUserID               = sandboxpod.AnnotationTemplateUserID
	AnnotationOwnerKind                    = sandboxpod.AnnotationOwnerKind

	OwnerKindTeamWarmPool = sandboxpod.OwnerKindTeamWarmPool

	HotClaimReservationStateInitializing = sandboxpod.HotClaimReservationStateInitializing
	HotClaimReservationStateReady        = sandboxpod.HotClaimReservationStateReady
	HotClaimCompletionProtocolRecordV2   = sandboxpod.HotClaimCompletionProtocolRecordV2

	warmPoolRolloutRequeueAfter      = 10 * time.Second
	claimedPodAnnotationRequeueAfter = 2 * time.Second

	warmPoolRolloutMaxUnavailablePercent   int32 = 10
	warmPoolRolloutMaxUnavailableLimit     int32 = 10
	claimedPodAnnotationReconcileBatchSize       = 50
)

func TemplateLogicalID(template *v1alpha1.SandboxTemplate) string {
	if template == nil {
		return ""
	}
	if template.Labels != nil {
		if logicalID := template.Labels[LabelTemplateLogicalID]; logicalID != "" {
			return logicalID
		}
	}
	return template.Name
}

// ClaimedSandboxPodAnnotations returns manager-owned metadata for active
// sandbox pods. Platform-configured autoscaler annotations are set to false.
func ClaimedSandboxPodAnnotations(extra map[string]string, autoscalerAnnotationKeys []string) map[string]string {
	annotations := make(map[string]string, len(extra)+len(autoscalerAnnotationKeys))
	for key, value := range extra {
		annotations[key] = value
	}
	applyAutoscalerSafeToEvictAnnotations(annotations, autoscalerAnnotationKeys, false)
	return annotations
}

// NormalizeAutoscalerSafeToEvictAnnotationKeys validates, trims, and
// de-duplicates platform-provided annotation keys.
func NormalizeAutoscalerSafeToEvictAnnotationKeys(keys []string) ([]string, error) {
	normalized := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, raw := range keys {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return nil, fmt.Errorf("invalid autoscaler safe-to-evict annotation key %q: %s", key, strings.Join(errs, "; "))
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, key)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func applyAutoscalerSafeToEvictAnnotations(annotations map[string]string, keys []string, safe bool) {
	value := "false"
	if safe {
		value = "true"
	}
	for _, key := range keys {
		annotations[key] = value
	}
}

// IsHotClaimReservedPod reports whether an idle warm-pool pod is reserved by a
// sandbox claim and must no longer be exposed as idle capacity.
func IsHotClaimReservedPod(pod *corev1.Pod) bool {
	return sandboxpod.IsHotClaimReserved(pod)
}

// IsClaimedSandboxPod reports whether a pod is an active sandbox, including
// the short interval in which a hot claim still belongs to its warm pool.
func IsClaimedSandboxPod(pod *corev1.Pod) bool {
	return sandboxpod.IsClaimed(pod)
}

// PoolManager manages the idle pool (ReplicaSet)
type PoolManager struct {
	k8sClient                kubernetes.Interface
	podLister                corelisters.PodLister
	replicaSetLister         appslisters.ReplicaSetLister
	secretLister             corelisters.SecretLister
	recorder                 record.EventRecorder
	logger                   *zap.Logger
	teardown                 *PodTeardownCoordinator
	autoscalerAnnotationKeys []string
}

// NewPoolManager creates a new PoolManager
func NewPoolManager(
	k8sClient kubernetes.Interface,
	podLister corelisters.PodLister,
	replicaSetLister appslisters.ReplicaSetLister,
	secretLister corelisters.SecretLister,
	recorder record.EventRecorder,
	logger *zap.Logger,
	teardown *PodTeardownCoordinator,
	autoscalerAnnotationKeys []string,
) *PoolManager {
	return &PoolManager{
		k8sClient:                k8sClient,
		podLister:                podLister,
		replicaSetLister:         replicaSetLister,
		secretLister:             secretLister,
		recorder:                 recorder,
		logger:                   logger,
		teardown:                 teardown,
		autoscalerAnnotationKeys: append([]string(nil), autoscalerAnnotationKeys...),
	}
}

// ReconcilePool reconciles the idle pool for a template.
func (pm *PoolManager) ReconcilePool(ctx context.Context, template *v1alpha1.SandboxTemplate) (time.Duration, error) {
	pm.logger.Debug("Reconciling pool",
		zap.String("template", template.Name),
		zap.String("namespace", template.Namespace),
		zap.Int32("minIdle", template.Spec.Pool.MinIdle),
	)

	desiredTemplateHash, err := TemplateSpecHash(template)
	if err != nil {
		return 0, fmt.Errorf("compute template hash: %w", err)
	}

	// 1. Ensure ReplicaSet exists and is configured correctly
	rs, err := pm.getOrCreateReplicaSet(ctx, template)
	if err != nil {
		return 0, fmt.Errorf("get or create replicaset: %w", err)
	}

	// 2. Ensure newly created pods use the latest template spec hash.
	rs, err = pm.reconcileReplicaSetTemplate(ctx, template, rs, desiredTemplateHash)
	if err != nil {
		return 0, fmt.Errorf("reconcile replicaset template: %w", err)
	}

	// 3. Keep a workload-availability guard for autoscalers that use the
	// Eviction API. Manager's own direct deletes are governed by the node-aware
	// teardown coordinator below.
	if err := pm.reconcileIdlePoolDisruptionBudget(ctx, template, rs); err != nil {
		return 0, fmt.Errorf("reconcile idle pool disruption budget: %w", err)
	}
	annotationReconcilePending, err := pm.reconcileClaimedPodAutoscalerAnnotations(ctx, template)
	if err != nil {
		return 0, fmt.Errorf("reconcile claimed pod autoscaler annotations: %w", err)
	}

	// 4. Drain stale idle pods in availability-bounded batches. A following
	// batch is not released until replacements from the previous batch are
	// ready, which prevents a manager rollout from recreating the whole pool at
	// once.
	rolloutPending, err := pm.drainStaleIdlePods(ctx, template, desiredTemplateHash)
	if err != nil {
		return 0, fmt.Errorf("drain stale idle pods: %w", err)
	}

	// 5. Repair current-hash idle pods that are stuck and will keep the
	// ReplicaSet from creating replacements.
	repairRequeueAfter, err := pm.repairUnhealthyIdlePods(ctx, template, desiredTemplateHash)
	if err != nil {
		return 0, fmt.Errorf("repair unhealthy idle pods: %w", err)
	}

	// 6. Keep the warm pool fixed at minIdle. Template autoscaling is disabled
	// because burst cold claims can already stress the data plane; expanding the
	// idle pool at the same time compounds that pressure and may create unused
	// pods after the burst ends.
	currentReplicas := getInt32Value(rs.Spec.Replicas)
	desiredReplicas := desiredPoolReplicas(template)
	requeueAfter := time.Duration(0)
	if rs.Spec.Replicas == nil || currentReplicas != desiredReplicas {
		requeueAfter, err = pm.reconcileReplicaSetReplicas(ctx, template, rs, desiredReplicas)
		if err != nil {
			return 0, err
		}
	}
	if rolloutPending && (requeueAfter <= 0 || warmPoolRolloutRequeueAfter < requeueAfter) {
		requeueAfter = warmPoolRolloutRequeueAfter
	}
	if annotationReconcilePending && (requeueAfter <= 0 || claimedPodAnnotationRequeueAfter < requeueAfter) {
		requeueAfter = claimedPodAnnotationRequeueAfter
	}
	if repairRequeueAfter > 0 && (requeueAfter <= 0 || repairRequeueAfter < requeueAfter) {
		requeueAfter = repairRequeueAfter
	}

	return requeueAfter, nil
}

// reconcileReplicaSetReplicas updates the warm-pool size to the configured minimum.
func (pm *PoolManager) reconcileReplicaSetReplicas(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	rs *appsv1.ReplicaSet,
	desiredReplicas int32,
) (time.Duration, error) {
	liveRS, err := pm.k8sClient.AppsV1().ReplicaSets(rs.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get replicaset for scale: %w", err)
	}
	currentReplicas := getInt32Value(liveRS.Spec.Replicas)
	if liveRS.Spec.Replicas != nil && currentReplicas == desiredReplicas {
		return 0, nil
	}

	pm.logger.Info("Updating ReplicaSet replicas",
		zap.String("template", template.Name),
		zap.Int32("current", currentReplicas),
		zap.Int32("desired", desiredReplicas),
	)

	if _, err := pm.updateReplicaSetReplicas(ctx, template.Namespace, rs.Name, desiredReplicas); err != nil {
		pm.recorder.Eventf(template, corev1.EventTypeWarning, "ReplicaSetUpdateFailed",
			"Failed to update ReplicaSet: %v", err)
		return 0, fmt.Errorf("update replicaset: %w", err)
	}
	pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetUpdated",
		"Updated ReplicaSet replicas to %d", desiredReplicas)
	return 0, nil
}

func (pm *PoolManager) updateReplicaSetReplicas(ctx context.Context, namespace, name string, replicas int32) (*appsv1.ReplicaSet, error) {
	var updatedRS *appsv1.ReplicaSet
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := pm.k8sClient.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		currentReplicas := getInt32Value(current.Spec.Replicas)
		if current.Spec.Replicas != nil && currentReplicas == replicas {
			updatedRS = current
			return nil
		}
		updated := current.DeepCopy()
		updated.Spec.Replicas = &replicas
		updatedRS, err = pm.k8sClient.AppsV1().ReplicaSets(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return updatedRS, nil
}

func desiredPoolReplicas(template *v1alpha1.SandboxTemplate) int32 {
	minIdle, _ := normalizedPoolBounds(template)
	return minIdle
}

func normalizedPoolBounds(template *v1alpha1.SandboxTemplate) (minIdle, maxIdle int32) {
	if template == nil {
		return 0, 0
	}
	minIdle = template.Spec.Pool.MinIdle
	maxIdle = template.Spec.Pool.MaxIdle
	if minIdle < 0 {
		minIdle = 0
	}
	if maxIdle < minIdle {
		maxIdle = minIdle
	}
	return minIdle, maxIdle
}

// getOrCreateReplicaSet gets or creates the ReplicaSet for a template
func (pm *PoolManager) getOrCreateReplicaSet(ctx context.Context, template *v1alpha1.SandboxTemplate) (*appsv1.ReplicaSet, error) {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := managernaming.ReplicaSetName(clusterID, template.Name)
	if err != nil {
		return nil, fmt.Errorf("generate replicaset name: %w", err)
	}
	if err := EnsureProcdConfigSecret(ctx, pm.k8sClient, pm.secretLister, template); err != nil {
		return nil, fmt.Errorf("ensure procd config secret: %w", err)
	}
	if err := EnsureNetdMITMCASecret(ctx, pm.k8sClient, pm.secretLister, template.Namespace); err != nil {
		return nil, fmt.Errorf("ensure network-runtime MITM CA secret: %w", err)
	}
	// Try to get existing ReplicaSet
	rs, err := pm.replicaSetLister.ReplicaSets(template.Namespace).Get(rsName)
	if err == nil {
		return pm.reconcileReplicaSetMetadata(ctx, template, rs)
	}

	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	// Create new ReplicaSet
	pm.logger.Info("Creating new ReplicaSet", zap.String("name", rsName))
	initialReplicas := int32(0)
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
			Replicas: &initialReplicas,
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
		if apierrors.IsAlreadyExists(err) {
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
		"Created ReplicaSet with %d replicas", initialReplicas)

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
	spec := v1alpha1.BuildIdlePodSpec(template)
	annotations := map[string]string{
		AnnotationTemplateSpecHash: specHash,
	}
	applyAutoscalerSafeToEvictAnnotations(annotations, pm.autoscalerAnnotationKeys, true)
	labels := map[string]string{
		LabelTemplateID:        template.Name,
		LabelTemplateLogicalID: TemplateLogicalID(template),
		LabelPoolType:          PoolTypeIdle,
	}
	if teamID := teamOwnedTemplateTeamID(template); teamID != "" {
		annotations[AnnotationTeamID] = teamID
		annotations[AnnotationOwnerKind] = OwnerKindTeamWarmPool
		labels[LabelOwnerKind] = OwnerKindTeamWarmPool
		if userID := teamOwnedTemplateUserID(template); userID != "" {
			annotations[AnnotationUserID] = userID
		}
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: spec,
	}, nil
}

type warmPoolTemplateMetadata struct {
	TeamID    string `json:"team_id"`
	UserID    string `json:"user_id,omitempty"`
	OwnerKind string `json:"owner_kind"`
}

func teamWarmPoolTemplateMetadata(template *v1alpha1.SandboxTemplate) *warmPoolTemplateMetadata {
	teamID := teamOwnedTemplateTeamID(template)
	if teamID == "" {
		return nil
	}
	return &warmPoolTemplateMetadata{
		TeamID:    teamID,
		UserID:    teamOwnedTemplateUserID(template),
		OwnerKind: OwnerKindTeamWarmPool,
	}
}

func teamOwnedTemplateTeamID(template *v1alpha1.SandboxTemplate) string {
	if template == nil || template.Labels[LabelTemplateScope] != naming.ScopeTeam {
		return ""
	}
	return template.Annotations[AnnotationTemplateTeamID]
}

func teamOwnedTemplateUserID(template *v1alpha1.SandboxTemplate) string {
	if template == nil || template.Labels[LabelTemplateScope] != naming.ScopeTeam {
		return ""
	}
	return template.Annotations[AnnotationTemplateUserID]
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

	var updatedRS *appsv1.ReplicaSet
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.Spec.Template.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			updatedRS = current
			return nil
		}
		updated := current.DeepCopy()
		updated.Spec.Template = newTemplate
		updatedRS, err = pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
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

func (pm *PoolManager) drainStaleIdlePods(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	desiredTemplateHash string,
) (bool, error) {
	selector := labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	})
	podList, err := pm.k8sClient.CoreV1().Pods(template.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return false, err
	}
	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for idx := range podList.Items {
		pods = append(pods, &podList.Items[idx])
	}

	desiredReplicas := desiredPoolReplicas(template)
	maxUnavailable := warmPoolRolloutMaxUnavailable(desiredReplicas)
	availabilityFloor := desiredReplicas - maxUnavailable
	if availabilityFloor < 0 {
		availabilityFloor = 0
	}

	readyIdlePods := int32(0)
	stalePods := make([]*corev1.Pod, 0)
	rolloutPending := false
	for _, pod := range pods {
		if IsHotClaimReservedPod(pod) {
			continue
		}
		if pod.DeletionTimestamp == nil && IsPodReady(pod) {
			readyIdlePods++
		}
		if pod.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			continue
		}
		rolloutPending = true
		if pod.DeletionTimestamp == nil {
			stalePods = append(stalePods, pod)
		}
	}

	// Unready stale pods do not contribute to pool availability, so replace
	// them before spending the availability budget on ready pods.
	sort.SliceStable(stalePods, func(i, j int) bool {
		iReady := IsPodReady(stalePods[i])
		jReady := IsPodReady(stalePods[j])
		if iReady != jReady {
			return !iReady
		}
		return stalePods[i].Name < stalePods[j].Name
	})

	readyDeletionBudget := readyIdlePods - availabilityFloor
	if readyDeletionBudget < 0 {
		readyDeletionBudget = 0
	}
	eligibleCandidates := make([]*corev1.Pod, 0, len(stalePods))
	for _, pod := range stalePods {
		if IsPodReady(pod) {
			if readyDeletionBudget <= 0 {
				continue
			}
			readyDeletionBudget--
		}
		eligibleCandidates = append(eligibleCandidates, pod)
	}
	leases, err := pm.teardownCoordinator().Acquire(eligibleCandidates, TeardownReasonStaleRollout)
	if err != nil {
		return rolloutPending, err
	}

	drained := 0
	for _, lease := range leases {
		pod := lease.Pod()
		deleted, err := pm.deleteStaleIdlePodWithRetry(ctx, template.Namespace, pod.Name, desiredTemplateHash)
		if err != nil {
			lease.Release()
			return false, err
		}
		if deleted {
			lease.Commit()
			drained++
		} else {
			lease.Release()
		}
	}

	if drained > 0 {
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "StaleIdlePodsDrained",
			"Drained %d stale idle pod(s) with outdated template hash in rollout batch", drained)
		pm.logger.Info("Drained stale idle pod rollout batch",
			zap.String("template", template.Name),
			zap.Int("count", drained),
			zap.Int32("readyIdle", readyIdlePods),
			zap.Int32("availabilityFloor", availabilityFloor),
			zap.Int32("maxUnavailable", maxUnavailable),
			zap.String("desiredHash", desiredTemplateHash),
		)
	}
	return rolloutPending, nil
}

func warmPoolRolloutMaxUnavailable(desiredReplicas int32) int32 {
	if desiredReplicas <= 0 {
		return 1
	}
	maxUnavailable := (desiredReplicas*warmPoolRolloutMaxUnavailablePercent + 99) / 100
	if maxUnavailable < 1 {
		maxUnavailable = 1
	}
	if maxUnavailable > warmPoolRolloutMaxUnavailableLimit {
		maxUnavailable = warmPoolRolloutMaxUnavailableLimit
	}
	return maxUnavailable
}

func (pm *PoolManager) reconcileIdlePoolDisruptionBudget(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	rs *appsv1.ReplicaSet,
) error {
	if rs == nil {
		return fmt.Errorf("replicaset is required")
	}
	maxUnavailable := intstr.FromInt32(warmPoolRolloutMaxUnavailable(desiredPoolReplicas(template)))
	unhealthyPolicy := policyv1.AlwaysAllow
	desired := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rs.Name + "-idle-pdb",
			Namespace: template.Namespace,
			Labels: map[string]string{
				LabelTemplateID: template.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(template, v1alpha1.SchemeGroupVersion.WithKind("SandboxTemplate")),
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				LabelTemplateID: template.Name,
				LabelPoolType:   PoolTypeIdle,
			}},
			UnhealthyPodEvictionPolicy: &unhealthyPolicy,
		},
	}

	pdbs := pm.k8sClient.PolicyV1().PodDisruptionBudgets(template.Namespace)
	current, err := pdbs.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := pdbs.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "IdlePoolDisruptionBudgetCreated",
			"Created idle pool disruption budget with maxUnavailable %s", maxUnavailable.String())
		return nil
	}
	if err != nil {
		return err
	}
	if current.DeletionTimestamp != nil {
		return nil
	}
	if reflect.DeepEqual(current.Labels, desired.Labels) &&
		reflect.DeepEqual(current.OwnerReferences, desired.OwnerReferences) &&
		reflect.DeepEqual(current.Spec, desired.Spec) {
		return nil
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := pdbs.Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if latest.DeletionTimestamp != nil {
			return nil
		}
		updated := latest.DeepCopy()
		updated.Labels = desired.Labels
		updated.OwnerReferences = desired.OwnerReferences
		updated.Spec = desired.Spec
		_, err = pdbs.Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

func (pm *PoolManager) reconcileClaimedPodAutoscalerAnnotations(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
) (bool, error) {
	if len(pm.autoscalerAnnotationKeys) == 0 {
		return false, nil
	}
	pods, err := pm.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
	}))
	if err != nil {
		return false, err
	}
	candidates := make([]*corev1.Pod, 0)
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || !IsClaimedSandboxPod(pod) || autoscalerAnnotationsMatch(pod.Annotations, pm.autoscalerAnnotationKeys, false) {
			continue
		}
		candidates = append(candidates, pod)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
	limit := len(candidates)
	if limit > claimedPodAnnotationReconcileBatchSize {
		limit = claimedPodAnnotationReconcileBatchSize
	}
	updated := 0
	for _, pod := range candidates[:limit] {
		changed, err := pm.updateClaimedPodAutoscalerAnnotations(ctx, pod.Namespace, pod.Name, template.Name)
		if err != nil {
			return len(candidates) > updated, err
		}
		if changed {
			updated++
		}
	}
	if updated > 0 {
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "ClaimedPodAutoscalerAnnotationsReconciled",
			"Marked %d claimed sandbox pod(s) unsafe for platform autoscaler eviction", updated)
		pm.logger.Info("Reconciled claimed pod autoscaler annotations",
			zap.String("template", template.Name),
			zap.Int("count", updated),
		)
	}
	return len(candidates) > limit, nil
}

func (pm *PoolManager) updateClaimedPodAutoscalerAnnotations(ctx context.Context, namespace, name, templateID string) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := pm.k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if pod.DeletionTimestamp != nil || pod.Labels[LabelTemplateID] != templateID || !IsClaimedSandboxPod(pod) || autoscalerAnnotationsMatch(pod.Annotations, pm.autoscalerAnnotationKeys, false) {
			return nil
		}
		updated := pod.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = make(map[string]string)
		}
		applyAutoscalerSafeToEvictAnnotations(updated.Annotations, pm.autoscalerAnnotationKeys, false)
		_, err = pm.k8sClient.CoreV1().Pods(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err == nil {
			changed = true
		}
		return err
	})
	return changed, err
}

func autoscalerAnnotationsMatch(annotations map[string]string, keys []string, safe bool) bool {
	want := "false"
	if safe {
		want = "true"
	}
	for _, key := range keys {
		if annotations[key] != want {
			return false
		}
	}
	return true
}

func (pm *PoolManager) repairUnhealthyIdlePods(ctx context.Context, template *v1alpha1.SandboxTemplate, desiredTemplateHash string) (time.Duration, error) {
	pods, err := pm.podLister.Pods(template.Namespace).List(labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	}))
	if err != nil {
		return 0, err
	}

	now := time.Now()
	gracePeriod := pm.teardownCoordinator().idlePodRepairGracePeriod
	candidates := make([]*corev1.Pod, 0)
	nextGraceCheck := time.Duration(0)
	for _, pod := range pods {
		if IsHotClaimReservedPod(pod) {
			continue
		}
		if pod.Annotations[AnnotationTemplateSpecHash] != desiredTemplateHash {
			continue
		}
		if shouldRepairUnhealthyIdlePod(pod, now, gracePeriod) {
			candidates = append(candidates, pod)
			continue
		}
		if pod.DeletionTimestamp != nil || IsPodReady(pod) || pod.CreationTimestamp.IsZero() {
			continue
		}
		remaining := gracePeriod - now.Sub(pod.CreationTimestamp.Time)
		if remaining > 0 && (nextGraceCheck <= 0 || remaining < nextGraceCheck) {
			nextGraceCheck = remaining
		}
	}

	leases, err := pm.teardownCoordinator().Acquire(candidates, TeardownReasonUnhealthyRepair)
	if err != nil {
		return warmPoolRolloutRequeueAfter, err
	}
	repaired := 0
	for _, lease := range leases {
		pod := lease.Pod()
		deleted, err := pm.deleteUnhealthyIdlePodWithRetry(ctx, template.Namespace, pod.Name, desiredTemplateHash)
		if err != nil {
			lease.Release()
			return warmPoolRolloutRequeueAfter, err
		}
		if deleted {
			lease.Commit()
			repaired++
		} else {
			lease.Release()
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
	if len(candidates) > repaired && (nextGraceCheck <= 0 || warmPoolRolloutRequeueAfter < nextGraceCheck) {
		nextGraceCheck = warmPoolRolloutRequeueAfter
	}
	return nextGraceCheck, nil
}

func (pm *PoolManager) teardownCoordinator() *PodTeardownCoordinator {
	if pm.teardown == nil {
		// A missing node lister fails closed for scheduled Pods. This fallback is
		// primarily useful to keep narrow controller tests safe; manager wiring
		// always injects the shared, node-aware coordinator.
		pm.teardown = NewPodTeardownCoordinator(pm.podLister, nil, config.PodTeardownConfig{}, 0, nil, pm.logger)
	}
	return pm.teardown
}

func shouldRepairUnhealthyIdlePod(pod *corev1.Pod, now time.Time, gracePeriod time.Duration) bool {
	if pod == nil || pod.DeletionTimestamp != nil || IsHotClaimReservedPod(pod) || IsPodReady(pod) {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return true
	}
	if pod.CreationTimestamp.IsZero() {
		return false
	}
	return now.Sub(pod.CreationTimestamp.Time) >= gracePeriod
}

func (pm *PoolManager) deleteUnhealthyIdlePodWithRetry(ctx context.Context, namespace, podName, desiredTemplateHash string) (bool, error) {
	deleted := false
	gracePeriod := pm.teardownCoordinator().idlePodRepairGracePeriod
	retryErr := retry.OnError(retry.DefaultRetry, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsInvalid(err)
	}, func() error {
		pod, err := pm.k8sClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		if pod.Labels[LabelPoolType] != PoolTypeIdle ||
			IsHotClaimReservedPod(pod) ||
			pod.Annotations[AnnotationTemplateSpecHash] != desiredTemplateHash ||
			!shouldRepairUnhealthyIdlePod(pod, time.Now(), gracePeriod) {
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
		if err != nil && !apierrors.IsNotFound(err) {
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
		return apierrors.IsConflict(err) || apierrors.IsInvalid(err)
	}, func() error {
		pod, err := pm.k8sClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		// If pod is already deleting, claimed, or already updated to latest hash, skip delete.
		if pod.DeletionTimestamp != nil ||
			pod.Labels[LabelPoolType] != PoolTypeIdle ||
			IsHotClaimReservedPod(pod) ||
			pod.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
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
		if err != nil && !apierrors.IsNotFound(err) {
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

// TemplateSpecHash returns the idle pod spec hash used to identify current idle pods.
func TemplateSpecHash(template *v1alpha1.SandboxTemplate) (string, error) {
	podSpec := v1alpha1.BuildIdlePodSpec(template)
	cfg := config.LoadManagerConfig()
	autoscalerAnnotationKeys, err := NormalizeAutoscalerSafeToEvictAnnotationKeys(cfg.AutoscalerSafeToEvictAnnotationKeys)
	if err != nil {
		return "", err
	}
	payload := struct {
		PodSpec                  corev1.PodSpec            `json:"podSpec"`
		WarmPool                 *warmPoolTemplateMetadata `json:"warmPool,omitempty"`
		AutoscalerAnnotationKeys []string                  `json:"autoscalerAnnotationKeys,omitempty"`
	}{
		PodSpec:                  podSpec,
		WarmPool:                 teamWarmPoolTemplateMetadata(template),
		AutoscalerAnnotationKeys: autoscalerAnnotationKeys,
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
