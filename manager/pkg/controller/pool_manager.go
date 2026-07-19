package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeclassquota"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	LabelTemplateID        = "sandbox0.ai/template-id"
	LabelTemplateLogicalID = "sandbox0.ai/template-logical-id"
	LabelTemplateScope     = "sandbox0.ai/template-scope"
	LabelPoolType          = "sandbox0.ai/pool-type"
	LabelSandboxID         = "sandbox0.ai/sandbox-id"
	LabelOwnerKind         = "sandbox0.ai/owner-kind"

	// Pool types
	PoolTypeIdle   = "idle"
	PoolTypeActive = "active"

	// Annotations
	AnnotationTeamID                       = "sandbox0.ai/team-id"
	AnnotationUserID                       = "sandbox0.ai/user-id"
	AnnotationClusterID                    = "sandbox0.ai/cluster-id"
	AnnotationClaimedAt                    = "sandbox0.ai/claimed-at"
	AnnotationClaimType                    = "sandbox0.ai/claim-type" // "hot" or "cold"
	AnnotationExpiresAt                    = "sandbox0.ai/expires-at"
	AnnotationHardExpiresAt                = "sandbox0.ai/hard-expires-at"
	AnnotationConfig                       = "sandbox0.ai/config"
	AnnotationMounts                       = "sandbox0.ai/mounts"
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
	AnnotationRuntimeGeneration            = "sandbox0.ai/runtime-generation"
	AnnotationClaimStartReservation        = startlimiter.AnnotationClaimStartReservation
	AnnotationWebhookStateVolumeID         = "sandbox0.ai/webhook-state-volume-id"
	AnnotationTemplateSpecHash             = "sandbox0.ai/template-spec-hash"
	AnnotationTemplateTeamID               = "sandbox0.ai/template-team-id"
	AnnotationTemplateUserID               = "sandbox0.ai/template-user-id"
	AnnotationClusterAutoscalerSafeToEvict = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	AnnotationOwnerKind                    = "sandbox0.ai/owner-kind"
	AnnotationTeamQuotaWarmPoolTransfers   = "sandbox0.ai/team-quota-warm-pool-transfers"

	OwnerKindTeamWarmPool = "team_warm_pool"

	unhealthyIdlePodRepairGracePeriod = 2 * time.Minute
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

// ClaimedSandboxPodAnnotations returns manager-owned metadata for active sandbox
// pods. Active sandboxes are marked unsafe for Cluster Autoscaler eviction.
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
	k8sClient         kubernetes.Interface
	podLister         corelisters.PodLister
	replicaSetLister  appslisters.ReplicaSetLister
	secretLister      corelisters.SecretLister
	recorder          record.EventRecorder
	logger            *zap.Logger
	claimStartLimiter *startlimiter.Limiter
	teamQuotaStore    teamquota.CapacityStore
	teamQuotaLimiter  TeamQuotaRateLimiter
	quotaResources    func(*corev1.PodSpec) teamquota.Values
}

// TeamQuotaRateLimiter is the region-shared rate-admission contract used by
// warm-pool starts.
type TeamQuotaRateLimiter interface {
	Take(ctx context.Context, teamID string, key teamquota.Key, cost int64) (tokenbucket.Decision, error)
}

type teamQuotaInventoryStore interface {
	GetRecoveryAllocation(
		ctx context.Context,
		owner teamquota.Owner,
	) (*teamquota.RecoveryAllocation, error)
	ReconcileTargetIfRevision(
		ctx context.Context,
		owner teamquota.Owner,
		target teamquota.Values,
		runtime teamquota.RuntimeRef,
		expectedRevision int64,
	) (bool, error)
}

type teamWarmPoolRateLimitError struct {
	retryAfter time.Duration
}

func (e *teamWarmPoolRateLimitError) Error() string {
	return fmt.Sprintf("team warm-pool start rate exceeded; retry after %s", e.retryAfter)
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

func (pm *PoolManager) SetClaimStartLimiter(limiter *startlimiter.Limiter) {
	if pm == nil {
		return
	}
	pm.claimStartLimiter = limiter
}

// SetTeamQuotaStore configures region-wide accounting for team-owned warm
// pools. Public warm pools do not consume a Team Quota.
func (pm *PoolManager) SetTeamQuotaStore(
	store teamquota.CapacityStore,
	resources func(*corev1.PodSpec) teamquota.Values,
) {
	if pm == nil {
		return
	}
	pm.teamQuotaStore = store
	pm.quotaResources = resources
}

// SetTeamQuotaRateLimiter configures region-shared start-rate admission for
// team-owned warm-pool replenishment.
func (pm *PoolManager) SetTeamQuotaRateLimiter(limiter TeamQuotaRateLimiter) {
	if pm == nil {
		return
	}
	pm.teamQuotaLimiter = limiter
}

// ReconcilePool reconciles the idle pool for a template. A positive duration
// requests another reconciliation after a start-limited scale-up.
func (pm *PoolManager) ReconcilePool(ctx context.Context, template *v1alpha1.SandboxTemplate) (time.Duration, error) {
	pm.logger.Info("Reconciling pool",
		zap.String("template", template.Name),
		zap.String("namespace", template.Namespace),
		zap.Int32("minIdle", template.Spec.Pool.MinIdle),
	)

	if _, err := ValidateTeamOwnedTemplate(template); err != nil {
		scaleErr := pm.scaleInvalidTeamPoolToZero(ctx, template)
		if pm.recorder != nil {
			pm.recorder.Eventf(
				template,
				corev1.EventTypeWarning,
				"InvalidTeamTemplateOwnership",
				"Disabled warm pool because team template ownership is invalid: %v",
				err,
			)
		}
		if scaleErr != nil {
			return 0, stderrors.Join(err, scaleErr)
		}
		return 0, err
	}

	desiredTemplateHash, err := TemplateSpecHash(template)
	if err != nil {
		return 0, fmt.Errorf("compute template hash: %w", err)
	}

	// 1. Ensure ReplicaSet exists and is configured correctly
	rs, err := pm.getOrCreateReplicaSet(ctx, template)
	if err != nil {
		return 0, fmt.Errorf("get or create replicaset: %w", err)
	}

	// 2. Adopt the complete physical warm-pool target before any rollout,
	// repair, or replacement can remove a Pod. This is the post-admission
	// fence for RuntimeClass and mutating-webhook overhead.
	quotaHandled, err := pm.reconcileCommittedTeamWarmPoolQuota(ctx, template)
	if err != nil {
		return 0, fmt.Errorf("reconcile observed warm-pool quota: %w", err)
	}
	if quotaHandled {
		return time.Second, nil
	}

	// 3. Ensure newly created pods use the latest template spec hash.
	rs, err = pm.reconcileReplicaSetTemplate(ctx, template, rs, desiredTemplateHash)
	if err != nil {
		return 0, fmt.Errorf("reconcile replicaset template: %w", err)
	}

	// 4. Drain stale idle pods atomically with delete preconditions.
	if err := pm.drainStaleIdlePods(ctx, template, desiredTemplateHash); err != nil {
		if retryAfter, limited := warmPoolReplacementRetryAfter(err); limited {
			pm.recordWarmPoolReplacementThrottle(template, retryAfter, err)
			return retryAfter, nil
		}
		return 0, fmt.Errorf("drain stale idle pods: %w", err)
	}

	// 5. Repair current-hash idle pods that are stuck and will keep the
	// ReplicaSet from creating replacements.
	if err := pm.repairUnhealthyIdlePods(ctx, template, desiredTemplateHash); err != nil {
		if retryAfter, limited := warmPoolReplacementRetryAfter(err); limited {
			pm.recordWarmPoolReplacementThrottle(template, retryAfter, err)
			return retryAfter, nil
		}
		return 0, fmt.Errorf("repair unhealthy idle pods: %w", err)
	}

	// 6. Keep the warm pool fixed at minIdle. Template autoscaling is disabled
	// because burst cold claims can already stress the data plane; expanding the
	// idle pool at the same time compounds that pressure and may create unused
	// pods after the burst ends.
	currentReplicas := getInt32Value(rs.Spec.Replicas)
	desiredReplicas := desiredPoolReplicas(template)
	if rs.Spec.Replicas == nil || currentReplicas != desiredReplicas {
		return pm.reconcileReplicaSetReplicas(ctx, template, rs, desiredReplicas)
	}
	if err := pm.reconcileTeamWarmPoolQuota(ctx, template, desiredReplicas); err != nil {
		return 0, err
	}

	return 0, nil
}

func (pm *PoolManager) scaleInvalidTeamPoolToZero(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
) error {
	if pm == nil || pm.k8sClient == nil || template == nil {
		return fmt.Errorf("disable invalid team warm pool: Kubernetes client and template are required")
	}
	rsName, err := naming.ReplicasetName(
		naming.ClusterIDOrDefault(template.Spec.ClusterId),
		template.Name,
	)
	if err != nil {
		return fmt.Errorf("disable invalid team warm pool: generate ReplicaSet name: %w", err)
	}
	rs, err := pm.k8sClient.AppsV1().
		ReplicaSets(template.Namespace).
		Get(ctx, rsName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("disable invalid team warm pool: get ReplicaSet: %w", err)
	}
	if getInt32Value(rs.Spec.Replicas) == 0 {
		return nil
	}
	if _, err := pm.updateReplicaSetReplicas(ctx, template.Namespace, rsName, 0); err != nil {
		return fmt.Errorf("disable invalid team warm pool: scale ReplicaSet to zero: %w", err)
	}
	return nil
}

// reconcileReplicaSetReplicas updates the warm-pool size without admitting more
// concurrent starts than the claim start limiter currently allows.
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
		return 0, pm.reconcileTeamWarmPoolQuota(ctx, template, desiredReplicas)
	}
	recoveryHandled, err := pm.reconcileCommittedTeamWarmPoolQuota(ctx, template)
	if err != nil {
		return 0, err
	}
	if recoveryHandled {
		return time.Second, nil
	}

	var quotaReservation *teamquota.Reservation
	quotaCommitted := false
	physicalMutationAttempted := false
	defer func() {
		if quotaReservation == nil || quotaCommitted || physicalMutationAttempted {
			return
		}
		abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := pm.teamQuotaStore.Abort(
			abortCtx,
			teamquota.Ref(quotaReservation.Owner, quotaReservation.Operation),
			"warm-pool ReplicaSet update failed",
		); err != nil {
			pm.logger.Error("Failed to abort warm-pool team quota reservation",
				zap.String("template", template.Name),
				zap.Error(err),
			)
		}
	}()

	pm.logger.Info("Updating ReplicaSet replicas",
		zap.String("template", template.Name),
		zap.Int32("current", currentReplicas),
		zap.Int32("desired", desiredReplicas),
	)

	submittedTarget := currentReplicas
	rateRequeueAfter := time.Duration(0)
	updateReplicas := func(target int32) func(context.Context) error {
		return func(updateCtx context.Context) error {
			admitted, retryAfter, err := pm.admitTeamWarmPoolStarts(
				updateCtx,
				template,
				target-currentReplicas,
			)
			if err != nil {
				return err
			}
			if admitted <= 0 && target > currentReplicas {
				return &teamWarmPoolRateLimitError{retryAfter: retryAfter}
			}
			actualTarget := target
			if target > currentReplicas {
				actualTarget = currentReplicas + admitted
			}
			quotaReservation, err = pm.reserveTeamWarmPoolQuota(
				updateCtx,
				template,
				actualTarget,
			)
			if err != nil {
				pm.recorder.Eventf(template, corev1.EventTypeWarning, "TeamQuotaExceeded",
					"Cannot resize the team warm pool: %v", err)
				return fmt.Errorf("reserve team warm-pool quota: %w", err)
			}
			// Once the Kubernetes mutation is submitted, an ambiguous response
			// must leave the reservation pending for crash recovery.
			physicalMutationAttempted = true
			_, err = pm.updateReplicaSetReplicas(
				updateCtx,
				template.Namespace,
				rs.Name,
				actualTarget,
			)
			if err == nil {
				submittedTarget = actualTarget
				rateRequeueAfter = retryAfter
			}
			return err
		}
	}
	recordUpdateFailure := func(err error) (time.Duration, error) {
		pm.recorder.Eventf(template, corev1.EventTypeWarning, "ReplicaSetUpdateFailed",
			"Failed to update ReplicaSet: %v", err)
		return 0, fmt.Errorf("update replicaset: %w", err)
	}

	if desiredReplicas <= currentReplicas || pm.claimStartLimiter == nil {
		if err := updateReplicas(desiredReplicas)(ctx); err != nil {
			if retryAfter, limited := teamWarmPoolRateRetryAfter(err); limited {
				pm.recordTeamWarmPoolRateThrottle(
					template,
					currentReplicas,
					desiredReplicas,
					retryAfter,
				)
				return retryAfter, nil
			}
			return recordUpdateFailure(err)
		}
		if err := pm.commitTeamWarmPoolQuota(ctx, quotaReservation); err != nil {
			return 0, err
		}
		quotaCommitted = true
		if rateRequeueAfter > 0 {
			pm.recordTeamWarmPoolRateThrottle(
				template,
				submittedTarget,
				desiredReplicas,
				rateRequeueAfter,
			)
			return rateRequeueAfter, nil
		}
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetUpdated",
			"Updated ReplicaSet replicas to %d", desiredReplicas)
		return 0, nil
	}

	requested := desiredReplicas - currentReplicas
	_, err = pm.claimStartLimiter.Admit(ctx, startlimiter.ReasonPoolReconcile, requested, updateReplicas(desiredReplicas))
	if err == nil {
		if err := pm.commitTeamWarmPoolQuota(ctx, quotaReservation); err != nil {
			return 0, err
		}
		quotaCommitted = true
		if rateRequeueAfter > 0 {
			pm.recordTeamWarmPoolRateThrottle(
				template,
				submittedTarget,
				desiredReplicas,
				rateRequeueAfter,
			)
			return rateRequeueAfter, nil
		}
		pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetUpdated",
			"Updated ReplicaSet replicas to %d", desiredReplicas)
		return 0, nil
	}
	if retryAfter, limited := teamWarmPoolRateRetryAfter(err); limited {
		pm.recordTeamWarmPoolRateThrottle(
			template,
			currentReplicas,
			desiredReplicas,
			retryAfter,
		)
		return retryAfter, nil
	}
	if !stderrors.Is(err, startlimiter.ErrThrottled) {
		return recordUpdateFailure(err)
	}

	retryAfter := startlimiter.RetryAfter(err)
	var throttled *startlimiter.ThrottledError
	if !stderrors.As(err, &throttled) || throttled.Snapshot.Available <= 0 {
		pm.recordReplicaSetScaleUpThrottle(template, currentReplicas, desiredReplicas, retryAfter, err)
		return retryAfter, nil
	}

	batchSize := min(requested, throttled.Snapshot.Available)
	batchTarget := currentReplicas + batchSize
	_, err = pm.claimStartLimiter.Admit(ctx, startlimiter.ReasonPoolReconcile, batchSize, updateReplicas(batchTarget))
	if err != nil {
		if rateRetryAfter, limited := teamWarmPoolRateRetryAfter(err); limited {
			pm.recordTeamWarmPoolRateThrottle(
				template,
				currentReplicas,
				desiredReplicas,
				rateRetryAfter,
			)
			return rateRetryAfter, nil
		}
		if stderrors.Is(err, startlimiter.ErrThrottled) {
			retryAfter = startlimiter.RetryAfter(err)
			pm.recordReplicaSetScaleUpThrottle(template, currentReplicas, desiredReplicas, retryAfter, err)
			return retryAfter, nil
		}
		return recordUpdateFailure(err)
	}
	// Commit exactly the ReplicaSet target admitted by both the cluster guard
	// and Team Quota capacity and start-rate enforcement.
	if err := pm.commitTeamWarmPoolQuota(ctx, quotaReservation); err != nil {
		return 0, err
	}
	quotaCommitted = true
	if rateRequeueAfter > retryAfter {
		retryAfter = rateRequeueAfter
	}

	pm.logger.Info("Scaled ReplicaSet within available claim start budget",
		zap.String("template", template.Name),
		zap.Int32("current", currentReplicas),
		zap.Int32("updated", submittedTarget),
		zap.Int32("desired", desiredReplicas),
		zap.Duration("requeueAfter", retryAfter),
	)
	pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetScaleUpBatched",
		"Updated ReplicaSet replicas to %d of desired %d; retrying in %s", submittedTarget, desiredReplicas, retryAfter)
	return retryAfter, nil
}

func (pm *PoolManager) admitTeamWarmPoolStarts(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	count int32,
) (int32, time.Duration, error) {
	if count <= 0 {
		return max(count, 0), 0, nil
	}
	owner, ok := TeamWarmPoolQuotaOwner(template)
	if !ok {
		return count, 0, nil
	}
	if pm == nil || pm.teamQuotaLimiter == nil {
		return 0, 0, &teamquota.UnavailableError{
			Operation: "admit team warm-pool starts",
			Err:       fmt.Errorf("rate limiter is not configured"),
		}
	}
	remaining := int64(count)
	cost := remaining
	var admitted int64
	for remaining > 0 {
		decision, err := pm.teamQuotaLimiter.Take(
			ctx,
			owner.TeamID,
			teamquota.KeySandboxStarts,
			cost,
		)
		if err != nil {
			if stderrors.Is(err, tokenbucket.ErrCostExceedsBurst) && cost > 1 {
				cost = 1
				continue
			}
			return 0, 0, fmt.Errorf("admit team warm-pool starts: %w", err)
		}
		if decision.Allowed {
			admitted += cost
			remaining -= cost
			if remaining == 0 {
				return int32(admitted), 0, nil
			}
			cost = min(remaining, max(decision.Remaining, 1))
			continue
		}
		if decision.Remaining > 0 {
			nextCost := min(remaining, decision.Remaining)
			if nextCost < cost {
				cost = nextCost
				continue
			}
		}
		retryAfter := decision.RetryAfter
		if retryAfter <= 0 {
			retryAfter = time.Second
		}
		return int32(admitted), retryAfter, nil
	}
	return int32(admitted), 0, nil
}

func teamWarmPoolRateRetryAfter(err error) (time.Duration, bool) {
	var limited *teamWarmPoolRateLimitError
	if !stderrors.As(err, &limited) || limited == nil {
		return 0, false
	}
	return limited.retryAfter, true
}

func (pm *PoolManager) reserveTeamWarmPoolQuota(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	desiredReplicas int32,
) (*teamquota.Reservation, error) {
	owner, ok := TeamWarmPoolQuotaOwner(template)
	if !ok {
		return nil, nil
	}
	if pm == nil || pm.teamQuotaStore == nil || pm.quotaResources == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "reserve team warm-pool capacity",
			Err:       fmt.Errorf("capacity accounting is not configured"),
		}
	}
	target, err := pm.teamWarmPoolQuotaTarget(ctx, template, desiredReplicas)
	if err != nil {
		return nil, err
	}
	return pm.teamQuotaStore.ReserveTarget(ctx, teamquota.ReserveRequest{
		Owner: owner,
		Operation: teamquota.Operation{
			ID:   uuid.NewString(),
			Kind: "scale_warm_pool",
		},
		Target: target,
	})
}

func (pm *PoolManager) reconcileTeamWarmPoolQuota(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	desiredReplicas int32,
) error {
	recoveryHandled, err := pm.reconcileCommittedTeamWarmPoolQuota(ctx, template)
	if err != nil {
		return err
	}
	if recoveryHandled {
		return nil
	}
	reservation, err := pm.reserveTeamWarmPoolQuota(ctx, template, desiredReplicas)
	if err != nil {
		return fmt.Errorf("reserve team warm-pool quota: %w", err)
	}
	return pm.commitTeamWarmPoolQuota(ctx, reservation)
}

// reconcileCommittedTeamWarmPoolQuota adopts the greater of live idle pods
// and the ReplicaSet's current replica commitment. The latter prevents an
// already admitted scale-up from losing its hold while pods are still Pending.
func (pm *PoolManager) reconcileCommittedTeamWarmPoolQuota(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
) (bool, error) {
	owner, ok := TeamWarmPoolQuotaOwner(template)
	if !ok {
		return false, nil
	}
	if pm == nil || pm.teamQuotaStore == nil || pm.quotaResources == nil {
		return false, &teamquota.UnavailableError{
			Operation: "reconcile team warm-pool capacity",
			Err:       fmt.Errorf("capacity accounting is not configured"),
		}
	}
	inventoryStore, ok := pm.teamQuotaStore.(teamQuotaInventoryStore)
	if !ok {
		return false, &teamquota.UnavailableError{
			Operation: "reconcile team warm-pool capacity",
			Err:       fmt.Errorf("revision-fenced inventory accounting is not configured"),
		}
	}
	allocation, err := inventoryStore.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		return false, fmt.Errorf("capture team warm-pool quota revision: %w", err)
	}
	expectedRevision := int64(0)
	if allocation != nil {
		expectedRevision = allocation.Revision
	}
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return false, fmt.Errorf("resolve team warm-pool ReplicaSet: %w", err)
	}
	var committedReplicas int32
	rs, err := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rsName, metav1.GetOptions{})
	switch {
	case err == nil:
		committedReplicas = getInt32Value(rs.Spec.Replicas)
	case apierrors.IsNotFound(err):
	default:
		return false, fmt.Errorf("get team warm-pool ReplicaSet: %w", err)
	}
	target, err := pm.teamWarmPoolQuotaTarget(ctx, template, committedReplicas)
	if err != nil {
		return false, fmt.Errorf("compute committed team warm-pool quota: %w", err)
	}
	recoveryHandled := false
	// A physical Pod may exceed the admitted target after RuntimeClass or
	// mutating admission. Finalize that observed complete target before cleanup
	// so the pending operation can never undercount the live pool.
	if allocation != nil && allocation.Operation != nil {
		if _, exceeds := teamQuotaTargetExcess(target, allocation.Pending); !exceeds {
			// A separate recovery pass will decide whether an in-bound
			// ambiguous mutation committed or aborted. Do not overlap another
			// warm-pool mutation while its reservation remains pending.
			return true, nil
		}
		observedStore, ok := pm.teamQuotaStore.(teamquota.ObservedExactCapacityStore)
		if !ok || observedStore == nil {
			return false, &teamquota.UnavailableError{
				Operation: "commit observed warm-pool capacity",
				Err:       fmt.Errorf("observed exact capacity store is not configured"),
			}
		}
		if err := observedStore.CommitObservedExact(
			ctx,
			teamquota.Ref(owner, *allocation.Operation),
			target,
		); err != nil {
			return false, fmt.Errorf("commit observed warm-pool capacity: %w", err)
		}
		recoveryHandled = true
	} else {
		applied, err := inventoryStore.ReconcileTargetIfRevision(
			ctx,
			owner,
			target,
			teamquota.RuntimeRef{},
			expectedRevision,
		)
		if err != nil {
			return false, fmt.Errorf("reconcile committed team warm-pool quota: %w", err)
		}
		if !applied {
			pm.logger.Debug(
				"Skipped stale team warm-pool inventory observation",
				zap.String("team_id", owner.TeamID),
				zap.String("owner_id", owner.ID),
				zap.Int64("expected_revision", expectedRevision),
			)
			return false, nil
		}
	}
	if err := pm.clearTerminalWarmPoolTransferMarkers(ctx, template, rsName); err != nil {
		return false, fmt.Errorf("clear terminal warm-pool transfer markers: %w", err)
	}
	oversizedPods, err := pm.unexpectedWarmPoolQuotaPods(ctx, template, rs)
	if err != nil {
		return false, err
	}
	if len(oversizedPods) == 0 {
		return recoveryHandled, nil
	}
	if rs == nil {
		return false, fmt.Errorf("cleanup oversized warm-pool pods: ReplicaSet is unavailable")
	}
	targetReplicas := max(getInt32Value(rs.Spec.Replicas)-int32(len(oversizedPods)), 0)
	if _, err := pm.updateReplicaSetReplicas(ctx, rs.Namespace, rs.Name, targetReplicas); err != nil {
		return false, fmt.Errorf("scale down oversized warm-pool runtimes: %w", err)
	}
	for _, pod := range oversizedPods {
		uid := pod.UID
		preconditions := &metav1.Preconditions{UID: &uid}
		if resourceVersion := strings.TrimSpace(pod.ResourceVersion); resourceVersion != "" {
			preconditions.ResourceVersion = &resourceVersion
		}
		if err := pm.k8sClient.CoreV1().Pods(pod.Namespace).Delete(
			ctx,
			pod.Name,
			metav1.DeleteOptions{Preconditions: preconditions},
		); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete oversized warm-pool pod %s: %w", pod.Name, err)
		}
	}
	pm.recorder.Eventf(
		template,
		corev1.EventTypeWarning,
		"WarmPoolRuntimeQuotaMismatch",
		"Counted and removed %d warm-pool runtime(s) whose admitted Pod resources exceeded the reserved target",
		len(oversizedPods),
	)
	return true, nil
}

func (pm *PoolManager) clearTerminalWarmPoolTransferMarkers(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	replicaSetName string,
) error {
	if pm == nil || pm.k8sClient == nil || template == nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs, err := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).
			Get(ctx, replicaSetName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		raw := ""
		if rs.Annotations != nil {
			raw = strings.TrimSpace(rs.Annotations[AnnotationTeamQuotaWarmPoolTransfers])
		}
		if raw == "" {
			return nil
		}
		markers := make(map[string]int32)
		if err := json.Unmarshal([]byte(raw), &markers); err != nil {
			return fmt.Errorf("decode transfer markers: %w", err)
		}
		store, ok := pm.teamQuotaStore.(teamquota.TransferStateStore)
		if !ok || store == nil {
			return &teamquota.UnavailableError{
				Operation: "garbage collect warm-pool transfer markers",
				Err:       fmt.Errorf("transfer state store is not configured"),
			}
		}
		operationIDs := make([]string, 0, len(markers))
		for operationID := range markers {
			operationIDs = append(operationIDs, operationID)
		}
		states, err := store.TransferStates(
			ctx,
			strings.TrimSpace(TeamOwnedTemplateTeamID(template)),
			operationIDs,
		)
		if err != nil {
			return err
		}
		changed := false
		for operationID := range markers {
			switch states[operationID] {
			case "prepared":
			case "", "committed", "aborted":
				delete(markers, operationID)
				changed = true
			default:
				return fmt.Errorf(
					"transfer %s has unknown state %q",
					operationID,
					states[operationID],
				)
			}
		}
		if !changed {
			return nil
		}
		updated := rs.DeepCopy()
		if len(markers) == 0 {
			delete(updated.Annotations, AnnotationTeamQuotaWarmPoolTransfers)
		} else {
			encoded, err := json.Marshal(markers)
			if err != nil {
				return fmt.Errorf("encode transfer markers: %w", err)
			}
			updated.Annotations[AnnotationTeamQuotaWarmPoolTransfers] = string(encoded)
		}
		_, err = pm.k8sClient.AppsV1().ReplicaSets(updated.Namespace).
			Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

func (pm *PoolManager) commitTeamWarmPoolQuota(
	ctx context.Context,
	reservation *teamquota.Reservation,
) error {
	if reservation == nil {
		return nil
	}
	if pm == nil || pm.teamQuotaStore == nil {
		return &teamquota.UnavailableError{
			Operation: "commit team warm-pool capacity",
			Err:       fmt.Errorf("capacity store is not configured"),
		}
	}
	if err := pm.teamQuotaStore.Commit(ctx, teamquota.Ref(reservation.Owner, reservation.Operation)); err != nil {
		return fmt.Errorf("commit team warm-pool quota: %w", err)
	}
	return nil
}

func (pm *PoolManager) teamWarmPoolQuotaTarget(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	desiredReplicas int32,
) (teamquota.Values, error) {
	if desiredReplicas < 0 {
		return nil, fmt.Errorf("desired replicas must be non-negative")
	}
	selector := labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	})
	podList, err := pm.k8sClient.CoreV1().Pods(template.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list team warm-pool pods: %w", err)
	}
	target := teamquota.Values{
		teamquota.KeySandboxRuntimeCount:          0,
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
	activeTarget := target.Clone()
	terminatingTarget := target.Clone()
	var active, terminating int32
	for i := range podList.Items {
		pod := &podList.Items[i]
		if strings.TrimSpace(pod.Annotations[AnnotationTeamID]) != TeamOwnedTemplateTeamID(template) ||
			pod.Annotations[AnnotationOwnerKind] != OwnerKindTeamWarmPool {
			continue
		}
		if pod.DeletionTimestamp == nil {
			active++
			addTeamQuotaValues(activeTarget, pm.quotaResources(&pod.Spec))
			continue
		}
		terminating++
		addTeamQuotaValues(terminatingTarget, pm.quotaResources(&pod.Spec))
	}
	// Keep the full desired-spec commitment while active pods are still being
	// created. Terminating pods are added separately because replacements may
	// already exist before their predecessors leave the node.
	desiredTarget := teamquota.Values{
		teamquota.KeySandboxRuntimeCount:          int64(desiredReplicas),
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
	spec := v1alpha1.BuildIdlePodSpec(template)
	perPod, err := pm.desiredPodQuotaResources(ctx, &spec)
	if err != nil {
		return nil, fmt.Errorf("resolve desired warm-pool pod quota: %w", err)
	}
	for i := int32(0); i < desiredReplicas; i++ {
		addTeamQuotaValues(desiredTarget, perPod)
	}
	for key, value := range desiredTarget {
		if value > activeTarget[key] {
			activeTarget[key] = value
		}
	}
	activeTarget[teamquota.KeySandboxRuntimeCount] = int64(active)
	if int64(desiredReplicas) > activeTarget[teamquota.KeySandboxRuntimeCount] {
		activeTarget[teamquota.KeySandboxRuntimeCount] = int64(desiredReplicas)
	}
	activeTarget[teamquota.KeySandboxRuntimeCount] += int64(terminating)
	addTeamQuotaValues(activeTarget, terminatingTarget)
	return activeTarget, nil
}

func addTeamQuotaValues(target, values teamquota.Values) {
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		target[key] += values[key]
	}
}

func (pm *PoolManager) desiredPodQuotaResources(
	ctx context.Context,
	spec *corev1.PodSpec,
) (teamquota.Values, error) {
	if pm == nil || pm.quotaResources == nil {
		return nil, fmt.Errorf("quota resource calculator is not configured")
	}
	quotaSpec, err := runtimeclassquota.ResolvePodSpec(ctx, pm.k8sClient, spec)
	if err != nil {
		return nil, err
	}
	return pm.quotaResources(quotaSpec), nil
}

func (pm *PoolManager) unexpectedWarmPoolQuotaPods(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	rs *appsv1.ReplicaSet,
) ([]corev1.Pod, error) {
	spec := v1alpha1.BuildIdlePodSpec(template)
	expected, err := pm.desiredPodQuotaResources(ctx, &spec)
	if err != nil {
		return nil, fmt.Errorf("resolve expected warm-pool pod quota: %w", err)
	}
	templateHash, err := TemplateSpecHash(template)
	if err != nil {
		return nil, fmt.Errorf("compute warm-pool template hash: %w", err)
	}
	selector := labels.SelectorFromSet(map[string]string{
		LabelTemplateID: template.Name,
		LabelPoolType:   PoolTypeIdle,
	})
	pods, err := pm.k8sClient.CoreV1().Pods(template.Namespace).List(
		ctx,
		metav1.ListOptions{LabelSelector: selector.String()},
	)
	if err != nil {
		return nil, fmt.Errorf("list warm-pool pods for actual quota fence: %w", err)
	}
	teamID := TeamOwnedTemplateTeamID(template)
	var oversized []corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		controllerRef := metav1.GetControllerOf(pod)
		if pod.DeletionTimestamp != nil ||
			pod.Annotations[AnnotationTemplateSpecHash] != templateHash ||
			strings.TrimSpace(pod.Annotations[AnnotationTeamID]) != teamID ||
			pod.Annotations[AnnotationOwnerKind] != OwnerKindTeamWarmPool ||
			rs == nil ||
			controllerRef == nil ||
			controllerRef.Kind != "ReplicaSet" ||
			controllerRef.Name != rs.Name ||
			(rs.UID != "" && controllerRef.UID != rs.UID) {
			continue
		}
		if _, exceeds := teamQuotaTargetExcess(pm.quotaResources(&pod.Spec), expected); exceeds {
			oversized = append(oversized, *pod.DeepCopy())
		}
	}
	return oversized, nil
}

func teamQuotaTargetExcess(observed, admitted teamquota.Values) (teamquota.Key, bool) {
	for _, key := range observed.Keys() {
		if observed[key] > admitted[key] {
			return key, true
		}
	}
	return "", false
}

// TeamWarmPoolQuotaOwner returns the aggregate quota owner for a team-owned
// template warm pool.
func TeamWarmPoolQuotaOwner(template *v1alpha1.SandboxTemplate) (teamquota.Owner, bool) {
	teamID := strings.TrimSpace(TeamOwnedTemplateTeamID(template))
	if teamID == "" {
		return teamquota.Owner{}, false
	}
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	return teamquota.Owner{
		TeamID:    teamID,
		Kind:      "warm_pool",
		ID:        clusterID + "/" + TemplateLogicalID(template),
		ClusterID: clusterID,
	}, true
}

func (pm *PoolManager) recordReplicaSetScaleUpThrottle(
	template *v1alpha1.SandboxTemplate,
	currentReplicas int32,
	desiredReplicas int32,
	retryAfter time.Duration,
	err error,
) {
	pm.logger.Info("Delaying ReplicaSet scale-up due to claim start limit",
		zap.String("template", template.Name),
		zap.Int32("current", currentReplicas),
		zap.Int32("desired", desiredReplicas),
		zap.Duration("requeueAfter", retryAfter),
		zap.Error(err),
	)
	pm.recorder.Eventf(template, corev1.EventTypeNormal, "ReplicaSetScaleUpThrottled",
		"Delayed ReplicaSet scale-up to %d replicas for %s: %v", desiredReplicas, retryAfter, err)
}

func (pm *PoolManager) recordTeamWarmPoolRateThrottle(
	template *v1alpha1.SandboxTemplate,
	currentReplicas int32,
	desiredReplicas int32,
	retryAfter time.Duration,
) {
	pm.logger.Info("Delaying team warm-pool scale-up due to Team Quota rate limit",
		zap.String("template", template.Name),
		zap.Int32("current", currentReplicas),
		zap.Int32("desired", desiredReplicas),
		zap.Duration("requeueAfter", retryAfter),
	)
	pm.recorder.Eventf(template, corev1.EventTypeNormal, "TeamQuotaRateLimited",
		"Delayed team warm-pool scale-up to %d replicas for %s",
		desiredReplicas,
		retryAfter,
	)
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
		AnnotationTemplateSpecHash:             specHash,
		AnnotationClusterAutoscalerSafeToEvict: "true",
		AnnotationClusterID:                    naming.ClusterIDOrDefault(template.Spec.ClusterId),
	}
	labels := map[string]string{
		LabelTemplateID:        template.Name,
		LabelTemplateLogicalID: TemplateLogicalID(template),
		LabelPoolType:          PoolTypeIdle,
	}
	if teamID := TeamOwnedTemplateTeamID(template); teamID != "" {
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
	teamID := TeamOwnedTemplateTeamID(template)
	if teamID == "" {
		return nil
	}
	return &warmPoolTemplateMetadata{
		TeamID:    teamID,
		UserID:    teamOwnedTemplateUserID(template),
		OwnerKind: OwnerKindTeamWarmPool,
	}
}

// ValidateTeamOwnedTemplate validates the scheduler-projected identity of a
// team-scoped template. Public templates return an empty owner without error.
func ValidateTeamOwnedTemplate(template *v1alpha1.SandboxTemplate) (string, error) {
	if template == nil {
		return "", fmt.Errorf("template is required")
	}
	if template.Labels[LabelTemplateScope] != naming.ScopeTeam {
		return "", nil
	}
	teamID := strings.TrimSpace(template.Annotations[AnnotationTemplateTeamID])
	if teamID == "" {
		return "", fmt.Errorf("team-scoped template is missing %s", AnnotationTemplateTeamID)
	}
	expectedNamespace, err := naming.TemplateNamespaceForTeam(teamID)
	if err != nil {
		return "", fmt.Errorf("derive team template namespace: %w", err)
	}
	if namespace := strings.TrimSpace(template.Namespace); namespace != "" && namespace != expectedNamespace {
		return "", fmt.Errorf(
			"team-scoped template namespace %q does not match owner %q",
			namespace,
			teamID,
		)
	}
	logicalID := strings.TrimSpace(template.Labels[LabelTemplateLogicalID])
	if logicalID != "" {
		expectedName := naming.TemplateNameForCluster(naming.ScopeTeam, teamID, logicalID)
		if template.Name != expectedName {
			return "", fmt.Errorf(
				"team-scoped template name %q does not match owner %q and logical ID %q",
				template.Name,
				teamID,
				logicalID,
			)
		}
	}
	return teamID, nil
}

// TeamOwnedTemplateTeamID returns the owning team for a valid team-scoped
// template. Callers that make an admission decision must use
// ValidateTeamOwnedTemplate so malformed ownership cannot fail open.
func TeamOwnedTemplateTeamID(template *v1alpha1.SandboxTemplate) string {
	if template == nil || template.Labels[LabelTemplateScope] != naming.ScopeTeam {
		return ""
	}
	return strings.TrimSpace(template.Annotations[AnnotationTemplateTeamID])
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
	liveRS, err := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get replicaset for template rollout: %w", err)
	}
	rs = liveRS
	currentTemplateHash := rs.Spec.Template.Annotations[AnnotationTemplateSpecHash]
	if currentTemplateHash == desiredTemplateHash {
		return rs, nil
	}

	newTemplate, err := pm.buildPodTemplate(template, desiredTemplateHash)
	if err != nil {
		return nil, fmt.Errorf("build pod template: %w", err)
	}

	var quotaReservation *teamquota.Reservation
	quotaCommitted := false
	rolloutMayExist := false
	if _, teamOwned := TeamWarmPoolQuotaOwner(template); teamOwned {
		committedReplicas := max(getInt32Value(rs.Spec.Replicas), desiredPoolReplicas(template))
		quotaReservation, err = pm.reserveTeamWarmPoolQuota(
			ctx,
			template,
			committedReplicas,
		)
		if err != nil {
			return nil, fmt.Errorf("reserve updated warm-pool template quota: %w", err)
		}
		defer func() {
			if quotaReservation == nil || quotaCommitted || rolloutMayExist {
				return
			}
			abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if abortErr := pm.teamQuotaStore.Abort(
				abortCtx,
				teamquota.Ref(quotaReservation.Owner, quotaReservation.Operation),
				"warm-pool template update failed before mutation",
			); abortErr != nil {
				pm.logger.Error("Failed to abort warm-pool rollout quota reservation",
					zap.String("template", template.Name),
					zap.Error(abortErr),
				)
			}
		}()
	}

	var updatedRS *appsv1.ReplicaSet
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Get(ctx, rs.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.Spec.Template.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			rolloutMayExist = true
			updatedRS = current
			return nil
		}
		updated := current.DeepCopy()
		updated.Spec.Template = newTemplate
		// An error after submitting this update is ambiguous. Keep the quota
		// reservation pending so recovery cannot undercount an applied rollout.
		rolloutMayExist = true
		updatedRS, err = pm.k8sClient.AppsV1().ReplicaSets(template.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := pm.commitTeamWarmPoolQuota(ctx, quotaReservation); err != nil {
		return nil, err
	}
	quotaCommitted = true

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
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			continue
		}
		deleted, err := pm.admitWarmPoolReplacement(ctx, template, func(
			admitCtx context.Context,
			beforeDelete func(context.Context, *corev1.Pod) error,
		) (bool, error) {
			return pm.deleteStaleIdlePodWithRetry(
				admitCtx,
				template.Namespace,
				pod.Name,
				desiredTemplateHash,
				beforeDelete,
			)
		})
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
		deleted, err := pm.admitWarmPoolReplacement(ctx, template, func(
			admitCtx context.Context,
			beforeDelete func(context.Context, *corev1.Pod) error,
		) (bool, error) {
			return pm.deleteUnhealthyIdlePodWithRetry(
				admitCtx,
				template.Namespace,
				pod.Name,
				desiredTemplateHash,
				beforeDelete,
			)
		})
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

func (pm *PoolManager) admitWarmPoolReplacement(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	deletePod func(context.Context, func(context.Context, *corev1.Pod) error) (bool, error),
) (bool, error) {
	if deletePod == nil {
		return false, nil
	}
	deleted := false
	rateAdmitted := false
	capacityCommitted := false
	mutate := func(admitCtx context.Context) error {
		var err error
		deleted, err = deletePod(admitCtx, func(deleteCtx context.Context, pod *corev1.Pod) error {
			if rateAdmitted && capacityCommitted {
				return nil
			}
			reservation, reserveErr := pm.reserveTeamWarmPoolReplacementQuota(deleteCtx, template, pod)
			if reserveErr != nil {
				return reserveErr
			}
			admitted, retryAfter, rateErr := pm.admitTeamWarmPoolStarts(deleteCtx, template, 1)
			if rateErr != nil {
				return pm.abortWarmPoolReplacementQuota(deleteCtx, reservation, rateErr)
			}
			if admitted < 1 {
				return pm.abortWarmPoolReplacementQuota(
					deleteCtx,
					reservation,
					&teamWarmPoolRateLimitError{retryAfter: retryAfter},
				)
			}
			rateAdmitted = true
			if err := pm.commitTeamWarmPoolQuota(deleteCtx, reservation); err != nil {
				return err
			}
			capacityCommitted = true
			return nil
		})
		return err
	}
	if pm.claimStartLimiter == nil {
		// Manager startup validation requires this dependency. Keeping the
		// direct path supports isolated unit tests for public templates.
		return deleted, mutate(ctx)
	}
	_, err := pm.claimStartLimiter.Admit(ctx, startlimiter.ReasonPoolReconcile, 1, mutate)
	return deleted, err
}

func (pm *PoolManager) reserveTeamWarmPoolReplacementQuota(
	ctx context.Context,
	template *v1alpha1.SandboxTemplate,
	pod *corev1.Pod,
) (*teamquota.Reservation, error) {
	owner, ok := TeamWarmPoolQuotaOwner(template)
	if !ok {
		return nil, nil
	}
	if pm == nil || pm.teamQuotaStore == nil || pm.quotaResources == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "reserve team warm-pool replacement capacity",
			Err:       fmt.Errorf("capacity accounting is not configured"),
		}
	}
	if pod == nil {
		return nil, fmt.Errorf("warm-pool replacement pod is required")
	}
	replacementSpec := v1alpha1.BuildIdlePodSpec(template)
	replacementResources, err := pm.desiredPodQuotaResources(ctx, &replacementSpec)
	if err != nil {
		return nil, fmt.Errorf("resolve replacement warm-pool pod quota: %w", err)
	}
	delta := teamquota.Values{
		teamquota.KeySandboxRuntimeCount:          1,
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
	addTeamQuotaValues(delta, replacementResources)
	reservation, err := pm.teamQuotaStore.ReserveDelta(ctx, teamquota.DeltaRequest{
		Owner: owner,
		Operation: teamquota.Operation{
			ID:   uuid.NewString(),
			Kind: "replace_warm_pool_pod",
		},
		Delta: delta,
	})
	if err != nil {
		return nil, fmt.Errorf("reserve team warm-pool replacement capacity: %w", err)
	}
	return reservation, nil
}

func (pm *PoolManager) abortWarmPoolReplacementQuota(
	ctx context.Context,
	reservation *teamquota.Reservation,
	cause error,
) error {
	if reservation == nil {
		return cause
	}
	if pm == nil || pm.teamQuotaStore == nil {
		return stderrors.Join(cause, fmt.Errorf("abort team warm-pool replacement quota: capacity store is not configured"))
	}
	if err := pm.teamQuotaStore.Abort(
		ctx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		"warm-pool replacement was not admitted",
	); err != nil {
		return stderrors.Join(cause, fmt.Errorf("abort team warm-pool replacement quota: %w", err))
	}
	return cause
}

func warmPoolReplacementRetryAfter(err error) (time.Duration, bool) {
	if retryAfter, limited := teamWarmPoolRateRetryAfter(err); limited {
		return retryAfter, true
	}
	if stderrors.Is(err, startlimiter.ErrThrottled) {
		return startlimiter.RetryAfter(err), true
	}
	return 0, false
}

func (pm *PoolManager) recordWarmPoolReplacementThrottle(
	template *v1alpha1.SandboxTemplate,
	retryAfter time.Duration,
	err error,
) {
	pm.logger.Info("Delaying warm-pool replacement due to start admission",
		zap.String("template", template.Name),
		zap.Duration("requeueAfter", retryAfter),
		zap.Error(err),
	)
	pm.recorder.Eventf(template, corev1.EventTypeNormal, "WarmPoolReplacementThrottled",
		"Delayed warm-pool replacement for %s: %v", retryAfter, err)
}

func (pm *PoolManager) deleteUnhealthyIdlePodWithRetry(
	ctx context.Context,
	namespace,
	podName,
	desiredTemplateHash string,
	beforeDelete func(context.Context, *corev1.Pod) error,
) (bool, error) {
	deleted := false
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
			pod.Annotations[AnnotationTemplateSpecHash] != desiredTemplateHash ||
			!shouldRepairUnhealthyIdlePod(pod, time.Now()) {
			return nil
		}
		if beforeDelete != nil {
			if err := beforeDelete(ctx, pod); err != nil {
				return err
			}
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

func (pm *PoolManager) deleteStaleIdlePodWithRetry(
	ctx context.Context,
	namespace,
	podName,
	desiredTemplateHash string,
	beforeDelete func(context.Context, *corev1.Pod) error,
) (bool, error) {
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
			pod.Annotations[AnnotationTemplateSpecHash] == desiredTemplateHash {
			return nil
		}
		if beforeDelete != nil {
			if err := beforeDelete(ctx, pod); err != nil {
				return err
			}
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
	payload := struct {
		PodSpec  corev1.PodSpec            `json:"podSpec"`
		WarmPool *warmPoolTemplateMetadata `json:"warmPool,omitempty"`
	}{
		PodSpec:  podSpec,
		WarmPool: teamWarmPoolTemplateMetadata(template),
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
