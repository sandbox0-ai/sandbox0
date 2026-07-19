package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeclassquota"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	defaultTeamQuotaRecoveryInterval       = 30 * time.Second
	defaultTeamQuotaRecoveryStaleAfter     = 5 * time.Minute
	defaultTeamQuotaRecoveryBatchSize      = 100
	internalTeamQuotaTransferOperationKind = "team_quota_transfer"
)

// TeamQuotaRecoveryConfig controls bounded startup and periodic quota saga
// recovery for one data-plane cluster.
type TeamQuotaRecoveryConfig struct {
	ClusterID  string
	Interval   time.Duration
	StaleAfter time.Duration
	BatchSize  int
}

// TeamQuotaRecoveryController resolves crash-interrupted quota operations
// against current Kubernetes and durable sandbox state.
type TeamQuotaRecoveryController struct {
	service       *SandboxService
	recoveryStore teamquota.RecoveryStore
	config        TeamQuotaRecoveryConfig
	logger        *zap.Logger
}

// NewTeamQuotaRecoveryController creates a cluster-scoped recovery controller.
func NewTeamQuotaRecoveryController(
	service *SandboxService,
	recoveryStore teamquota.RecoveryStore,
	config TeamQuotaRecoveryConfig,
	logger *zap.Logger,
) *TeamQuotaRecoveryController {
	config.ClusterID = naming.ClusterIDOrDefault(&config.ClusterID)
	if config.Interval <= 0 {
		config.Interval = defaultTeamQuotaRecoveryInterval
	}
	if config.StaleAfter <= 0 {
		config.StaleAfter = defaultTeamQuotaRecoveryStaleAfter
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaultTeamQuotaRecoveryBatchSize
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TeamQuotaRecoveryController{
		service:       service,
		recoveryStore: recoveryStore,
		config:        config,
		logger:        logger,
	}
}

// RecoverStartup performs the same fenced recovery pass used periodically.
// Fresh sagas are deliberately retained because another manager replica may
// still own them during a rolling startup.
func (c *TeamQuotaRecoveryController) RecoverStartup(ctx context.Context) error {
	return c.recover(ctx)
}

// RunOnce performs one periodic recovery pass. Only operations whose recovery
// delay has elapsed are touched, so live requests retain their saga window.
func (c *TeamQuotaRecoveryController) RunOnce(ctx context.Context) error {
	return c.recover(ctx)
}

// Run retries periodic recovery until ctx is cancelled. A failed lookup keeps
// quota pending and is retried on the next interval.
func (c *TeamQuotaRecoveryController) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Error("Team quota recovery pass failed", zap.Error(err))
			}
		}
	}
}

func (c *TeamQuotaRecoveryController) recover(ctx context.Context) error {
	if c == nil || c.service == nil {
		return fmt.Errorf("sandbox service is required for team quota recovery")
	}
	if c.recoveryStore == nil {
		return fmt.Errorf("team quota recovery store is required")
	}
	if c.service.teamQuotaStore == nil {
		return fmt.Errorf("team quota capacity store is required for recovery")
	}
	if _, ok := c.service.teamQuotaStore.(teamquota.RevisionReconcileStore); !ok {
		return fmt.Errorf("revision-fenced team quota reconciliation is required for recovery")
	}
	if err := c.recoverPreparedTransfers(ctx); err != nil {
		return err
	}
	if err := c.recoverWarmPoolAllocations(ctx); err != nil {
		return err
	}
	// Capture allocation revisions before external inventory. Exact
	// reconciliation then uses compare-and-swap, so a lifecycle operation that
	// commits during the inventory read cannot be overwritten by that snapshot.
	allocations, err := c.listSandboxAllocations(ctx)
	if err != nil {
		return err
	}
	inventory, err := c.loadSandboxInventory(ctx)
	if err != nil {
		return err
	}
	for i := range allocations {
		allocation := &allocations[i]
		if allocation.Operation == nil ||
			allocation.Operation.Kind == internalTeamQuotaTransferOperationKind {
			continue
		}
		if !recoveryAllocationDue(allocation) {
			continue
		}
		if err := c.recoverSandboxAllocation(ctx, inventory, allocation); err != nil {
			return fmt.Errorf(
				"recover sandbox allocation %s (%s/%s): %w",
				allocation.AllocationID,
				allocation.Owner.TeamID,
				allocation.Owner.ID,
				err,
			)
		}
	}
	if err := c.reconcileSandboxInventory(ctx, inventory, allocations); err != nil {
		return err
	}
	if err := c.service.recoverDueRootFSPublishStages(ctx, c.config.BatchSize); err != nil {
		return fmt.Errorf("recover rootfs publish stages: %w", err)
	}
	return nil
}

type teamQuotaWarmPoolInventory struct {
	templates   map[teamquota.Owner]*v1alpha1.SandboxTemplate
	replicaSets []appsv1.ReplicaSet
	pods        []corev1.Pod
}

func (c *TeamQuotaRecoveryController) recoverWarmPoolAllocations(
	ctx context.Context,
) error {
	allocations, err := c.listRecoveryAllocations(ctx, "warm_pool")
	if err != nil {
		return fmt.Errorf("list recovery warm-pool allocations: %w", err)
	}
	inventory, err := c.loadWarmPoolInventory(ctx)
	if err != nil {
		return err
	}
	for i := range allocations {
		allocation := &allocations[i]
		if allocation.Operation != nil {
			if allocation.Operation.Kind == internalTeamQuotaTransferOperationKind {
				continue
			}
			if !recoveryAllocationDue(allocation) {
				continue
			}
			if allocation.Operation.Kind == "scale_warm_pool" {
				if err := c.recoverWarmPoolScale(ctx, inventory, allocation); err != nil {
					return err
				}
				continue
			}
			if err := c.service.teamQuotaStore.Abort(
				ctx,
				teamquota.Ref(allocation.Owner, *allocation.Operation),
				"recovery found no external mutation for the interrupted warm-pool operation",
			); err != nil {
				return fmt.Errorf(
					"abort interrupted warm-pool allocation %s: %w",
					allocation.AllocationID,
					err,
				)
			}
			continue
		}
		if inventory.hasOwner(allocation.Owner) {
			continue
		}
		if err := c.reconcileTargetIfRevision(
			ctx,
			allocation.Owner,
			zeroWarmPoolTeamQuotaTarget(),
			teamquota.RuntimeRef{},
			allocation.Revision,
		); err != nil {
			return fmt.Errorf(
				"release orphan warm-pool allocation %s: %w",
				allocation.AllocationID,
				err,
			)
		}
	}
	return nil
}

func (c *TeamQuotaRecoveryController) recoverWarmPoolScale(
	ctx context.Context,
	inventory *teamQuotaWarmPoolInventory,
	allocation *teamquota.RecoveryAllocation,
) error {
	if allocation == nil || allocation.Operation == nil {
		return nil
	}
	observed, found, err := inventory.observedTarget(
		ctx,
		c.service.k8sClient,
		allocation.Owner,
	)
	if err != nil {
		return fmt.Errorf(
			"measure interrupted warm-pool allocation %s: %w",
			allocation.AllocationID,
			err,
		)
	}
	if !found {
		// The absence of a ReplicaSet is ambiguous with an informer or control
		// plane delay. Retaining the pending target is conservative for both
		// increases and decreases.
		return nil
	}
	ref := teamquota.Ref(allocation.Owner, *allocation.Operation)
	if quotaValuesEqual(observed, allocation.Pending) ||
		quotaValuesEqual(allocation.Pending, allocation.Committed) {
		if err := c.service.teamQuotaStore.Commit(ctx, ref); err != nil {
			return fmt.Errorf(
				"commit observed warm-pool allocation %s: %w",
				allocation.AllocationID,
				err,
			)
		}
		return nil
	}
	if quotaValuesEqual(observed, allocation.Committed) {
		if err := c.service.teamQuotaStore.Abort(
			ctx,
			ref,
			"recovery observed the pre-mutation warm-pool commitment",
		); err != nil {
			return fmt.Errorf(
				"abort unapplied warm-pool allocation %s: %w",
				allocation.AllocationID,
				err,
			)
		}
		return nil
	}
	store, ok := c.service.teamQuotaStore.(teamquota.ObservedExactCapacityStore)
	if !ok || store == nil {
		return &teamquota.UnavailableError{
			Operation: "commit observed recovered warm-pool capacity",
			Err:       fmt.Errorf("observed exact capacity store is not configured"),
		}
	}
	if err := store.CommitObservedExact(ctx, ref, observed); err != nil {
		return fmt.Errorf(
			"commit observed recovered warm-pool allocation %s: %w",
			allocation.AllocationID,
			err,
		)
	}
	c.logger.Warn(
		"Adopted warm-pool capacity that exceeded its prepared target",
		zap.String("allocation_id", allocation.AllocationID),
		zap.String("team_id", allocation.Owner.TeamID),
		zap.String("owner_id", allocation.Owner.ID),
	)
	return nil
}

func (c *TeamQuotaRecoveryController) loadWarmPoolInventory(
	ctx context.Context,
) (*teamQuotaWarmPoolInventory, error) {
	if c.service.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is required for warm-pool recovery")
	}
	if c.service.templateLister == nil {
		return nil, fmt.Errorf("template lister is required for warm-pool recovery")
	}
	templates, err := c.service.templateLister.List()
	if err != nil {
		return nil, fmt.Errorf("list templates for warm-pool recovery: %w", err)
	}
	replicaSets, err := c.service.k8sClient.AppsV1().
		ReplicaSets("").
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list ReplicaSets for warm-pool recovery: %w", err)
	}
	pods, err := c.service.k8sClient.CoreV1().
		Pods("").
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods for warm-pool recovery: %w", err)
	}
	inventory := &teamQuotaWarmPoolInventory{
		templates:   make(map[teamquota.Owner]*v1alpha1.SandboxTemplate),
		replicaSets: append([]appsv1.ReplicaSet(nil), replicaSets.Items...),
		pods:        append([]corev1.Pod(nil), pods.Items...),
	}
	for _, template := range templates {
		owner, ok := controller.TeamWarmPoolQuotaOwner(template)
		if ok && owner.ClusterID == c.config.ClusterID {
			inventory.templates[owner] = template.DeepCopy()
		}
	}
	return inventory, nil
}

func (i *teamQuotaWarmPoolInventory) hasOwner(owner teamquota.Owner) bool {
	if i == nil {
		return true
	}
	if _, ok := i.templates[owner]; ok {
		return true
	}
	logicalID := warmPoolLogicalID(owner)
	if logicalID == "" {
		// An invalid or legacy owner cannot be safely released automatically.
		return true
	}
	for index := range i.replicaSets {
		if warmPoolMetadataMayBelongToOwner(
			i.replicaSets[index].Spec.Template.Labels,
			i.replicaSets[index].Spec.Template.Annotations,
			logicalID,
			owner.TeamID,
		) {
			return true
		}
	}
	for index := range i.pods {
		pod := &i.pods[index]
		if pod.Labels[controller.LabelPoolType] != controller.PoolTypeIdle {
			continue
		}
		if warmPoolMetadataMayBelongToOwner(
			pod.Labels,
			pod.Annotations,
			logicalID,
			owner.TeamID,
		) {
			return true
		}
	}
	return false
}

func (i *teamQuotaWarmPoolInventory) observedTarget(
	ctx context.Context,
	client kubernetes.Interface,
	owner teamquota.Owner,
) (teamquota.Values, bool, error) {
	if i == nil {
		return nil, false, nil
	}
	template := i.templates[owner]
	if template == nil {
		return nil, false, nil
	}
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	replicaSetName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return nil, false, fmt.Errorf("resolve warm-pool ReplicaSet name: %w", err)
	}
	var replicaSet *appsv1.ReplicaSet
	for index := range i.replicaSets {
		candidate := &i.replicaSets[index]
		if candidate.Namespace == template.Namespace && candidate.Name == replicaSetName {
			replicaSet = candidate
			break
		}
	}
	if replicaSet == nil {
		return nil, false, nil
	}
	target, err := observedTeamWarmPoolTarget(
		ctx,
		client,
		template,
		replicaSet,
		i.pods,
		owner,
	)
	if err != nil {
		return nil, false, err
	}
	return target, true, nil
}

func observedTeamWarmPoolTarget(
	ctx context.Context,
	client kubernetes.Interface,
	template *v1alpha1.SandboxTemplate,
	replicaSet *appsv1.ReplicaSet,
	pods []corev1.Pod,
	owner teamquota.Owner,
) (teamquota.Values, error) {
	if template == nil || replicaSet == nil {
		return nil, fmt.Errorf("warm-pool template and ReplicaSet are required")
	}
	desiredReplicas := int32(0)
	if replicaSet.Spec.Replicas != nil {
		desiredReplicas = *replicaSet.Spec.Replicas
	}
	if desiredReplicas < 0 {
		return nil, fmt.Errorf("observed warm-pool replicas must be non-negative")
	}
	target := zeroWarmPoolTeamQuotaTarget()
	quotaSpec, err := runtimeclassquota.ResolvePodSpec(
		ctx,
		client,
		&replicaSet.Spec.Template.Spec,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve warm-pool RuntimeClass quota overhead: %w", err)
	}
	perPod := PodSpecTeamQuotaResources(quotaSpec)
	target[teamquota.KeySandboxRuntimeCount] = int64(desiredReplicas)
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		value, err := multiplyQuotaValue(perPod[key], int64(desiredReplicas))
		if err != nil {
			return nil, err
		}
		target[key] = value
	}

	active := zeroWarmPoolTeamQuotaTarget()
	terminating := zeroWarmPoolTeamQuotaTarget()
	logicalID := warmPoolLogicalID(owner)
	for index := range pods {
		pod := &pods[index]
		if pod.Namespace != template.Namespace ||
			pod.Labels[controller.LabelPoolType] != controller.PoolTypeIdle ||
			!warmPoolMetadataMayBelongToOwner(
				pod.Labels,
				pod.Annotations,
				logicalID,
				owner.TeamID,
			) {
			continue
		}
		destination := active
		if pod.DeletionTimestamp != nil {
			destination = terminating
		}
		if err := addObservedWarmPoolPod(destination, &pod.Spec); err != nil {
			return nil, err
		}
	}
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxRuntimeCount,
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		if active[key] > target[key] {
			target[key] = active[key]
		}
		if terminating[key] > math.MaxInt64-target[key] {
			return nil, fmt.Errorf("observed warm-pool %s quota overflows int64", key)
		}
		target[key] += terminating[key]
	}
	return target, nil
}

func addObservedWarmPoolPod(target teamquota.Values, spec *corev1.PodSpec) error {
	if target[teamquota.KeySandboxRuntimeCount] == math.MaxInt64 {
		return fmt.Errorf("observed warm-pool runtime count overflows int64")
	}
	target[teamquota.KeySandboxRuntimeCount]++
	resources := PodSpecTeamQuotaResources(spec)
	for _, key := range []teamquota.Key{
		teamquota.KeySandboxCPUMillicores,
		teamquota.KeySandboxMemoryBytes,
		teamquota.KeySandboxEphemeralStorageBytes,
	} {
		if resources[key] > math.MaxInt64-target[key] {
			return fmt.Errorf("observed warm-pool %s quota overflows int64", key)
		}
		target[key] += resources[key]
	}
	return nil
}

func warmPoolLogicalID(owner teamquota.Owner) string {
	clusterID := strings.TrimSpace(owner.ClusterID)
	ownerID := strings.TrimSpace(owner.ID)
	prefix := clusterID + "/"
	if clusterID == "" || !strings.HasPrefix(ownerID, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(ownerID, prefix))
}

func warmPoolMetadataMayBelongToOwner(
	objectLabels map[string]string,
	annotations map[string]string,
	logicalID string,
	teamID string,
) bool {
	if strings.TrimSpace(objectLabels[controller.LabelTemplateLogicalID]) != logicalID {
		return false
	}
	annotatedTeamID := strings.TrimSpace(annotations[controller.AnnotationTeamID])
	if annotatedTeamID != "" && annotatedTeamID != teamID {
		return false
	}
	ownerKind := strings.TrimSpace(annotations[controller.AnnotationOwnerKind])
	if ownerKind == "" {
		ownerKind = strings.TrimSpace(objectLabels[controller.LabelOwnerKind])
	}
	return ownerKind == "" || ownerKind == controller.OwnerKindTeamWarmPool
}

func zeroWarmPoolTeamQuotaTarget() teamquota.Values {
	return teamquota.Values{
		teamquota.KeySandboxRuntimeCount:          0,
		teamquota.KeySandboxCPUMillicores:         0,
		teamquota.KeySandboxMemoryBytes:           0,
		teamquota.KeySandboxEphemeralStorageBytes: 0,
	}
}

func (c *TeamQuotaRecoveryController) recoverPreparedTransfers(
	ctx context.Context,
) error {
	for {
		transfers, err := c.recoveryStore.ListRecoveryTransfers(
			ctx,
			c.config.ClusterID,
			c.config.StaleAfter,
			c.config.BatchSize,
		)
		if err != nil {
			return fmt.Errorf("list prepared team quota transfers: %w", err)
		}
		if len(transfers) == 0 {
			return nil
		}
		processed := 0
		for i := range transfers {
			transfer := &transfers[i]
			terminalized, err := c.recoverPreparedTransfer(ctx, transfer)
			if err != nil {
				return fmt.Errorf(
					"recover prepared transfer %s for sandbox %s: %w",
					transfer.Operation.ID,
					transfer.Destination.ID,
					err,
				)
			}
			if terminalized {
				processed++
			}
		}
		if processed == 0 || len(transfers) < c.config.BatchSize {
			return nil
		}
	}
}

func (c *TeamQuotaRecoveryController) recoverPreparedTransfer(
	ctx context.Context,
	transfer *teamquota.RecoveryTransfer,
) (bool, error) {
	if transfer == nil {
		return false, nil
	}
	reservation := &teamquota.Reservation{
		Owner:     transfer.Destination,
		Operation: transfer.Operation,
		Target:    transfer.DestinationTarget.Clone(),
	}
	pod, err := c.service.k8sClient.CoreV1().
		Pods(transfer.Runtime.Namespace).
		Get(ctx, transfer.Runtime.Name, metav1.GetOptions{})
	if err == nil {
		if string(pod.UID) != transfer.Runtime.UID {
			// The exact transfer runtime is gone even if Kubernetes reused its
			// namespace/name for a different Pod.
			pod = nil
		} else if pod.DeletionTimestamp != nil {
			c.logger.Warn(
				"Retaining prepared warm-pool transfer until its terminating runtime is gone",
				zap.String("team_id", transfer.Destination.TeamID),
				zap.String("sandbox_id", transfer.Destination.ID),
				zap.String("operation_id", transfer.Operation.ID),
			)
			return false, nil
		} else if !recoveryPodOwnsTransfer(pod, transfer) {
			commitment, commitmentErr := c.preparedTransferWarmPoolCommitment(ctx, transfer)
			if commitmentErr != nil {
				return false, commitmentErr
			}
			released, markerErr := c.service.warmPoolReplicaCommitmentReleased(ctx, commitment)
			if markerErr != nil {
				return false, fmt.Errorf("inspect recovered warm-pool transfer marker: %w", markerErr)
			}
			if recoveryPodIsOriginalIdleTransferSource(pod, transfer) && !released {
				if err := c.service.teamQuotaStore.AbortTransfer(
					ctx,
					teamquota.Ref(transfer.Destination, transfer.Operation),
					"recovery found the original idle source before external mutation",
				); err != nil {
					return false, fmt.Errorf("abort unapplied prepared transfer: %w", err)
				}
				return true, nil
			}
			c.logger.Warn(
				"Retaining prepared warm-pool transfer with ambiguous runtime ownership",
				zap.String("team_id", transfer.Destination.TeamID),
				zap.String("sandbox_id", transfer.Destination.ID),
				zap.String("operation_id", transfer.Operation.ID),
			)
			return false, nil
		} else {
			if !quotaValuesEqual(activeSandboxQuotaTarget(pod), transfer.DestinationTarget) {
				c.logger.Warn(
					"Retaining prepared warm-pool transfer until runtime resources match its quota target",
					zap.String("team_id", transfer.Destination.TeamID),
					zap.String("sandbox_id", transfer.Destination.ID),
					zap.String("operation_id", transfer.Operation.ID),
				)
				return false, nil
			}
			commitment, err := c.preparedTransferWarmPoolCommitment(ctx, transfer)
			if err != nil {
				return false, err
			}
			if commitment == nil {
				return false, fmt.Errorf("warm-pool ReplicaSet is unavailable for active transfer")
			}
			if err := c.service.releaseWarmPoolReplicaCommitment(ctx, commitment); err != nil {
				return false, err
			}
			if err := c.service.commitTeamWarmPoolTransfer(
				ctx,
				reservation,
				commitment.observedSource,
			); err != nil {
				return false, err
			}
			if err := c.service.clearWarmPoolReplicaCommitment(ctx, commitment); err != nil {
				c.logger.Error(
					"Failed to clear recovered warm-pool transfer marker",
					zap.String("team_id", transfer.Destination.TeamID),
					zap.String("sandbox_id", transfer.Destination.ID),
					zap.String("operation_id", transfer.Operation.ID),
					zap.Error(err),
				)
			}
			return true, nil
		}
	}
	if err != nil && !k8serrors.IsNotFound(err) {
		return false, fmt.Errorf("lookup transfer runtime: %w", err)
	}

	commitment, err := c.preparedTransferWarmPoolCommitment(ctx, transfer)
	if err != nil {
		return false, err
	}
	released, err := c.service.warmPoolReplicaCommitmentReleased(ctx, commitment)
	if err != nil {
		return false, fmt.Errorf("inspect recovered warm-pool transfer marker: %w", err)
	}
	ref := teamquota.Ref(transfer.Destination, transfer.Operation)
	if released {
		observed, err := c.service.waitForWarmPoolReplicaCommitment(ctx, commitment)
		if err != nil {
			return false, err
		}
		commitment.observedSource = observed
		if err := c.service.commitTeamWarmPoolTransfer(
			ctx,
			reservation,
			commitment.observedSource,
		); err != nil {
			return false, err
		}
		if err := c.service.clearWarmPoolReplicaCommitment(ctx, commitment); err != nil {
			c.logger.Error(
				"Failed to clear committed warm-pool transfer marker",
				zap.String("team_id", transfer.Destination.TeamID),
				zap.String("sandbox_id", transfer.Destination.ID),
				zap.String("operation_id", transfer.Operation.ID),
				zap.Error(err),
			)
		}
		c.service.releaseCommittedSandboxTeamQuotaRuntimeAfterFailure(
			reservation,
			transfer.Runtime,
			nil,
			"recovery found applied transfer runtime gone",
		)
		return true, nil
	}
	if err := c.service.teamQuotaStore.AbortTransfer(
		ctx,
		ref,
		"recovery found no destination runtime or external commitment",
	); err != nil {
		return false, fmt.Errorf("abort prepared transfer: %w", err)
	}
	return true, nil
}

func recoveryPodIsOriginalIdleTransferSource(
	pod *corev1.Pod,
	transfer *teamquota.RecoveryTransfer,
) bool {
	if pod == nil || transfer == nil ||
		string(pod.UID) != transfer.Runtime.UID ||
		pod.DeletionTimestamp != nil ||
		pod.Labels[controller.LabelPoolType] != controller.PoolTypeIdle ||
		metav1.GetControllerOf(pod) == nil {
		return false
	}
	return warmPoolMetadataMayBelongToOwner(
		pod.Labels,
		pod.Annotations,
		warmPoolLogicalID(transfer.Source),
		transfer.Source.TeamID,
	)
}

func recoveryPodOwnsTransfer(pod *corev1.Pod, transfer *teamquota.RecoveryTransfer) bool {
	if pod == nil || transfer == nil {
		return false
	}
	if pod.DeletionTimestamp != nil {
		return false
	}
	if transfer.Runtime.UID == "" || string(pod.UID) != transfer.Runtime.UID {
		return false
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive ||
		strings.TrimSpace(sandboxIDFromPod(pod)) != transfer.Destination.ID {
		return false
	}
	if strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) != transfer.Destination.TeamID {
		return false
	}
	if runtimeGenerationFromPod(pod) != transfer.Runtime.Generation {
		return false
	}
	return metav1.GetControllerOf(pod) == nil
}

func (c *TeamQuotaRecoveryController) preparedTransferWarmPoolCommitment(
	ctx context.Context,
	transfer *teamquota.RecoveryTransfer,
) (*warmPoolReplicaCommitment, error) {
	if transfer == nil {
		return nil, nil
	}
	template, err := c.templateForWarmPoolOwner(transfer.Source)
	if err != nil {
		return nil, err
	}
	if template == nil {
		return nil, nil
	}
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	rsName, err := naming.ReplicasetName(clusterID, template.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve recovery warm-pool ReplicaSet: %w", err)
	}
	rs, err := c.service.k8sClient.AppsV1().
		ReplicaSets(template.Namespace).
		Get(ctx, rsName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get recovery warm-pool ReplicaSet: %w", err)
	}
	return &warmPoolReplicaCommitment{
		namespace:   rs.Namespace,
		name:        rs.Name,
		uid:         string(rs.UID),
		operationID: transfer.Operation.ID,
		template:    template,
		source:      transfer.Source,
	}, nil
}

func (c *TeamQuotaRecoveryController) templateForWarmPoolOwner(
	owner teamquota.Owner,
) (*v1alpha1.SandboxTemplate, error) {
	if c.service.templateLister == nil {
		return nil, fmt.Errorf("template lister is required for warm-pool recovery")
	}
	templates, err := c.service.templateLister.List()
	if err != nil {
		return nil, fmt.Errorf("list templates for warm-pool recovery: %w", err)
	}
	for _, template := range templates {
		candidate, ok := controller.TeamWarmPoolQuotaOwner(template)
		if ok && candidate == owner {
			return template, nil
		}
	}
	return nil, nil
}

type teamQuotaSandboxInventory struct {
	pods    map[string]*corev1.Pod
	records map[string]*SandboxRecord
}

type teamQuotaSandboxRecordStore interface {
	ListTeamQuotaSandboxRecords(ctx context.Context, clusterID string) ([]*SandboxRecord, error)
}

func (c *TeamQuotaRecoveryController) loadSandboxInventory(
	ctx context.Context,
) (*teamQuotaSandboxInventory, error) {
	if c.service.k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is required for team quota recovery")
	}
	pods, err := c.service.k8sClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			controller.LabelPoolType: controller.PoolTypeActive,
		}).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list active sandbox pods for team quota recovery: %w", err)
	}
	recordStore, ok := c.service.sandboxStore.(teamQuotaSandboxRecordStore)
	if !ok || recordStore == nil {
		return nil, fmt.Errorf("durable sandbox inventory is required for team quota recovery")
	}
	records, err := recordStore.ListTeamQuotaSandboxRecords(ctx, c.config.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("list durable sandboxes for team quota recovery: %w", err)
	}
	inventory := &teamQuotaSandboxInventory{
		pods:    make(map[string]*corev1.Pod, len(pods.Items)),
		records: make(map[string]*SandboxRecord, len(records)),
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		sandboxID := strings.TrimSpace(sandboxIDFromPod(pod))
		teamID := strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID])
		if sandboxID == "" || teamID == "" {
			return nil, fmt.Errorf(
				"active sandbox pod %s/%s is missing quota ownership",
				pod.Namespace,
				pod.Name,
			)
		}
		if existing := inventory.pods[sandboxID]; existing != nil &&
			string(existing.UID) != string(pod.UID) {
			return nil, fmt.Errorf("multiple active runtimes found for sandbox %s", sandboxID)
		}
		inventory.pods[sandboxID] = pod.DeepCopy()
	}
	for _, record := range records {
		if record == nil {
			continue
		}
		sandboxID := strings.TrimSpace(record.ID)
		teamID := strings.TrimSpace(record.TeamID)
		if sandboxID == "" || teamID == "" {
			return nil, fmt.Errorf("durable sandbox %q is missing quota ownership", sandboxID)
		}
		if pod := inventory.pods[sandboxID]; pod != nil &&
			strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) != teamID {
			return nil, fmt.Errorf("sandbox %s has conflicting pod and record teams", sandboxID)
		}
		inventory.records[sandboxID] = cloneSandboxRecordForLifecycle(record)
	}
	return inventory, nil
}

func (c *TeamQuotaRecoveryController) listSandboxAllocations(
	ctx context.Context,
) ([]teamquota.RecoveryAllocation, error) {
	allocations, err := c.listRecoveryAllocations(ctx, "sandbox")
	if err != nil {
		return nil, fmt.Errorf("list recovery sandbox allocations: %w", err)
	}
	return allocations, nil
}

func (c *TeamQuotaRecoveryController) listRecoveryAllocations(
	ctx context.Context,
	ownerKind string,
) ([]teamquota.RecoveryAllocation, error) {
	var allocations []teamquota.RecoveryAllocation
	after := ""
	for {
		page, err := c.recoveryStore.ListRecoveryAllocations(ctx, teamquota.RecoveryAllocationFilter{
			ClusterID:         c.config.ClusterID,
			OwnerKind:         ownerKind,
			AfterAllocationID: after,
			Limit:             c.config.BatchSize,
		})
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return allocations, nil
		}
		allocations = append(allocations, page...)
		after = page[len(page)-1].AllocationID
		if len(page) < c.config.BatchSize {
			return allocations, nil
		}
	}
}

func recoveryAllocationDue(allocation *teamquota.RecoveryAllocation) bool {
	return allocation != nil &&
		allocation.Operation != nil &&
		allocation.ReconcileDue
}

func (c *TeamQuotaRecoveryController) recoverSandboxAllocation(
	ctx context.Context,
	inventory *teamQuotaSandboxInventory,
	allocation *teamquota.RecoveryAllocation,
) error {
	if allocation == nil || allocation.Operation == nil {
		return nil
	}
	pod := inventory.pods[allocation.Owner.ID]
	record := inventory.records[allocation.Owner.ID]
	if err := validateRecoveryOwner(allocation.Owner, pod, record); err != nil {
		return err
	}
	ref := teamquota.Ref(allocation.Owner, *allocation.Operation)
	switch allocation.State {
	case "reserved":
		if pod != nil {
			observed := activeSandboxQuotaTarget(pod)
			if quotaValuesEqual(allocation.Committed, observed) {
				if err := c.service.teamQuotaStore.Abort(
					ctx,
					ref,
					"recovery found the reserved mutation was not applied",
				); err != nil {
					return fmt.Errorf("abort recovered reservation: %w", err)
				}
				return nil
			}
			if err := c.service.teamQuotaStore.AttachRuntime(
				ctx,
				ref,
				sandboxTeamQuotaRuntimeRef(pod),
			); err != nil {
				return fmt.Errorf("attach recovered runtime: %w", err)
			}
			if quotaValuesEqual(allocation.Pending, observed) {
				if err := c.service.teamQuotaStore.Commit(ctx, ref); err != nil {
					return fmt.Errorf("commit recovered reservation: %w", err)
				}
				return nil
			}
			store, ok := c.service.teamQuotaStore.(teamquota.ObservedExactCapacityStore)
			if !ok || store == nil {
				return &teamquota.UnavailableError{
					Operation: "commit observed recovered sandbox capacity",
					Err:       fmt.Errorf("observed exact capacity store is not configured"),
				}
			}
			if err := store.CommitObservedExact(ctx, ref, observed); err != nil {
				return fmt.Errorf("commit observed recovered reservation: %w", err)
			}
			return nil
		}
		if err := c.service.teamQuotaStore.Abort(
			ctx,
			ref,
			"recovery found no runtime matching the reserved target",
		); err != nil {
			return fmt.Errorf("abort recovered reservation: %w", err)
		}
		return nil
	case "releasing":
		stillExists, err := c.recoveryRuntimeStillExists(ctx, allocation.Runtime)
		if err != nil {
			return err
		}
		if stillExists {
			return nil
		}
		if pod != nil {
			if err := c.service.teamQuotaStore.Abort(
				ctx,
				ref,
				"recovery found a replacement active runtime",
			); err != nil {
				return fmt.Errorf("abort stale runtime release: %w", err)
			}
			return nil
		}
		if record != nil && allocation.Pending[teamquota.KeySandboxIdentityCount] == 0 {
			// The physical runtime is gone, but durable identity deletion has not
			// committed. Keep the conservative release pending.
			return nil
		}
		if err := c.service.teamQuotaStore.ConfirmRelease(
			ctx,
			ref,
			allocation.Runtime,
		); err != nil {
			return fmt.Errorf("confirm recovered release: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported interrupted allocation state %q", allocation.State)
	}
}

func validateRecoveryOwner(
	owner teamquota.Owner,
	pod *corev1.Pod,
	record *SandboxRecord,
) error {
	if pod != nil &&
		strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]) != owner.TeamID {
		return fmt.Errorf("active pod team does not match allocation team")
	}
	if record != nil && strings.TrimSpace(record.TeamID) != owner.TeamID {
		return fmt.Errorf("durable sandbox team does not match allocation team")
	}
	return nil
}

func (c *TeamQuotaRecoveryController) recoveryRuntimeStillExists(
	ctx context.Context,
	runtime teamquota.RuntimeRef,
) (bool, error) {
	if strings.TrimSpace(runtime.Namespace) == "" || strings.TrimSpace(runtime.Name) == "" {
		return false, nil
	}
	pod, err := c.service.k8sClient.CoreV1().
		Pods(runtime.Namespace).
		Get(ctx, runtime.Name, metav1.GetOptions{})
	switch {
	case k8serrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("lookup releasing runtime: %w", err)
	case runtime.UID != "" && runtime.UID != string(pod.UID):
		return false, nil
	default:
		return true, nil
	}
}

func (c *TeamQuotaRecoveryController) reconcileSandboxInventory(
	ctx context.Context,
	inventory *teamQuotaSandboxInventory,
	allocations []teamquota.RecoveryAllocation,
) error {
	bySandboxID := make(map[string]*teamquota.RecoveryAllocation, len(allocations))
	for i := range allocations {
		allocation := &allocations[i]
		bySandboxID[allocation.Owner.ID] = allocation
		if allocation.Operation != nil {
			continue
		}
		if inventory.pods[allocation.Owner.ID] != nil ||
			inventory.records[allocation.Owner.ID] != nil {
			continue
		}
		if err := c.reconcileTargetIfRevision(
			ctx,
			allocation.Owner,
			zeroSandboxTeamQuotaTarget(),
			teamquota.RuntimeRef{},
			allocation.Revision,
		); err != nil {
			return fmt.Errorf("release orphan sandbox allocation %s: %w", allocation.Owner.ID, err)
		}
	}
	for sandboxID, pod := range inventory.pods {
		allocation := bySandboxID[sandboxID]
		if allocation != nil && allocation.Operation != nil {
			continue
		}
		owner := teamquota.Owner{
			TeamID:    strings.TrimSpace(pod.Annotations[controller.AnnotationTeamID]),
			Kind:      "sandbox",
			ID:        sandboxID,
			ClusterID: c.config.ClusterID,
		}
		if allocation != nil && allocation.Owner.TeamID != owner.TeamID {
			return fmt.Errorf("active sandbox %s conflicts with quota allocation team", sandboxID)
		}
		expectedRevision := int64(0)
		if allocation != nil {
			expectedRevision = allocation.Revision
		}
		if err := c.reconcileTargetIfRevision(
			ctx,
			owner,
			activeSandboxQuotaTarget(pod),
			sandboxTeamQuotaRuntimeRef(pod),
			expectedRevision,
		); err != nil {
			return fmt.Errorf("reconcile active sandbox %s: %w", sandboxID, err)
		}
	}
	for sandboxID, record := range inventory.records {
		if inventory.pods[sandboxID] != nil {
			continue
		}
		allocation := bySandboxID[sandboxID]
		if allocation != nil && allocation.Operation != nil {
			continue
		}
		owner := teamquota.Owner{
			TeamID:    strings.TrimSpace(record.TeamID),
			Kind:      "sandbox",
			ID:        sandboxID,
			ClusterID: c.config.ClusterID,
		}
		if allocation != nil && allocation.Owner.TeamID != owner.TeamID {
			return fmt.Errorf("durable sandbox %s conflicts with quota allocation team", sandboxID)
		}
		expectedRevision := int64(0)
		if allocation != nil {
			expectedRevision = allocation.Revision
		}
		if err := c.reconcileTargetIfRevision(
			ctx,
			owner,
			pausedSandboxTeamQuotaTarget(),
			teamquota.RuntimeRef{},
			expectedRevision,
		); err != nil {
			return fmt.Errorf("reconcile durable sandbox %s: %w", sandboxID, err)
		}
	}
	return nil
}

func (c *TeamQuotaRecoveryController) reconcileTargetIfRevision(
	ctx context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	runtimeRef teamquota.RuntimeRef,
	expectedRevision int64,
) error {
	store, ok := c.service.teamQuotaStore.(teamquota.RevisionReconcileStore)
	if !ok {
		return &teamquota.UnavailableError{
			Operation: "reconcile observed team quota target",
			Err:       fmt.Errorf("revision-fenced capacity store is not configured"),
		}
	}
	applied, err := store.ReconcileTargetIfRevision(
		ctx,
		owner,
		target,
		runtimeRef,
		expectedRevision,
	)
	if err != nil {
		return err
	}
	if !applied {
		c.logger.Debug(
			"Skipped stale team quota inventory observation",
			zap.String("team_id", owner.TeamID),
			zap.String("owner_kind", owner.Kind),
			zap.String("owner_id", owner.ID),
			zap.Int64("expected_revision", expectedRevision),
		)
	}
	return nil
}

func activeSandboxQuotaTarget(pod *corev1.Pod) teamquota.Values {
	target := PodSpecTeamQuotaResources(nil)
	if pod != nil {
		target = PodSpecTeamQuotaResources(&pod.Spec)
	}
	target[teamquota.KeySandboxIdentityCount] = 1
	target[teamquota.KeySandboxRuntimeCount] = 1
	return target
}

func quotaValuesEqual(left, right teamquota.Values) bool {
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	for key, value := range right {
		if left[key] != value {
			return false
		}
	}
	return true
}
