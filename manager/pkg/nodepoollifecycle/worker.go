package nodepoollifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

const (
	TransitionScaleOut = "scale_out"
	TransitionScaleIn  = "scale_in"

	LifecycleContinue = "CONTINUE"
	LifecycleRollback = "ROLLBACK"
	LifecycleAbandon  = "ABANDON"
)

type Action struct {
	Token       string
	HookID      string
	Transition  string
	InstanceIDs []string
}

// ErrAllocationRoutesPending keeps node identity and CIDR ownership fenced
// until asynchronous cloud route deletion is observed complete.
var ErrAllocationRoutesPending = errors.New("allocation route deletion is pending")

type Cloud interface {
	ListPendingLifecycleActions(context.Context) ([]Action, error)
	HeartbeatLifecycleAction(context.Context, Action, time.Duration) error
	CompleteLifecycleAction(context.Context, Action, string) error
	SetInstancesProtection(context.Context, []string, bool) error
	ElasticInstancesInService(context.Context, []string) (map[string]bool, error)
	ElasticInstancesAttached(context.Context, []string) (map[string]bool, error)
	DeleteAllocationRoutes(context.Context, string, string) error
}

type Nomad interface {
	FenceAndStopWarmAllocations(context.Context, string) error
	NodeHasNonterminalAllocations(context.Context, string) (bool, error)
	NodeHasNonWarmNonterminalAllocations(context.Context, string) (bool, error)
	PurgeNode(context.Context, string) error
}

type Store interface {
	GetRuntimeNodePoolSnapshot(context.Context, string) (*sandboxstore.RuntimeNodePoolSnapshot, error)
	GetRuntimeNodeDrainStatus(context.Context, string, string) (*sandboxstore.RuntimeNodeDrainStatus, error)
	BeginRuntimeNodeDrain(context.Context, string, string, string) error
	RevokeRuntimeNode(context.Context, string, string, string) error
	MarkRuntimeNodeProviderReady(context.Context, string, string, int) error
	CompleteReadyRuntimeNodeScaleOutActions(context.Context, string) error
	AbandonRuntimeNodeEnrollment(context.Context, string, string) error
	ObserveRuntimeNodeLifecycleAction(context.Context, *sandboxstore.ObserveRuntimeNodeLifecycleActionRequest) (*sandboxstore.RuntimeNodeLifecycleAction, error)
	ReserveRuntimeNodeLifecycleHeartbeat(context.Context, string, string, time.Duration) (bool, error)
	ListRuntimeNodeLifecycleActions(context.Context, string) ([]*sandboxstore.RuntimeNodeLifecycleAction, error)
	AcquireRuntimeNodeLifecycleAction(context.Context, string, string, string, time.Duration) (*sandboxstore.RuntimeNodeLifecycleAction, error)
	ObserveRuntimeNodeLifecycleProviderAction(context.Context, string, string, string, int64, bool) (*sandboxstore.RuntimeNodeLifecycleAction, error)
	ObserveRuntimeNodeLifecycleProviderInstances(context.Context, string, string, string, int64, bool) (*sandboxstore.RuntimeNodeLifecycleAction, error)
	TerminalizeRuntimeSlotsForProviderAbsentInstance(context.Context, string, string, string, string, int64, []byte) (int, error)
	BeginRuntimeNodeLifecycleActionCleanup(context.Context, string) error
	BeginRuntimeNodeLifecycleActionCleanupForOwner(context.Context, string, string, string, int64) error
	CompleteRuntimeNodeLifecycleAction(context.Context, string, string) error
	CompleteRuntimeNodeLifecycleActionWithProof(context.Context, string, string, any) error
	CompleteRuntimeNodeLifecycleActionRecovery(context.Context, string, string, string, int64, string, any) error
}

type Config struct {
	PoolID                    string
	ScaleOutHookID            string
	ScaleInHookID             string
	WarmSlotsPerNode          int
	Interval                  time.Duration
	HeartbeatTimeout          time.Duration
	ScaleOutEnrollmentTimeout time.Duration
	OwnerID                   string
	RecoveryLeaseTTL          time.Duration
	ProviderAbsenceGrace      time.Duration
	Now                       func() time.Time
}

type Result struct {
	Observed   int
	Completed  int
	RolledBack int
	Recovered  int
}

type Worker struct {
	store  Store
	cloud  Cloud
	nomad  Nomad
	config Config
}

func New(store Store, cloud Cloud, nomad Nomad, config Config) (*Worker, error) {
	config.PoolID = strings.TrimSpace(config.PoolID)
	config.ScaleOutHookID = strings.TrimSpace(config.ScaleOutHookID)
	config.ScaleInHookID = strings.TrimSpace(config.ScaleInHookID)
	if config.Interval == 0 {
		config.Interval = 10 * time.Second
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = 120 * time.Second
	}
	if config.ScaleOutEnrollmentTimeout == 0 {
		config.ScaleOutEnrollmentTimeout = 20 * time.Minute
	}
	if config.OwnerID == "" {
		config.OwnerID = "runtime-node-lifecycle"
	}
	if config.RecoveryLeaseTTL == 0 {
		config.RecoveryLeaseTTL = 5 * time.Minute
	}
	if config.ProviderAbsenceGrace == 0 {
		config.ProviderAbsenceGrace = max(2*config.Interval, 10*time.Second)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if store == nil || cloud == nil || nomad == nil || config.PoolID == "" ||
		config.ScaleOutHookID == "" || config.ScaleInHookID == "" ||
		config.ScaleOutHookID == config.ScaleInHookID || config.WarmSlotsPerNode <= 0 ||
		config.Interval < time.Second || config.Interval > time.Minute ||
		config.HeartbeatTimeout < 30*time.Second || config.HeartbeatTimeout > 10*time.Minute ||
		config.ScaleOutEnrollmentTimeout < 5*time.Minute ||
		config.ScaleOutEnrollmentTimeout > 50*time.Minute || len(config.OwnerID) > 256 ||
		config.RecoveryLeaseTTL < 2*config.Interval || config.RecoveryLeaseTTL > 5*time.Minute ||
		config.ProviderAbsenceGrace < config.Interval || config.ProviderAbsenceGrace > 10*time.Minute {
		return nil, errors.New("runtime node lifecycle controller config is invalid")
	}
	return &Worker{store: store, cloud: cloud, nomad: nomad, config: config}, nil
}

func (w *Worker) Run(ctx context.Context, report func(Result, error)) {
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()
	for {
		result, err := w.Reconcile(ctx)
		if report != nil {
			report(result, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Reconcile renews pending hooks before independent readiness and protection
// work. A failed provider operation must not starve another action's deadline.
func (w *Worker) Reconcile(ctx context.Context) (Result, error) {
	var result Result
	var failures []error
	type pendingAction struct {
		action   Action
		observed *sandboxstore.RuntimeNodeLifecycleAction
	}
	var pending []pendingAction
	actions, err := w.cloud.ListPendingLifecycleActions(ctx)
	if err != nil {
		failures = append(failures, err)
	}
	providerListSucceeded := err == nil
	presentTokens := make(map[string]bool, len(actions))
	for _, action := range actions {
		transition, ok := w.transitionForHook(action.HookID)
		if !ok {
			continue
		}
		action.Transition = transition
		result.Observed++
		presentTokens[action.Token] = true
		observed, err := w.store.ObserveRuntimeNodeLifecycleAction(ctx,
			&sandboxstore.ObserveRuntimeNodeLifecycleActionRequest{
				Token: action.Token, PoolID: w.config.PoolID, LifecycleHookID: action.HookID,
				ProviderInstanceIDs: action.InstanceIDs, Transition: transition,
				RecoveryDeadline: w.config.Now().UTC().Add(w.config.ScaleOutEnrollmentTimeout),
			})
		if err != nil {
			failures = append(failures, err)
			continue
		}
		// Claiming during observation prevents a newly nonterminal action from
		// existing without its bounded recovery owner and lease.
		claimed, err := w.store.AcquireRuntimeNodeLifecycleAction(
			ctx, w.config.PoolID, action.Token, w.config.OwnerID, w.config.RecoveryLeaseTTL,
		)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if claimed != nil {
			observed = claimed
		}
		interval, timeout := w.heartbeatSchedule()
		reserved, err := w.store.ReserveRuntimeNodeLifecycleHeartbeat(ctx, w.config.PoolID, action.Token, interval)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if reserved {
			if err := w.cloud.HeartbeatLifecycleAction(ctx, action, timeout); err != nil {
				failures = append(failures, err)
				continue
			}
		}
		pending = append(pending, pendingAction{action: action, observed: observed})
	}
	if err := w.reconcileProviderReadiness(ctx); err != nil {
		failures = append(failures, err)
	}
	if err := w.reconcileProtection(ctx); err != nil {
		failures = append(failures, err)
	}
	for _, item := range pending {
		var completed, rolledBack bool
		var err error
		switch item.action.Transition {
		case TransitionScaleOut:
			completed, err = w.reconcileScaleOut(ctx, item.action, item.observed)
		case TransitionScaleIn:
			completed, rolledBack, err = w.reconcileScaleIn(ctx, item.action)
		}
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if completed {
			result.Completed++
		}
		if rolledBack {
			result.RolledBack++
		}
	}
	recovered, err := w.recoverDurableActions(ctx, providerListSucceeded, presentTokens)
	if err != nil {
		failures = append(failures, err)
	}
	result.Recovered += recovered
	return result, errors.Join(failures...)
}

// recoverDurableActions treats PostgreSQL as the recovery source. Provider's
// pending-action list is evidence, but a successful empty observation must not
// strand a durable pending or draining action after its deadline.
func (w *Worker) recoverDurableActions(
	ctx context.Context,
	providerListSucceeded bool,
	presentTokens map[string]bool,
) (int, error) {
	durableActions, err := w.store.ListRuntimeNodeLifecycleActions(ctx, w.config.PoolID)
	if err != nil {
		return 0, err
	}
	var failures []error
	recovered := 0
	now := w.config.Now().UTC()
	for _, durable := range durableActions {
		action := Action{
			Token: durable.Token, HookID: durable.LifecycleHookID,
			Transition: durable.Transition, InstanceIDs: durable.ProviderInstanceIDs,
		}
		claimed, err := w.store.AcquireRuntimeNodeLifecycleAction(
			ctx, w.config.PoolID, durable.Token, w.config.OwnerID, w.config.RecoveryLeaseTTL,
		)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if claimed == nil {
			continue
		}
		if providerListSucceeded {
			claimed, err = w.store.ObserveRuntimeNodeLifecycleProviderAction(
				ctx, w.config.PoolID, claimed.Token, w.config.OwnerID,
				claimed.RecoveryEpoch, presentTokens[claimed.Token],
			)
			if err != nil {
				failures = append(failures, err)
				continue
			}
		} else if claimed.ProviderActionAbsentSince == nil {
			continue
		}

		allInstancesAbsent := false
		if claimed.ProviderActionAbsentSince != nil {
			attached, err := w.cloud.ElasticInstancesAttached(ctx, claimed.ProviderInstanceIDs)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			allInstancesAbsent = len(claimed.ProviderInstanceIDs) > 0
			for _, instanceID := range claimed.ProviderInstanceIDs {
				if attached[instanceID] {
					allInstancesAbsent = false
					break
				}
			}
			claimed, err = w.store.ObserveRuntimeNodeLifecycleProviderInstances(
				ctx, w.config.PoolID, claimed.Token, w.config.OwnerID,
				claimed.RecoveryEpoch, allInstancesAbsent,
			)
			if err != nil {
				failures = append(failures, err)
				continue
			}
		}

		actionDeadline := claimed.RecoveryDeadlineAt
		if actionDeadline.IsZero() || now.Before(actionDeadline) ||
			claimed.ProviderActionAbsentSince == nil || claimed.ProviderInstanceAbsentSince == nil ||
			now.Sub(*claimed.ProviderActionAbsentSince) < w.config.ProviderAbsenceGrace ||
			now.Sub(*claimed.ProviderInstanceAbsentSince) < w.config.ProviderAbsenceGrace ||
			!allInstancesAbsent {
			continue
		}

		switch claimed.Transition {
		case TransitionScaleOut:
			if allDurableChildrenProviderReady(ctx, w.store, w.config.PoolID, claimed.ProviderInstanceIDs) {
				if err := w.store.CompleteRuntimeNodeLifecycleActionRecovery(
					ctx, w.config.PoolID, claimed.Token, w.config.OwnerID, claimed.RecoveryEpoch,
					"completed", convergenceProof(action, claimed, "provider_completed_all_children_ready"),
				); err != nil {
					failures = append(failures, err)
					continue
				}
				recovered++
				continue
			}
			completed, err := w.abandonScaleOut(ctx, action, claimed)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if completed {
				recovered++
			}
		case TransitionScaleIn:
			completed, err := w.reconcileAbsentScaleIn(ctx, action, claimed)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			if completed {
				recovered++
			}
		}
	}
	return recovered, errors.Join(failures...)
}

func allDurableChildrenProviderReady(
	ctx context.Context,
	store Store,
	poolID string,
	instanceIDs []string,
) bool {
	if len(instanceIDs) == 0 {
		return false
	}
	for _, instanceID := range instanceIDs {
		status, err := store.GetRuntimeNodeDrainStatus(ctx, poolID, instanceID)
		if err != nil || status == nil ||
			status.Instance.State != sandboxstore.RuntimeNodeInstanceActive || !status.Instance.ProviderReady {
			return false
		}
	}
	return true
}

func (w *Worker) reconcileAbsentScaleIn(
	ctx context.Context,
	action Action,
	recovery *sandboxstore.RuntimeNodeLifecycleAction,
) (bool, error) {
	statuses := make(map[string]*sandboxstore.RuntimeNodeDrainStatus, len(action.InstanceIDs))
	for _, instanceID := range action.InstanceIDs {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if errors.Is(err, sandboxstore.ErrRuntimeNodeNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		statuses[instanceID] = status
		if status.Instance.State == sandboxstore.RuntimeNodeInstanceEnrolling ||
			status.Instance.ActiveLeases > 0 {
			return false, fmt.Errorf("absent scale-in child %s still owns enrollment or lease custody", instanceID)
		}
		if status.Instance.State == sandboxstore.RuntimeNodeInstanceActive {
			if err := w.store.BeginRuntimeNodeDrain(ctx, w.config.PoolID, instanceID,
				"durable scale-in action recovered after provider absence "+action.Token); err != nil {
				return false, err
			}
		}
		if _, err := w.store.TerminalizeRuntimeSlotsForProviderAbsentInstance(
			ctx, w.config.PoolID, instanceID, action.Token,
			recovery.RecoveryOwnerID, recovery.RecoveryEpoch,
			providerAbsenceProofDigest(recovery, instanceID),
		); err != nil {
			return false, err
		}
	}
	for instanceID := range statuses {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if err != nil {
			return false, err
		}
		if !status.SafeToStop() {
			return false, nil
		}
		hasForeign, err := w.nomad.NodeHasNonWarmNonterminalAllocations(ctx, status.Instance.NodeID)
		if err != nil {
			return false, err
		}
		if hasForeign {
			return false, fmt.Errorf("absent provider node %s retains a non-warm Nomad allocation", instanceID)
		}
	}
	for instanceID, status := range statuses {
		if err := w.cloud.DeleteAllocationRoutes(ctx, instanceID, status.Instance.AllocationCIDR); err != nil {
			if errors.Is(err, ErrAllocationRoutesPending) {
				return false, nil
			}
			return false, err
		}
		if err := w.nomad.PurgeNode(ctx, status.Instance.NodeID); err != nil {
			return false, err
		}
		if err := w.store.RevokeRuntimeNode(ctx, w.config.PoolID, instanceID,
			"elastic runtime node recovered after provider absence"); err != nil {
			return false, err
		}
	}
	return true, w.store.CompleteRuntimeNodeLifecycleActionRecovery(
		ctx, w.config.PoolID, action.Token, recovery.RecoveryOwnerID, recovery.RecoveryEpoch,
		"completed", convergenceProof(action, recovery, "provider_action_and_instances_absent"),
	)
}

// heartbeatSchedule leaves four of Aliyun's twenty extensions for cleanup and
// retries. Provider renewal is independent of frequent readiness polling; the
// requested timeout covers two renewal intervals, including a polling margin.
func (w *Worker) heartbeatSchedule() (interval, timeout time.Duration) {
	const plannedAttempts = sandboxstore.RuntimeNodeLifecycleHeartbeatMaxAttempts - 4
	interval = max(w.config.HeartbeatTimeout/2, w.config.ScaleOutEnrollmentTimeout/plannedAttempts)
	interval = (interval + time.Second - 1) / time.Second * time.Second
	timeout = max(w.config.HeartbeatTimeout, 2*interval+2*w.config.Interval)
	return interval, timeout
}

func (w *Worker) reconcileProviderReadiness(ctx context.Context) error {
	snapshot, err := w.store.GetRuntimeNodePoolSnapshot(ctx, w.config.PoolID)
	if err != nil {
		return err
	}
	var candidates []string
	for _, node := range snapshot.Nodes {
		if node.PoolKind == sandboxstore.RuntimeNodePoolKindElastic &&
			node.State == sandboxstore.RuntimeNodeInstanceActive && !node.ProviderReady &&
			node.CapacityLive && node.ReadySlots >= w.config.WarmSlotsPerNode {
			candidates = append(candidates, node.ProviderInstanceID)
		}
	}
	if len(candidates) == 0 {
		return w.store.CompleteReadyRuntimeNodeScaleOutActions(ctx, w.config.PoolID)
	}
	inService, err := w.cloud.ElasticInstancesInService(ctx, candidates)
	if err != nil {
		return err
	}
	for _, instanceID := range candidates {
		if !inService[instanceID] {
			continue
		}
		if err := w.store.MarkRuntimeNodeProviderReady(ctx, w.config.PoolID,
			instanceID, w.config.WarmSlotsPerNode); err != nil {
			return err
		}
	}
	return w.store.CompleteReadyRuntimeNodeScaleOutActions(ctx, w.config.PoolID)
}

func (w *Worker) reconcileProtection(ctx context.Context) error {
	snapshot, err := w.store.GetRuntimeNodePoolSnapshot(ctx, w.config.PoolID)
	if err != nil {
		return err
	}
	var busy, idle []string
	for _, node := range snapshot.Nodes {
		if node.PoolKind != sandboxstore.RuntimeNodePoolKindElastic ||
			node.State == sandboxstore.RuntimeNodeInstanceRevoked {
			continue
		}
		if node.State == sandboxstore.RuntimeNodeInstanceEnrolling ||
			!node.ProviderReady || node.ActiveLeases > 0 {
			busy = append(busy, node.ProviderInstanceID)
		} else if node.State == sandboxstore.RuntimeNodeInstanceActive {
			idle = append(idle, node.ProviderInstanceID)
		}
	}
	if len(busy) > 0 {
		if err := w.cloud.SetInstancesProtection(ctx, busy, true); err != nil {
			return fmt.Errorf("protect busy elastic nodes: %w", err)
		}
	}
	if len(idle) > 0 {
		if err := w.cloud.SetInstancesProtection(ctx, idle, false); err != nil {
			return fmt.Errorf("unprotect idle elastic nodes: %w", err)
		}
	}
	return nil
}

func (w *Worker) reconcileScaleOut(
	ctx context.Context,
	action Action,
	observed *sandboxstore.RuntimeNodeLifecycleAction,
) (bool, error) {
	if observed == nil {
		return false, errors.New("durable scale-out lifecycle action is required")
	}
	cleanupStarted := observed.State == "draining" || observed.State == "abandoned"
	timedOut := cleanupStarted
	if !timedOut {
		firstObservedAt := observed.FirstObservedAt
		if firstObservedAt.IsZero() {
			firstObservedAt = w.config.Now()
		}
		timedOut = !w.config.Now().Before(firstObservedAt.Add(w.config.ScaleOutEnrollmentTimeout))
	}
	allReady := true
	for _, instanceID := range action.InstanceIDs {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if errors.Is(err, sandboxstore.ErrRuntimeNodeNotFound) {
			allReady = false
			continue
		}
		if err != nil {
			return false, err
		}
		node := status.Instance
		if node.State != sandboxstore.RuntimeNodeInstanceActive || !node.CapacityLive ||
			node.ReadySlots < w.config.WarmSlotsPerNode {
			allReady = false
		}
	}
	if cleanupStarted {
		return w.abandonScaleOut(ctx, action, nil)
	}
	if !allReady {
		if !timedOut {
			return false, nil
		}
		return w.abandonScaleOut(ctx, action, nil)
	}
	if err := w.cloud.CompleteLifecycleAction(ctx, action, LifecycleContinue); err != nil {
		return false, err
	}
	for _, instanceID := range action.InstanceIDs {
		if err := w.store.MarkRuntimeNodeProviderReady(ctx, w.config.PoolID,
			instanceID, w.config.WarmSlotsPerNode); err != nil {
			return false, err
		}
	}
	if err := w.store.CompleteRuntimeNodeLifecycleActionWithProof(ctx, action.Token, "completed",
		convergenceProof(action, nil, "provider_continue_accepted")); err != nil {
		return false, err
	}
	return true, nil
}

// abandonScaleOut tears down every child before completing an action-wide
// ABANDON. Active children remain warming-fenced until they have no leases,
// slots, or Nomad allocations; enrollment-only children have no runtime
// identity and can release their route and subnet reservation directly.
func (w *Worker) abandonScaleOut(
	ctx context.Context,
	action Action,
	recovery *sandboxstore.RuntimeNodeLifecycleAction,
) (bool, error) {
	providerAbsent := recovery != nil && recovery.ProviderActionAbsentSince != nil &&
		recovery.ProviderInstanceAbsentSince != nil
	if providerAbsent {
		if err := w.store.BeginRuntimeNodeLifecycleActionCleanupForOwner(
			ctx, w.config.PoolID, action.Token, recovery.RecoveryOwnerID, recovery.RecoveryEpoch,
		); err != nil {
			return false, err
		}
	} else if err := w.store.BeginRuntimeNodeLifecycleActionCleanup(ctx, action.Token); err != nil {
		return false, err
	}
	statuses := make(map[string]*sandboxstore.RuntimeNodeDrainStatus, len(action.InstanceIDs))
	for _, instanceID := range action.InstanceIDs {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if errors.Is(err, sandboxstore.ErrRuntimeNodeNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		statuses[instanceID] = status
		switch status.Instance.State {
		case sandboxstore.RuntimeNodeInstanceEnrolling:
			if err := w.cloud.DeleteAllocationRoutes(ctx, instanceID, status.Instance.AllocationCIDR); err != nil {
				if errors.Is(err, ErrAllocationRoutesPending) {
					return false, nil
				}
				return false, err
			}
			if err := w.store.AbandonRuntimeNodeEnrollment(ctx, w.config.PoolID, instanceID); err != nil {
				return false, err
			}
			delete(statuses, instanceID)
		case sandboxstore.RuntimeNodeInstanceActive, sandboxstore.RuntimeNodeInstanceDraining:
			if status.Instance.ProviderReady {
				return false, fmt.Errorf("refuse to abandon provider-ready runtime node %s", instanceID)
			}
			if status.Instance.ActiveLeases > 0 {
				if err := w.cloud.SetInstancesProtection(ctx, []string{instanceID}, true); err != nil {
					return false, err
				}
				return false, fmt.Errorf("warming runtime node %s unexpectedly owns active leases", instanceID)
			}
			if status.Instance.State == sandboxstore.RuntimeNodeInstanceActive {
				if err := w.store.BeginRuntimeNodeDrain(ctx, w.config.PoolID, instanceID,
					"scale-out enrollment timed out for lifecycle action "+action.Token); err != nil {
					return false, err
				}
			}
			if providerAbsent {
				if _, err := w.store.TerminalizeRuntimeSlotsForProviderAbsentInstance(
					ctx, w.config.PoolID, instanceID, action.Token,
					recovery.RecoveryOwnerID, recovery.RecoveryEpoch,
					providerAbsenceProofDigest(recovery, instanceID),
				); err != nil {
					return false, err
				}
			} else if err := w.nomad.FenceAndStopWarmAllocations(ctx, status.Instance.NodeID); err != nil {
				return false, err
			}
		case sandboxstore.RuntimeNodeInstanceRevoked:
			delete(statuses, instanceID)
		default:
			return false, fmt.Errorf("runtime node %s has invalid scale-out cleanup state %q",
				instanceID, status.Instance.State)
		}
	}

	for instanceID := range statuses {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if errors.Is(err, sandboxstore.ErrRuntimeNodeNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !status.SafeToStop() {
			return false, nil
		}
		if providerAbsent {
			hasForeign, err := w.nomad.NodeHasNonWarmNonterminalAllocations(ctx, status.Instance.NodeID)
			if err != nil {
				return false, err
			}
			if hasForeign {
				return false, fmt.Errorf("absent provider node %s retains a non-warm Nomad allocation", instanceID)
			}
		} else {
			hasAllocations, err := w.nomad.NodeHasNonterminalAllocations(ctx, status.Instance.NodeID)
			if err != nil {
				return false, err
			}
			if hasAllocations {
				return false, nil
			}
		}
	}

	for instanceID := range statuses {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if errors.Is(err, sandboxstore.ErrRuntimeNodeNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if err := w.cloud.DeleteAllocationRoutes(ctx, instanceID, status.Instance.AllocationCIDR); err != nil {
			if errors.Is(err, ErrAllocationRoutesPending) {
				return false, nil
			}
			return false, err
		}
		if err := w.nomad.PurgeNode(ctx, status.Instance.NodeID); err != nil {
			return false, err
		}
		if err := w.store.RevokeRuntimeNode(ctx, w.config.PoolID, instanceID,
			"elastic runtime node enrollment timed out"); err != nil {
			return false, err
		}
	}
	if providerAbsent {
		if err := w.store.CompleteRuntimeNodeLifecycleActionRecovery(
			ctx, w.config.PoolID, action.Token, recovery.RecoveryOwnerID, recovery.RecoveryEpoch,
			"abandoned", convergenceProof(action, recovery, "provider_action_and_instances_absent"),
		); err != nil {
			return false, err
		}
	} else {
		if err := w.store.CompleteRuntimeNodeLifecycleActionWithProof(ctx, action.Token, "abandoned",
			convergenceProof(action, nil, "provider_abandon_accepted")); err != nil {
			return false, err
		}
		if err := w.cloud.CompleteLifecycleAction(ctx, action, LifecycleAbandon); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (w *Worker) reconcileScaleIn(ctx context.Context, action Action) (bool, bool, error) {
	statuses := make(map[string]*sandboxstore.RuntimeNodeDrainStatus, len(action.InstanceIDs))
	for _, instanceID := range action.InstanceIDs {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if errors.Is(err, sandboxstore.ErrRuntimeNodeNotFound) {
			continue
		}
		if err != nil {
			return false, false, err
		}
		statuses[instanceID] = status
		if status.Instance.State == sandboxstore.RuntimeNodeInstanceEnrolling ||
			status.Instance.ActiveLeases > 0 {
			if err := w.cloud.SetInstancesProtection(ctx, []string{instanceID}, true); err != nil {
				return false, false, err
			}
			if err := w.cloud.CompleteLifecycleAction(ctx, action, LifecycleRollback); err != nil {
				return false, false, err
			}
			if err := w.store.CompleteRuntimeNodeLifecycleActionWithProof(ctx, action.Token, "abandoned",
				convergenceProof(action, nil, "provider_rollback_accepted")); err != nil {
				return false, false, err
			}
			return true, true, nil
		}
	}

	for instanceID, status := range statuses {
		if status.Instance.State == sandboxstore.RuntimeNodeInstanceActive {
			if err := w.store.BeginRuntimeNodeDrain(ctx, w.config.PoolID, instanceID,
				"aliyun ESS scale-in lifecycle action "+action.Token); err != nil {
				return false, false, err
			}
		}
		if err := w.nomad.FenceAndStopWarmAllocations(ctx, status.Instance.NodeID); err != nil {
			return false, false, err
		}
	}

	for instanceID := range statuses {
		status, err := w.store.GetRuntimeNodeDrainStatus(ctx, w.config.PoolID, instanceID)
		if err != nil {
			return false, false, err
		}
		if !status.SafeToStop() {
			return false, false, nil
		}
		hasAllocations, err := w.nomad.NodeHasNonterminalAllocations(ctx, status.Instance.NodeID)
		if err != nil {
			return false, false, err
		}
		if hasAllocations {
			return false, false, nil
		}
	}

	for instanceID, status := range statuses {
		if err := w.cloud.DeleteAllocationRoutes(ctx, instanceID, status.Instance.AllocationCIDR); err != nil {
			if errors.Is(err, ErrAllocationRoutesPending) {
				return false, false, nil
			}
			return false, false, err
		}
		if err := w.nomad.PurgeNode(ctx, status.Instance.NodeID); err != nil {
			return false, false, err
		}
		if err := w.store.RevokeRuntimeNode(ctx, w.config.PoolID, instanceID,
			"elastic runtime node safely removed"); err != nil {
			return false, false, err
		}
	}
	if err := w.cloud.CompleteLifecycleAction(ctx, action, LifecycleContinue); err != nil {
		return false, false, err
	}
	if err := w.store.CompleteRuntimeNodeLifecycleActionWithProof(ctx, action.Token, "completed",
		convergenceProof(action, nil, "provider_continue_accepted")); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func (w *Worker) transitionForHook(hookID string) (string, bool) {
	switch strings.TrimSpace(hookID) {
	case w.config.ScaleOutHookID:
		return TransitionScaleOut, true
	case w.config.ScaleInHookID:
		return TransitionScaleIn, true
	default:
		return "", false
	}
}

func providerAbsenceProofDigest(
	action *sandboxstore.RuntimeNodeLifecycleAction,
	providerInstanceID string,
) []byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte(action.Token))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(providerInstanceID))
	_, _ = digest.Write([]byte{0})
	if action.ProviderActionAbsentSince != nil {
		_, _ = digest.Write([]byte(action.ProviderActionAbsentSince.UTC().Format(time.RFC3339Nano)))
	}
	_, _ = digest.Write([]byte{0})
	if action.ProviderInstanceAbsentSince != nil {
		_, _ = digest.Write([]byte(action.ProviderInstanceAbsentSince.UTC().Format(time.RFC3339Nano)))
	}
	return digest.Sum(nil)
}

func convergenceProof(
	action Action,
	recovery *sandboxstore.RuntimeNodeLifecycleAction,
	source string,
) map[string]any {
	proof := map[string]any{
		"source":     source,
		"token":      action.Token,
		"transition": action.Transition,
		"instances":  action.InstanceIDs,
	}
	if recovery != nil {
		if recovery.ProviderActionAbsentSince != nil {
			proof["provider_action_absent_since"] =
				recovery.ProviderActionAbsentSince.UTC().Format(time.RFC3339Nano)
		}
		if recovery.ProviderInstanceAbsentSince != nil {
			proof["provider_instance_absent_since"] =
				recovery.ProviderInstanceAbsentSince.UTC().Format(time.RFC3339Nano)
		}
	}
	return proof
}
