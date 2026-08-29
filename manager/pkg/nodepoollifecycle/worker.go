package nodepoollifecycle

import (
	"context"
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

type Cloud interface {
	ListPendingLifecycleActions(context.Context) ([]Action, error)
	HeartbeatLifecycleAction(context.Context, Action, time.Duration) error
	CompleteLifecycleAction(context.Context, Action, string) error
	SetInstancesProtection(context.Context, []string, bool) error
	ElasticInstancesInService(context.Context, []string) (map[string]bool, error)
	DeleteAllocationRoutes(context.Context, string, string) error
}

type Nomad interface {
	FenceAndStopWarmAllocations(context.Context, string) error
	NodeHasNonterminalAllocations(context.Context, string) (bool, error)
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
	BeginRuntimeNodeLifecycleActionCleanup(context.Context, string) error
	CompleteRuntimeNodeLifecycleAction(context.Context, string, string) error
}

type Config struct {
	PoolID                    string
	ScaleOutHookID            string
	ScaleInHookID             string
	WarmSlotsPerNode          int
	Interval                  time.Duration
	HeartbeatTimeout          time.Duration
	ScaleOutEnrollmentTimeout time.Duration
	Now                       func() time.Time
}

type Result struct {
	Observed   int
	Completed  int
	RolledBack int
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
	if config.Now == nil {
		config.Now = time.Now
	}
	if store == nil || cloud == nil || nomad == nil || config.PoolID == "" ||
		config.ScaleOutHookID == "" || config.ScaleInHookID == "" ||
		config.ScaleOutHookID == config.ScaleInHookID || config.WarmSlotsPerNode != 8 ||
		config.Interval < time.Second || config.Interval > time.Minute ||
		config.HeartbeatTimeout < 30*time.Second || config.HeartbeatTimeout > 10*time.Minute ||
		config.ScaleOutEnrollmentTimeout < 5*time.Minute ||
		config.ScaleOutEnrollmentTimeout > 50*time.Minute {
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

func (w *Worker) Reconcile(ctx context.Context) (Result, error) {
	var result Result
	if err := w.reconcileProviderReadiness(ctx); err != nil {
		return result, err
	}
	if err := w.reconcileProtection(ctx); err != nil {
		return result, err
	}
	actions, err := w.cloud.ListPendingLifecycleActions(ctx)
	if err != nil {
		return result, err
	}
	for _, action := range actions {
		transition, ok := w.transitionForHook(action.HookID)
		if !ok {
			continue
		}
		action.Transition = transition
		result.Observed++
		observed, err := w.store.ObserveRuntimeNodeLifecycleAction(ctx,
			&sandboxstore.ObserveRuntimeNodeLifecycleActionRequest{
				Token: action.Token, PoolID: w.config.PoolID, LifecycleHookID: action.HookID,
				ProviderInstanceIDs: action.InstanceIDs, Transition: transition,
			})
		if err != nil {
			return result, err
		}
		if err := w.cloud.HeartbeatLifecycleAction(ctx, action, w.config.HeartbeatTimeout); err != nil {
			return result, err
		}
		var completed, rolledBack bool
		switch transition {
		case TransitionScaleOut:
			completed, err = w.reconcileScaleOut(ctx, action, observed)
		case TransitionScaleIn:
			completed, rolledBack, err = w.reconcileScaleIn(ctx, action)
		}
		if err != nil {
			return result, err
		}
		if completed {
			result.Completed++
		}
		if rolledBack {
			result.RolledBack++
		}
	}
	return result, nil
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
		return w.abandonScaleOut(ctx, action)
	}
	if !allReady {
		if !timedOut {
			return false, nil
		}
		return w.abandonScaleOut(ctx, action)
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
	if err := w.store.CompleteRuntimeNodeLifecycleAction(ctx, action.Token, "completed"); err != nil {
		return false, err
	}
	return true, nil
}

// abandonScaleOut tears down every child before completing an action-wide
// ABANDON. Active children remain warming-fenced until they have no leases,
// slots, or Nomad allocations; enrollment-only children have no runtime
// identity and can release their route and subnet reservation directly.
func (w *Worker) abandonScaleOut(ctx context.Context, action Action) (bool, error) {
	if err := w.store.BeginRuntimeNodeLifecycleActionCleanup(ctx, action.Token); err != nil {
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
			if err := w.nomad.FenceAndStopWarmAllocations(ctx, status.Instance.NodeID); err != nil {
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
		hasAllocations, err := w.nomad.NodeHasNonterminalAllocations(ctx, status.Instance.NodeID)
		if err != nil {
			return false, err
		}
		if hasAllocations {
			return false, nil
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
	if err := w.store.CompleteRuntimeNodeLifecycleAction(ctx, action.Token, "abandoned"); err != nil {
		return false, err
	}
	if err := w.cloud.CompleteLifecycleAction(ctx, action, LifecycleAbandon); err != nil {
		return false, err
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
			if err := w.store.CompleteRuntimeNodeLifecycleAction(ctx, action.Token, "abandoned"); err != nil {
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
	if err := w.store.CompleteRuntimeNodeLifecycleAction(ctx, action.Token, "completed"); err != nil {
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
