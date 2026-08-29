// Package nodepoolautoscaler reconciles regional Sandbox0 capacity to one
// fixed worker plus a bounded provider-managed elastic pool.
package nodepoolautoscaler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

const (
	defaultInterval             = 10 * time.Second
	defaultLeaseTTL             = 30 * time.Second
	defaultScaleInStabilization = 10 * time.Minute
)

// Store is the PostgreSQL authority needed by the active-active controller.
type Store interface {
	EnsureRuntimeNodePoolState(context.Context, string, string) (*sandboxstore.RuntimeNodePoolState, error)
	AcquireRuntimeNodePoolControllerLease(context.Context, string, string, time.Duration) (bool, error)
	GetRuntimeNodePoolSnapshot(context.Context, string) (*sandboxstore.RuntimeNodePoolSnapshot, error)
	UpdateRuntimeNodePoolScaleState(context.Context, string, int, time.Time, string) (*sandboxstore.RuntimeNodePoolState, error)
}

// Cloud changes only ESS desired capacity. Scale-in deletion is separately
// fenced by provider lifecycle hooks and the node lifecycle reconciler.
type Cloud interface {
	DesiredCapacity(context.Context) (int, error)
	SetDesiredCapacity(context.Context, int) error
}

// Config defines a homogeneous elastic pool and its fixed baseline. Resource
// values are schedulable capacity after host reservations, not ECS marketing
// values.
type Config struct {
	PoolID                string
	ClusterID             string
	OwnerID               string
	FixedNodes            int
	MinElasticNodes       int
	MaxElasticNodes       int
	NodeCPUMillicores     int64
	NodeMemoryBytes       int64
	WarmSlotsPerNode      int
	HeadroomCPUMillicores int64
	HeadroomMemoryBytes   int64
	HeadroomSlots         int
	Interval              time.Duration
	ControllerLeaseTTL    time.Duration
	ScaleInStabilization  time.Duration
	ScaleOutCooldown      time.Duration
	Now                   func() time.Time
}

// Decision is one auditable reconcile result.
type Decision struct {
	CurrentElastic int
	TargetElastic  int
	AppliedElastic int
	RequiredNodes  int
	LowPressureAt  time.Time
	Action         string
}

// Worker runs an active-active-safe desired-capacity loop.
type Worker struct {
	store  Store
	cloud  Cloud
	config Config
}

// New validates capacity policy and constructs a Worker.
func New(store Store, cloud Cloud, config Config) (*Worker, error) {
	if store == nil || cloud == nil {
		return nil, errors.New("node pool autoscaler store and cloud are required")
	}
	config.PoolID = strings.TrimSpace(config.PoolID)
	config.ClusterID = strings.TrimSpace(config.ClusterID)
	config.OwnerID = strings.TrimSpace(config.OwnerID)
	if config.PoolID == "" || config.ClusterID == "" || config.OwnerID == "" {
		return nil, errors.New("node pool autoscaler identities are required")
	}
	if config.FixedNodes != 1 {
		return nil, errors.New("node pool autoscaler currently requires exactly one fixed worker")
	}
	if config.MinElasticNodes != 0 || config.MaxElasticNodes != 299 {
		return nil, errors.New("node pool autoscaler bounds must be exactly 0..299")
	}
	if config.NodeCPUMillicores <= 0 || config.NodeMemoryBytes <= 0 || config.WarmSlotsPerNode <= 0 {
		return nil, errors.New("positive per-node CPU, memory, and warm-slot capacity are required")
	}
	if config.HeadroomCPUMillicores < 0 || config.HeadroomMemoryBytes < 0 || config.HeadroomSlots < 0 {
		return nil, errors.New("node pool autoscaler headroom cannot be negative")
	}
	if config.Interval == 0 {
		config.Interval = defaultInterval
	}
	if config.ControllerLeaseTTL == 0 {
		config.ControllerLeaseTTL = defaultLeaseTTL
	}
	if config.ScaleInStabilization == 0 {
		config.ScaleInStabilization = defaultScaleInStabilization
	}
	if config.Interval < time.Second || config.ControllerLeaseTTL < config.Interval ||
		config.ControllerLeaseTTL > 5*time.Minute || config.ScaleInStabilization < time.Minute ||
		config.ScaleOutCooldown < 0 {
		return nil, errors.New("invalid node pool autoscaler timing policy")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Worker{store: store, cloud: cloud, config: config}, nil
}

// Run reconciles until cancellation. A failed pass is reported and retried;
// it does not stop manager or relinquish safety fencing.
func (w *Worker) Run(ctx context.Context, report func(Decision, error)) {
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()
	for {
		decision, err := w.Reconcile(ctx)
		if report != nil {
			report(decision, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Reconcile performs one lease-protected desired-capacity decision.
func (w *Worker) Reconcile(ctx context.Context) (Decision, error) {
	if _, err := w.store.EnsureRuntimeNodePoolState(ctx, w.config.PoolID, w.config.ClusterID); err != nil {
		return Decision{}, err
	}
	acquired, err := w.store.AcquireRuntimeNodePoolControllerLease(
		ctx, w.config.PoolID, w.config.OwnerID, w.config.ControllerLeaseTTL,
	)
	if err != nil || !acquired {
		if err != nil {
			return Decision{}, err
		}
		return Decision{Action: "not_leader"}, nil
	}
	snapshot, err := w.store.GetRuntimeNodePoolSnapshot(ctx, w.config.PoolID)
	if err != nil {
		return Decision{}, err
	}
	current, err := w.cloud.DesiredCapacity(ctx)
	if err != nil {
		return Decision{}, fmt.Errorf("read elastic desired capacity: %w", err)
	}
	target, required := w.target(snapshot)
	decision := Decision{
		CurrentElastic: current, TargetElastic: target, AppliedElastic: current,
		RequiredNodes: required, LowPressureAt: snapshot.State.LowPressureSince,
		Action: "stable",
	}
	now := w.config.Now().UTC()

	if target > current {
		if !snapshot.State.LastScaleOutAt.IsZero() && w.config.ScaleOutCooldown > 0 &&
			now.Sub(snapshot.State.LastScaleOutAt) < w.config.ScaleOutCooldown {
			decision.Action = "scale_out_cooldown"
			return decision, nil
		}
		if err := w.cloud.SetDesiredCapacity(ctx, target); err != nil {
			return decision, fmt.Errorf("increase elastic desired capacity: %w", err)
		}
		if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, target, time.Time{}, "out"); err != nil {
			return decision, err
		}
		decision.Action, decision.AppliedElastic, decision.LowPressureAt = "scale_out", target, time.Time{}
		return decision, nil
	}
	if target == current {
		if !snapshot.State.LowPressureSince.IsZero() || snapshot.State.DesiredNodes != current {
			if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, current, time.Time{}, ""); err != nil {
				return decision, err
			}
		}
		decision.LowPressureAt = time.Time{}
		return decision, nil
	}

	lowSince := snapshot.State.LowPressureSince
	if lowSince.IsZero() || snapshot.State.DesiredNodes != current {
		if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, current, now, ""); err != nil {
			return decision, err
		}
		decision.Action, decision.LowPressureAt = "scale_in_stabilizing", now
		return decision, nil
	}
	decision.LowPressureAt = lowSince
	if now.Sub(lowSince) < w.config.ScaleInStabilization {
		decision.Action = "scale_in_stabilizing"
		return decision, nil
	}
	if err := w.cloud.SetDesiredCapacity(ctx, target); err != nil {
		return decision, fmt.Errorf("decrease elastic desired capacity: %w", err)
	}
	if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, target, time.Time{}, "in"); err != nil {
		return decision, err
	}
	decision.Action, decision.AppliedElastic, decision.LowPressureAt = "scale_in", target, time.Time{}
	return decision, nil
}

func (w *Worker) target(snapshot *sandboxstore.RuntimeNodePoolSnapshot) (int, int) {
	requiredCPU := snapshot.ClusterUsedCPU + snapshot.DemandCPUMillicores + w.config.HeadroomCPUMillicores
	requiredMemory := snapshot.ClusterUsedMemory + snapshot.DemandMemoryBytes + w.config.HeadroomMemoryBytes
	requiredSlots := snapshot.ClusterActiveLeases + snapshot.DemandSlots + w.config.HeadroomSlots
	requiredNodes := max(
		ceilDiv(requiredCPU, w.config.NodeCPUMillicores),
		ceilDiv(requiredMemory, w.config.NodeMemoryBytes),
		ceilDiv(int64(requiredSlots), int64(w.config.WarmSlotsPerNode)),
		w.config.FixedNodes,
	)
	readyFixedNodes := min(
		w.config.FixedNodes,
		snapshot.ClusterFixedUsableSlots/w.config.WarmSlotsPerNode,
	)
	elastic := requiredNodes - readyFixedNodes
	elastic = min(max(elastic, w.config.MinElasticNodes), w.config.MaxElasticNodes)
	return elastic, requiredNodes
}

func ceilDiv(value, unit int64) int {
	if value <= 0 {
		return 0
	}
	return int(1 + (value-1)/unit)
}
