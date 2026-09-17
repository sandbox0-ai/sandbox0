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
// values are per-node admission budgets after host reservations, not ECS
// marketing values. Overcommitted pools must use the same admission budgets as
// their ctld enrollment profile; physical resource enforcement stays on ctld.
type Config struct {
	PoolID            string
	ClusterID         string
	OwnerID           string
	FixedNodes        int
	MinElasticNodes   int
	MaxElasticNodes   int
	NodeCPUMillicores int64
	NodeMemoryBytes   int64
	WarmSlotsPerNode  int
	// ElasticSlotsPerNode is the provisioned carrier capacity of a new elastic
	// worker, not its minimum readiness threshold or a cap on the fixed worker.
	// Zero preserves the legacy WarmSlotsPerNode capacity for existing configs.
	ElasticSlotsPerNode   int
	HeadroomCPUMillicores int64
	HeadroomMemoryBytes   int64
	HeadroomSlots         int
	Interval              time.Duration
	ControllerLeaseTTL    time.Duration
	ScaleInStabilization  time.Duration
	ScaleOutCooldown      time.Duration
	ScaleInCooldown       time.Duration
	ScaleOutWarmup        time.Duration
	MaxScaleOutStep       int
	MaxScaleInStep        int
	MaxPendingNodes       int
	Now                   func() time.Time
}

// Decision is one auditable reconcile result.
type Decision struct {
	CurrentElastic  int
	TargetElastic   int
	AppliedElastic  int
	RequiredNodes   int
	LowPressureAt   time.Time
	Action          string
	CapacityLimited bool
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
	if config.MinElasticNodes < 0 || config.MaxElasticNodes < config.MinElasticNodes || config.MaxElasticNodes > 299 {
		return nil, errors.New("node pool autoscaler bounds must satisfy 0 <= min <= max <= 299")
	}
	if config.NodeCPUMillicores <= 0 || config.NodeMemoryBytes <= 0 || config.WarmSlotsPerNode <= 0 {
		return nil, errors.New("positive per-node CPU, memory, and warm-slot capacity are required")
	}
	if config.ElasticSlotsPerNode == 0 {
		config.ElasticSlotsPerNode = config.WarmSlotsPerNode
	}
	if config.ElasticSlotsPerNode < config.WarmSlotsPerNode || config.ElasticSlotsPerNode > 576 {
		return nil, errors.New("elastic carrier capacity must cover readiness and not exceed 576")
	}
	if config.MaxScaleOutStep == 0 {
		config.MaxScaleOutStep = 10
	}
	if config.MaxScaleInStep == 0 {
		config.MaxScaleInStep = 1
	}
	if config.MaxPendingNodes == 0 {
		config.MaxPendingNodes = 10
	}
	if config.MaxScaleOutStep < 1 || config.MaxScaleOutStep > 299 || config.MaxScaleInStep < 1 || config.MaxScaleInStep > 299 {
		return nil, errors.New("node pool scaling steps must be between 1 and 299")
	}
	if config.MaxPendingNodes < 1 || config.MaxPendingNodes > 299 {
		return nil, errors.New("pending node budget must be between 1 and 299")
	}
	if config.ScaleInCooldown == 0 {
		config.ScaleInCooldown = time.Minute
	}
	if config.ScaleOutWarmup == 0 {
		config.ScaleOutWarmup = 5 * time.Minute
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
		config.ScaleOutCooldown < 0 || config.ScaleInCooldown < config.Interval ||
		config.ScaleOutWarmup < config.Interval || config.ScaleOutWarmup > time.Hour {
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
	if current < 0 || current > 299 {
		return Decision{}, errors.New("cloud desired capacity is outside the supported pool boundary")
	}
	target, required := w.target(snapshot)
	decision := Decision{
		CurrentElastic: current, TargetElastic: target, AppliedElastic: current,
		RequiredNodes: required, LowPressureAt: snapshot.State.LowPressureSince,
		Action: "stable",
	}
	liveFixed := 0
	if snapshot.ClusterFixedUsableSlots > 0 {
		liveFixed = 1
	}
	decision.CapacityLimited = required > liveFixed+w.config.MaxElasticNodes
	now := w.config.Now().UTC()
	readyElastic := 0
	for _, node := range snapshot.Nodes {
		if node.PoolKind == sandboxstore.RuntimeNodePoolKindElastic &&
			node.State == sandboxstore.RuntimeNodeInstanceActive && node.ProviderReady && node.CapacityLive {
			readyElastic++
		}
	}

	if target > current {
		// Renewed pressure invalidates the quiet window even when purchase rate
		// limits prevent a provider update on this pass.
		if !snapshot.State.LowPressureSince.IsZero() {
			if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, current, time.Time{}, ""); err != nil {
				return decision, err
			}
			decision.LowPressureAt = time.Time{}
		}
		// Durable leases may retain a target above a lowered ceiling or after
		// provider loss. That protects cleanup custody, not replacement purchases.
		if current >= w.config.MaxElasticNodes {
			decision.Action = "scale_out_capacity_limit"
			return decision, nil
		}
		if !snapshot.State.LastScaleOutAt.IsZero() && w.config.ScaleOutCooldown > 0 &&
			now.Sub(snapshot.State.LastScaleOutAt) < w.config.ScaleOutCooldown {
			decision.Action = "scale_out_cooldown"
			return decision, nil
		}
		// Include instances which have not registered yet by subtracting observed
		// ready workers from cloud desired capacity, rather than counting only DB rows.
		applied := min(target, w.config.MaxElasticNodes, current+w.config.MaxScaleOutStep, readyElastic+w.config.MaxPendingNodes)
		if applied <= current {
			decision.Action = "scale_out_pending_budget"
			return decision, nil
		}
		if err := w.cloud.SetDesiredCapacity(ctx, applied); err != nil {
			return decision, fmt.Errorf("increase elastic desired capacity: %w", err)
		}
		if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, applied, time.Time{}, "out"); err != nil {
			return decision, err
		}
		decision.Action, decision.AppliedElastic, decision.LowPressureAt = "scale_out", applied, time.Time{}
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
	// Failed-claim pressure is a TTL signal, not a durable queue. Never cancel
	// in-flight capacity just because that signal expires before boot completes.
	if !snapshot.State.LastScaleOutAt.IsZero() && now.Sub(snapshot.State.LastScaleOutAt) < w.config.ScaleOutWarmup {
		decision.Action = "scale_in_waiting_for_warmup"
		return decision, nil
	}
	for _, node := range snapshot.Nodes {
		if node.PoolKind == sandboxstore.RuntimeNodePoolKindElastic &&
			node.State == sandboxstore.RuntimeNodeInstanceDraining && node.CapacityLive {
			decision.Action = "scale_in_waiting_for_drain"
			return decision, nil
		}
		// Old enrollment records remain until their independent cleanup finishes.
		// They cannot represent pending desired capacity once healthy, admitted
		// workers cover the provider's entire current desired count. This only
		// permits normal fenced scale-in; it never revokes historical identity.
		if readyElastic < current && node.PoolKind == sandboxstore.RuntimeNodePoolKindElastic &&
			(node.State == sandboxstore.RuntimeNodeInstanceEnrolling ||
				(node.State == sandboxstore.RuntimeNodeInstanceActive && !node.ProviderReady)) {
			decision.Action = "scale_in_waiting_for_enrollment"
			return decision, nil
		}
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
	if !snapshot.State.LastScaleInAt.IsZero() && now.Sub(snapshot.State.LastScaleInAt) < w.config.ScaleInCooldown {
		decision.Action = "scale_in_cooldown"
		return decision, nil
	}
	applied := max(target, current-w.config.MaxScaleInStep)
	if err := w.cloud.SetDesiredCapacity(ctx, applied); err != nil {
		return decision, fmt.Errorf("decrease elastic desired capacity: %w", err)
	}
	// A bounded shrink keeps the already-established quiet window; subsequent
	// steps use a separate cooldown instead of paying another full idle hour.
	nextLow := time.Time{}
	if applied > target {
		nextLow = lowSince
	}
	if _, err := w.store.UpdateRuntimeNodePoolScaleState(ctx, w.config.PoolID, applied, nextLow, "in"); err != nil {
		return decision, err
	}
	decision.Action, decision.AppliedElastic, decision.LowPressureAt = "scale_in", applied, nextLow
	return decision, nil
}

func (w *Worker) target(snapshot *sandboxstore.RuntimeNodePoolSnapshot) (int, int) {
	// Retiring leases still fence node removal and consume physical admission,
	// but replacing their carriers is not new workload demand. Counting cleanup
	// backlog here can scale out indefinitely while the old nodes cannot drain.
	requiredCPU := snapshot.ClusterWorkloadCPU + snapshot.DemandCPUMillicores + w.config.HeadroomCPUMillicores
	requiredMemory := snapshot.ClusterWorkloadMemory + snapshot.DemandMemoryBytes + w.config.HeadroomMemoryBytes
	requiredSlots := snapshot.ClusterWorkloadSlots + snapshot.DemandSlots + w.config.HeadroomSlots
	// With the supported single fixed worker, any usable carrier proves a live
	// fixed node. A retiring carrier must not erase that node's CPU/memory credit,
	// but only its actual usable carriers can cover the slot requirement.
	fixedUsableSlots := max(snapshot.ClusterFixedUsableSlots, 0)
	liveFixedNodes := 0
	if fixedUsableSlots > 0 {
		liveFixedNodes = 1
	}
	fixedCPU, fixedMemory := snapshot.ClusterFixedCPU, snapshot.ClusterFixedMemory
	if liveFixedNodes == 0 {
		fixedCPU, fixedMemory = 0, 0
	} else {
		// Legacy snapshots without a physical-capacity projection retain their
		// homogeneous baseline. The PG store reports the actual admission budget.
		if fixedCPU <= 0 {
			fixedCPU = w.config.NodeCPUMillicores
		}
		if fixedMemory <= 0 {
			fixedMemory = w.config.NodeMemoryBytes
		}
	}
	elastic := max(
		ceilDiv(requiredCPU-fixedCPU, w.config.NodeCPUMillicores),
		ceilDiv(requiredMemory-fixedMemory, w.config.NodeMemoryBytes),
		ceilDiv(int64(requiredSlots)-int64(fixedUsableSlots), int64(w.config.ElasticSlotsPerNode)),
		w.config.FixedNodes-liveFixedNodes,
	)
	// Live sandboxes cannot be consolidated by pretending their leases can move
	// to the fixed worker. Lifecycle hooks remain the final race-safe authority.
	busyElastic, readyElastic := 0, 0
	for _, node := range snapshot.Nodes {
		if node.PoolKind == sandboxstore.RuntimeNodePoolKindElastic && node.State != sandboxstore.RuntimeNodeInstanceRevoked && node.ActiveLeases > 0 {
			busyElastic++
		}
		if node.PoolKind == sandboxstore.RuntimeNodePoolKindElastic && node.State == sandboxstore.RuntimeNodeInstanceActive && node.ProviderReady && node.CapacityLive {
			readyElastic++
		}
	}
	elastic = max(elastic, busyElastic)
	for _, demand := range snapshot.DemandShapes {
		// Add a progress floor only for a request a fresh worker can actually
		// satisfy. In-flight nodes already cover readyElastic+1, preventing
		// repeated purchases for the same fragmentation during enrollment.
		if demand.CPUMillicores > w.config.NodeCPUMillicores || demand.MemoryBytes > w.config.NodeMemoryBytes || demand.Slots > w.config.ElasticSlotsPerNode {
			continue
		}
		fits := false
		for _, node := range snapshot.PlacementNodes {
			if node.PhysicalCPU >= demand.CPUMillicores && node.PhysicalMemory >= demand.MemoryBytes &&
				node.FreeCPU >= demand.CPUMillicores && node.FreeMemory >= demand.MemoryBytes && node.ReadySlots >= demand.Slots {
				fits = true
				break
			}
		}
		if !fits {
			elastic = max(elastic, readyElastic+1)
			break
		}
	}
	requiredNodes := liveFixedNodes + elastic
	elastic = min(max(elastic, w.config.MinElasticNodes), w.config.MaxElasticNodes)
	// A lowered operator ceiling limits new purchases, not existing workloads.
	// Busy instances are retained even when they are above that ceiling.
	elastic = max(elastic, busyElastic)
	return elastic, requiredNodes
}

func ceilDiv(value, unit int64) int {
	if value <= 0 {
		return 0
	}
	return int(1 + (value-1)/unit)
}
