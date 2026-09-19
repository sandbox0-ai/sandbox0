package carrierpool

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
)

type Store interface {
	HeartbeatRuntimeCarrierController(context.Context, string, time.Duration) error
	RecordRuntimeCarrierSurplus(context.Context, string, int) error
	ListRuntimeCarrierNodes(context.Context, string) ([]sandboxstore.RuntimeCarrierNode, error)
	BeginRuntimeCarrierResize(context.Context, sandboxstore.RuntimeCarrierNode, int, []string, []string) (int64, error)
	RuntimeCarrierBusyAllocations(context.Context, sandboxstore.RuntimeCarrierNode) ([]string, error)
	RuntimeCarrierReadyAllocations(context.Context, sandboxstore.RuntimeCarrierNode) ([]string, error)
	CompleteRuntimeCarrierResize(context.Context, sandboxstore.RuntimeCarrierNode, []string) error
}

// Nomad must compare-and-swap each changed shard and retain a global monotonic
// revision on that shard. The store serializes resize intents per cluster. Merely
// setting dynamic node metadata cannot reject an old, delayed shrink request.
type Nomad interface {
	CarrierCatalog(context.Context, string) ([]string, error)
	CarrierAllocations(context.Context, string) ([]nomadinventory.Allocation, error)
	ApplyCarrierPlan(context.Context, string, int64, []string) error
}

type Config struct {
	ClusterID        string
	StandardDigest   string
	PrivilegedDigest string
	Maximum          int
	LowWatermark     int
	Spare            int
	ShrinkAfter      time.Duration
	Interval         time.Duration
	PoolID           string
	PrewarmWindows   []PrewarmWindow
}

type Worker struct {
	store  Store
	nomad  Nomad
	config Config
	wake   chan struct{}
}

var errUnchanged = errors.New("carrier placement is unchanged")

func New(store Store, nomad Nomad, c Config) (*Worker, error) {
	if store == nil || nomad == nil || c.ClusterID == "" || c.Maximum < 8 || c.Maximum > 576 ||
		c.LowWatermark < 1 || c.Spare < c.LowWatermark || c.Spare > 128 || c.Spare >= c.Maximum ||
		c.ShrinkAfter < time.Minute || c.Interval < time.Second {
		return nil, fmt.Errorf("invalid adaptive carrier configuration")
	}
	if len(c.PrewarmWindows) > 32 {
		return nil, fmt.Errorf("too many prewarm windows")
	}
	names := map[string]bool{}
	for _, window := range c.PrewarmWindows {
		if c.PoolID == "" || window.Name == "" || len(window.Name) > 80 || names[window.Name] || window.Start.IsZero() || !window.End.After(window.Start) || window.End.Sub(window.Start) > 24*time.Hour || window.Slots < 1 || window.Slots > 1024 || window.CPUMillicores < 1 || window.MemoryBytes < 64<<20 || (window.SecurityClass != "standard" && window.SecurityClass != "privileged") {
			return nil, fmt.Errorf("invalid carrier prewarm window")
		}
		names[window.Name] = true
		if _, ok := store.(demandStore); !ok {
			return nil, fmt.Errorf("prewarm requires durable demand store")
		}
	}
	return &Worker{store: store, nomad: nomad, config: c, wake: make(chan struct{}, 1)}, nil
}

// RequestReconcile coalesces request-path pressure; it never runs Nomad work on
// an HTTP handler and is safe when adaptive carriers are disabled.
func (w *Worker) RequestReconcile() {
	if w == nil {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *Worker) Run(ctx context.Context, report func(int, error)) {
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()
	for {
		changed, err := w.Reconcile(ctx)
		report(changed, err)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-w.wake:
		}
	}
}

// Reconcile changes at most one node per pass. Interrupted intents take priority
// and remain claim-fenced through manager restarts and uncertain API responses.
func (w *Worker) Reconcile(ctx context.Context) (changed int, resultErr error) {
	defer func() {
		if resultErr == nil {
			resultErr = w.store.HeartbeatRuntimeCarrierController(ctx, w.config.ClusterID, 30*time.Second)
		}
	}()
	if err := w.store.RecordRuntimeCarrierSurplus(ctx, w.config.ClusterID, 2*w.config.Spare); err != nil {
		return 0, err
	}
	nodes, err := w.store.ListRuntimeCarrierNodes(ctx, w.config.ClusterID)
	if err != nil {
		return 0, err
	}
	targets, err := w.demand(ctx, nodes)
	if err != nil {
		return 0, err
	}
	for _, pending := range []bool{true, false} {
		for _, n := range nodes {
			if n.Pending != pending {
				continue
			}
			if n.Pending && (n.StaleIdentity || (n.Retiring && len(n.Groups) > 8)) {
				// Revocation proves node lifecycle cleanup. Replace an interrupted
				// refill with retirement, or rebind to an admitted successor boot
				// only after the store proves all predecessor custody is gone.
				if err := w.prepare(ctx, &n, targets[n.NodeID]); err != nil {
					return 0, err
				}
			}
			if !n.Pending {
				ceiling := w.config.Maximum
				if n.MaxCarriers > 0 {
					ceiling = min(ceiling, n.MaxCarriers)
				}
				grow := len(n.Groups) < ceiling && n.Ready < w.config.LowWatermark && n.FreeCPU > 0 && n.FreeMemory >= 64<<20
				if w.config.StandardDigest != "" && n.Revision > 0 {
					counts := map[string]int{}
					for _, g := range n.Groups {
						class, _, _ := GroupIndex(g)
						counts[class]++
					}
					std := w.config.StandardDigest
					priv := w.config.PrivilegedDigest
					grow = n.FreeCPU > 0 && n.FreeMemory >= 64<<20 &&
						((n.ReadyByCompatibility[std] < w.config.LowWatermark && counts["standard"] < n.CompatibilityCapacity[std]) ||
							(n.ReadyByCompatibility[priv] < 2 && counts["privileged"] < n.CompatibilityCapacity[priv])) &&
						(len(n.Groups) < ceiling || n.Ready > 0)
				}
				for class, count := range targets[n.NodeID] {
					digest := w.config.StandardDigest
					if class == "privileged" {
						digest = w.config.PrivilegedDigest
					}
					if count > n.ReadyByCompatibility[digest] {
						grow = true
					}
				}
				shrink := n.Ready > 2*w.config.Spare && n.SurplusSince != nil && time.Since(*n.SurplusSince) >= w.config.ShrinkAfter
				if !grow && !shrink && n.Revision != 0 && !n.Retiring && !n.StaleIdentity {
					continue
				}
				if err := w.prepare(ctx, &n, targets[n.NodeID]); err != nil {
					if errors.Is(err, errUnchanged) {
						continue
					}
					return 0, err
				}
			}
			if err := w.reconcileNode(ctx, n); err != nil {
				return 0, err
			}
			return 1, nil
		}
	}
	return 0, nil
}

func (w *Worker) prepare(ctx context.Context, n *sandboxstore.RuntimeCarrierNode, demand map[string]int) error {
	previousGroups, previousCapacity, previousMaximum := n.Groups, n.CompatibilityCapacity, n.MaxCarriers
	catalogNode := n.NodeID
	if n.Retiring {
		catalogNode = ""
		n.FreeCPU, n.FreeMemory = 0, 0
	}
	catalog, err := w.nomad.CarrierCatalog(ctx, catalogNode)
	if err != nil {
		return err
	}
	busyIDs, err := w.store.RuntimeCarrierBusyAllocations(ctx, *n)
	if err != nil {
		return err
	}
	var allocations []nomadinventory.Allocation
	if n.Retiring {
		if len(busyIDs) > 0 {
			return errors.New("revoked node retains carrier cleanup custody")
		}
	} else {
		allocations, err = w.nomad.CarrierAllocations(ctx, n.NodeID)
		if err != nil {
			return err
		}
	}
	lookup := map[string]string{}
	for _, a := range allocations {
		lookup[a.ID] = a.TaskGroup
	}
	busy := make([]string, 0, len(busyIDs))
	for _, id := range busyIDs {
		g, ok := lookup[id]
		if !ok {
			return errors.New("busy allocation missing from exact Nomad inventory")
		}
		busy = append(busy, g)
	}
	n.MaxCarriers = min(w.config.Maximum, len(catalog))
	n.CompatibilityCapacity = map[string]int{}
	for _, g := range catalog {
		class, _, _ := GroupIndex(g)
		digest := w.config.StandardDigest
		if class == "privileged" {
			digest = w.config.PrivilegedDigest
		}
		if digest != "" {
			n.CompatibilityCapacity[digest]++
		}
	}
	for digest, count := range n.CompatibilityCapacity {
		n.CompatibilityCapacity[digest] = min(count, n.MaxCarriers)
	}
	n.Groups, err = planDemand(catalog, busy, w.config.Spare, n.MaxCarriers, n.FreeCPU, n.FreeMemory, demand)
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	if n.Revision > 0 && !n.StaleIdentity && slices.Equal(previousGroups, n.Groups) && maps.Equal(previousCapacity, n.CompatibilityCapacity) && previousMaximum == n.MaxCarriers {
		return errUnchanged
	}
	for _, g := range n.Groups {
		allowed[g] = true
	}
	retained := []string{}
	for _, a := range allocations {
		if allowed[a.TaskGroup] {
			retained = append(retained, a.ID)
		}
	}
	n.Revision, err = w.store.BeginRuntimeCarrierResize(ctx, *n, n.MaxCarriers, n.Groups, retained)
	n.Pending = err == nil
	return err
}

func (w *Worker) reconcileNode(ctx context.Context, n sandboxstore.RuntimeCarrierNode) error {
	if err := w.nomad.ApplyCarrierPlan(ctx, n.NodeID, n.Revision, n.Groups); err != nil {
		return err
	}
	if n.Retiring && len(n.Groups) == 8 {
		// The durable revocation fence already owns physical cleanup proof; the
		// Nomad node may have been purged, so do not require its inventory API.
		return w.store.CompleteRuntimeCarrierResize(ctx, n, nil)
	}
	// Do not reopen admission while a removed carrier is still executing. This
	// is not lease release; terminal recovery still owns physical cleanup proof.
	allocations, err := w.nomad.CarrierAllocations(ctx, n.NodeID)
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	for _, g := range n.Groups {
		allowed[g] = true
	}
	removed := []string{}
	observed := map[string]bool{}
	ready, err := w.store.RuntimeCarrierReadyAllocations(ctx, n)
	if err != nil {
		return err
	}
	usable := map[string]bool{}
	for _, id := range ready {
		usable[id] = true
	}
	for _, a := range allocations {
		if allowed[a.TaskGroup] && a.ClientStatus == "running" && usable[a.ID] {
			observed[a.TaskGroup] = true
		}
		if !allowed[a.TaskGroup] && a.ClientStatus != "complete" && a.ClientStatus != "failed" && a.ClientStatus != "lost" {
			return nil
		}
		if !allowed[a.TaskGroup] {
			removed = append(removed, a.ID)
		}
	}
	// A successful job registration is not ready capacity. Keep an incomplete
	// refill pending so the cloud scaler's bounded provisioning credit expires.
	// Revoked nodes only remove membership; they must not recreate carriers.
	if !n.Retiring && len(observed) != len(allowed) {
		return nil
	}
	return w.store.CompleteRuntimeCarrierResize(ctx, n, removed)
}
