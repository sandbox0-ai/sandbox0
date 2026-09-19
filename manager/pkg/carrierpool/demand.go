package carrierpool

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type demandStore interface {
	ListRuntimeCarrierDemand(context.Context, string) ([]sandboxstore.RuntimeNodePoolDemandShape, error)
	RecordRuntimeNodePoolDemand(context.Context, *sandboxstore.RuntimeNodePoolDemandRequest) error
}

// PrewarmWindow describes operator-planned spare capacity, not a reservation or
// an exception to team quotas. Times are absolute so missed schedules expire.
type PrewarmWindow struct {
	Name                       string
	Start, End                 time.Time
	Slots                      int
	CPUMillicores, MemoryBytes int64
	SecurityClass              string
}

func (w *Worker) demand(ctx context.Context, nodes []sandboxstore.RuntimeCarrierNode) (map[string]map[string]int, error) {
	store, ok := w.store.(demandStore)
	if !ok {
		return nil, nil
	}
	now := time.Now()
	for _, window := range w.config.PrewarmWindows {
		if now.Before(window.Start) || !now.Before(window.End) {
			continue
		}
		digest := w.config.StandardDigest
		if window.SecurityClass == "privileged" {
			digest = w.config.PrivilegedDigest
		}
		// Short renewable hints stop affecting scaling after a controller restart or
		// configuration removal. No running sandbox or lease is created here.
		err := store.RecordRuntimeNodePoolDemand(ctx, &sandboxstore.RuntimeNodePoolDemandRequest{
			PoolID: w.config.PoolID, ClusterID: w.config.ClusterID, OperationID: "carrier-prewarm/" + window.Name,
			CompatibilityDigest: digest, CPUMillicores: window.CPUMillicores, MemoryBytes: window.MemoryBytes,
			Slots: window.Slots, TTL: max(time.Second, min(30*time.Second, time.Until(window.End))),
		})
		if err != nil {
			return nil, fmt.Errorf("record planned carrier demand: %w", err)
		}
	}
	shapes, err := store.ListRuntimeCarrierDemand(ctx, w.config.ClusterID)
	if err != nil {
		return nil, err
	}
	return distributeDemand(nodes, shapes, w.config), nil
}

// distributeDemand credits existing inventory first, then assigns replenishable
// inventory. Virtual CPU/memory is consumed once across shapes and classes; a
// large indivisible request must also fit the physical machine. These hints
// never substitute for the store's real resource lease checks.
func distributeDemand(nodes []sandboxstore.RuntimeCarrierNode, shapes []sandboxstore.RuntimeNodePoolDemandShape, c Config) map[string]map[string]int {
	targets := map[string]map[string]int{}
	cpu, memory := make([]int64, len(nodes)), make([]int64, len(nodes))
	used := make([]int, len(nodes))
	assigned := make([]map[string]int, len(nodes))
	for i, n := range nodes {
		cpu[i], memory[i] = n.FreeCPU, n.FreeMemory
		assigned[i] = map[string]int{}
	}
	remaining := make([]int, len(shapes))
	for i, s := range shapes {
		remaining[i] = s.Slots
	}
	for _, readyOnly := range []bool{true, false} {
		for si, s := range shapes {
			class := ""
			switch s.CompatibilityDigest {
			case c.StandardDigest:
				class = "standard"
			case c.PrivilegedDigest:
				class = "privileged"
			}
			if class == "" || s.CompatibilityDigest == "" || s.CPUMillicores <= 0 || s.MemoryBytes <= 0 {
				continue
			}
			for i, n := range nodes {
				if n.Retiring || n.StaleIdentity || s.CPUMillicores > n.PhysicalCPU || s.MemoryBytes > n.PhysicalMemory {
					continue
				}
				ceiling := c.Maximum
				if n.MaxCarriers > 0 {
					ceiling = min(ceiling, n.MaxCarriers)
				}
				// Non-ready catalog members can include active or initializing carriers.
				// Treat all of them as busy until authoritative readiness says otherwise.
				busy := max(0, len(n.Groups)-n.Ready)
				classGroups := 0
				for _, group := range n.Groups {
					groupClass, _, _ := GroupIndex(group)
					if groupClass == class {
						classGroups++
					}
				}
				classBusy := max(0, classGroups-n.ReadyByCompatibility[s.CompatibilityDigest])
				available := min(ceiling-busy-used[i], n.CompatibilityCapacity[s.CompatibilityDigest]-classBusy-assigned[i][s.CompatibilityDigest])
				if readyOnly {
					available = min(available, n.ReadyByCompatibility[s.CompatibilityDigest]-assigned[i][s.CompatibilityDigest])
				}
				count := min(remaining[si], max(0, available), int(cpu[i]/s.CPUMillicores), int(memory[i]/s.MemoryBytes))
				if count <= 0 {
					continue
				}
				remaining[si] -= count
				used[i] += count
				assigned[i][s.CompatibilityDigest] += count
				cpu[i] -= int64(count) * s.CPUMillicores
				memory[i] -= int64(count) * s.MemoryBytes
				if targets[n.NodeID] == nil {
					targets[n.NodeID] = map[string]int{}
				}
				targets[n.NodeID][class] += count
			}
		}
	}
	return targets
}
