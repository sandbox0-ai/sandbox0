package carrierpool

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type demandStore interface {
	ListRuntimeCarrierDemand(context.Context, string) ([]sandboxstore.RuntimeNodePoolDemandShape, error)
	RecordRuntimeNodePoolDemand(context.Context, *sandboxstore.RuntimeNodePoolDemandRequest) error
}

// PrewarmWindow describes operator-planned spare capacity, not a reservation or
// an exception to team quotas. Missed occurrences never trigger catch-up work.
type PrewarmWindow struct {
	Name                       string
	Start, End                 time.Time
	Cron                       string
	Duration                   time.Duration
	Slots                      int
	CPUMillicores, MemoryBytes int64
	SecurityClass              string
	schedule                   cron.Schedule
}

func (w *PrewarmWindow) compile() error {
	if w.Cron == "" {
		if w.Duration != 0 || w.Start.IsZero() || !w.End.After(w.Start) || w.End.Sub(w.Start) > 24*time.Hour {
			return fmt.Errorf("absolute prewarm requires start/end within 24 hours")
		}
		return nil
	}
	if !w.Start.IsZero() || !w.End.IsZero() || w.Duration < time.Minute || w.Duration > 24*time.Hour || len(w.Cron) > 256 || len(strings.Fields(w.Cron)) != 5 {
		return fmt.Errorf("cron prewarm requires five UTC fields and duration between one minute and 24 hours, without start/end")
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse("CRON_TZ=UTC " + w.Cron)
	if err != nil || schedule.Next(time.Now().UTC()).IsZero() {
		return fmt.Errorf("invalid or unsatisfiable prewarm cron expression")
	}
	w.schedule = schedule
	return nil
}

// activeEnd checks one bounded lookback, without jobs, timers, or scheduler
// state. Overlapping occurrences renew the same demand rather than adding
// capacity repeatedly. The earliest still-active occurrence is sufficient to
// bound the renewal TTL; later ones are reconsidered on the next reconcile.
func (w PrewarmWindow) activeEnd(now time.Time) (time.Time, bool) {
	if w.schedule != nil {
		start := w.schedule.Next(now.UTC().Add(-w.Duration))
		if start.IsZero() || start.After(now) {
			return time.Time{}, false
		}
		return start.Add(w.Duration), true
	}
	return w.End, !now.Before(w.Start) && now.Before(w.End)
}

func (w *Worker) demand(ctx context.Context, nodes []sandboxstore.RuntimeCarrierNode) (map[string]map[string]int, error) {
	return w.demandAt(ctx, nodes, time.Now())
}

func (w *Worker) demandAt(ctx context.Context, nodes []sandboxstore.RuntimeCarrierNode, now time.Time) (map[string]map[string]int, error) {
	store, ok := w.store.(demandStore)
	if !ok {
		return nil, nil
	}
	for _, window := range w.config.PrewarmWindows {
		end, active := window.activeEnd(now)
		if !active {
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
			Slots: window.Slots, TTL: max(time.Second, min(30*time.Second, end.Sub(now))),
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
