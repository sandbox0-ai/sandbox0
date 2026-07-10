package portal

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal/nodefs"
)

var (
	nodeFSSlotRemaining = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "sandbox0",
		Subsystem: "ctld",
		Name:      "nodefs_slot_remaining",
		Help:      "Portal slots remaining in the current nodefs connection generation.",
	}, []string{"node", "shard"})
	nodeFSPortalsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "sandbox0",
		Subsystem: "ctld",
		Name:      "nodefs_portals_active",
		Help:      "Committed nodefs portals in each shard.",
	}, []string{"node", "shard"})
	nodeFSBindingGenerationRemaining = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "sandbox0",
		Subsystem: "ctld",
		Name:      "nodefs_binding_generation_remaining",
		Help:      "Minimum backend binding generations remaining among portals in each shard.",
	}, []string{"node", "shard"})
)

func recordNodeFSJournalMetrics(state nodeFSJournal) {
	if state.ShardCount <= 0 || state.NodeIdentity == "" {
		return
	}
	active := make([]int, state.ShardCount)
	minimumBindingRemaining := make([]uint64, state.ShardCount)
	for shard := range minimumBindingRemaining {
		minimumBindingRemaining[shard] = nodefs.MaxBindingGeneration
	}
	for _, portal := range state.Portals {
		if portal.Shard < 0 || portal.Shard >= state.ShardCount {
			continue
		}
		active[portal.Shard]++
		generation := max(portal.BindingGeneration, portal.PendingBindingGeneration)
		remaining := nodefs.MaxBindingGeneration - min(generation, nodefs.MaxBindingGeneration)
		if remaining < minimumBindingRemaining[portal.Shard] {
			minimumBindingRemaining[portal.Shard] = remaining
		}
	}
	for shard := 0; shard < state.ShardCount; shard++ {
		label := strconv.Itoa(shard)
		next := state.NextSlotByShard[shard]
		remaining := uint64(0)
		if next > 0 && next <= uint64(nodefs.MaxSlot) {
			remaining = uint64(nodefs.MaxSlot) - next + 1
		}
		nodeFSSlotRemaining.WithLabelValues(state.NodeIdentity, label).Set(float64(remaining))
		nodeFSPortalsActive.WithLabelValues(state.NodeIdentity, label).Set(float64(active[shard]))
		nodeFSBindingGenerationRemaining.WithLabelValues(state.NodeIdentity, label).Set(float64(minimumBindingRemaining[shard]))
	}
}
