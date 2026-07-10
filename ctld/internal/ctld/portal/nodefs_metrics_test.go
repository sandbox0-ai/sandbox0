package portal

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal/nodefs"
)

func TestRecordNodeFSJournalMetricsCountsPendingBindingGeneration(t *testing.T) {
	defer nodeFSBindingGenerationRemaining.DeleteLabelValues(t.Name(), "0")
	defer nodeFSPortalsActive.DeleteLabelValues(t.Name(), "0")
	defer nodeFSSlotRemaining.DeleteLabelValues(t.Name(), "0")

	state := nodeFSJournal{
		NodeIdentity:    t.Name(),
		ShardCount:      1,
		NextSlotByShard: []uint64{2},
		Portals: []nodeFSPortalState{{
			Shard:                    0,
			BindingGeneration:        nodefs.MaxBindingGeneration - 1,
			PendingBindingGeneration: nodefs.MaxBindingGeneration,
		}},
	}

	recordNodeFSJournalMetrics(state)

	if got := testutil.ToFloat64(nodeFSBindingGenerationRemaining.WithLabelValues(t.Name(), "0")); got != 0 {
		t.Fatalf("binding generation remaining = %v, want 0", got)
	}
	if got := testutil.ToFloat64(nodeFSPortalsActive.WithLabelValues(t.Name(), "0")); got != 1 {
		t.Fatalf("active portals = %v, want 1", got)
	}
	if got := testutil.ToFloat64(nodeFSSlotRemaining.WithLabelValues(t.Name(), "0")); got != float64(nodefs.MaxSlot-1) {
		t.Fatalf("slot remaining = %v, want %d", got, nodefs.MaxSlot-1)
	}
}
