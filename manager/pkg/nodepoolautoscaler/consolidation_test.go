package nodepoolautoscaler

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func TestConsolidationWaitsForQuietWindowThenFencesLightestElasticNode(t *testing.T) {
	store := &fakeStore{consolidationReady: true}
	cloud := &fakeCloud{desired: 2}
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.ClusterWorkloadCPU = 2_000
	store.snapshot.ClusterWorkloadMemory = 2 << 30
	store.snapshot.ClusterWorkloadSlots = 2
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{ProviderInstanceID: "heavy", PoolKind: "elastic", State: "active", ProviderReady: true,
			CapacityLive: true, ActiveLeases: 2, UsedMemoryBytes: 2 << 30},
		{ProviderInstanceID: "light", PoolKind: "elastic", State: "active", ProviderReady: true,
			CapacityLive: true, ActiveLeases: 1, UsedMemoryBytes: 1 << 30},
	}
	w := testWorker(t, store, cloud)
	w.config.ConsolidationEnabled = true
	decision, err := w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "consolidation_stabilizing", decision.Action)
	require.Empty(t, store.consolidationCalls)
	require.Empty(t, cloud.sets)

	store.state.LowPressureSince = testNow.Add(-11 * time.Minute)
	decision, err = w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "consolidation_started", decision.Action)
	require.Equal(t, []string{"light"}, store.consolidationCalls)
	require.Empty(t, cloud.sets)
}

func TestConsolidationShrinksOnlyAfterSourceLeasesClear(t *testing.T) {
	store := &fakeStore{}
	cloud := &fakeCloud{desired: 2}
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.ClusterWorkloadCPU = 1_000
	store.snapshot.ClusterWorkloadMemory = 1 << 30
	store.snapshot.ClusterWorkloadSlots = 1
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{ProviderInstanceID: "source", PoolKind: "elastic", State: "draining", DrainReason: sandboxstore.RuntimeNodeConsolidationReason,
			CapacityLive: true, ProviderReady: true, ActiveLeases: 1},
		{ProviderInstanceID: "other", PoolKind: "elastic", State: "active", ProviderReady: true, CapacityLive: true},
	}
	w := testWorker(t, store, cloud)
	w.config.ConsolidationEnabled = true
	store.state.LowPressureSince = testNow.Add(-11 * time.Minute)
	decision, err := w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scale_in_waiting_for_drain", decision.Action)
	require.Empty(t, cloud.sets)

	store.snapshot.Nodes[0].ActiveLeases = 0
	decision, err = w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scale_in", decision.Action)
	require.Equal(t, []int{1}, cloud.sets)
	require.Equal(t, []string{"other=true", "source=false"}, cloud.protection)
}

func TestConsolidationKeepsCloudSizeWhenFixedCapacityIsUnavailable(t *testing.T) {
	store := &fakeStore{}
	cloud := &fakeCloud{desired: 1}
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.ClusterWorkloadCPU = 1_000
	store.snapshot.ClusterWorkloadMemory = 1 << 30
	store.snapshot.ClusterWorkloadSlots = 1
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{{
		ProviderInstanceID: "source", PoolKind: "elastic", State: "active",
		CapacityLive: true, ProviderReady: true, ActiveLeases: 1,
	}}
	w := testWorker(t, store, cloud)
	w.config.ConsolidationEnabled = true
	store.state.LowPressureSince = testNow.Add(-11 * time.Minute)
	decision, err := w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "consolidation_no_fixed_capacity", decision.Action)
	require.Equal(t, []string{"source"}, store.consolidationCalls)
	require.Empty(t, cloud.sets)
}

func TestConsolidationKeepsQuietWindowAfterEmptyNodeScaleIn(t *testing.T) {
	store := &fakeStore{consolidationReady: true}
	cloud := &fakeCloud{desired: 2}
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.ClusterWorkloadCPU = 1_000
	store.snapshot.ClusterWorkloadMemory = 1 << 30
	store.snapshot.ClusterWorkloadSlots = 1
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{ProviderInstanceID: "occupied", PoolKind: "elastic", State: "active", ProviderReady: true,
			CapacityLive: true, ActiveLeases: 5},
		{ProviderInstanceID: "empty", PoolKind: "elastic", State: "active", ProviderReady: true,
			CapacityLive: true},
	}
	w := testWorker(t, store, cloud)
	w.config.ConsolidationEnabled = true
	quietSince := testNow.Add(-11 * time.Minute)
	store.state.LowPressureSince = quietSince

	decision, err := w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scale_in", decision.Action)
	require.Equal(t, []int{1}, cloud.sets)
	require.Equal(t, quietSince, store.state.LowPressureSince,
		"an empty-node removal must not impose another full quiet window")

	w.config.Now = func() time.Time { return testNow.Add(2 * time.Minute) }
	decision, err = w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "consolidation_started", decision.Action)
	require.Equal(t, []string{"occupied"}, store.consolidationCalls,
		"source occupancy is independent of the one-migration-per-node limit")
}

func TestDisabledConsolidationStillReopensStalledSource(t *testing.T) {
	store := &fakeStore{consolidationCancelled: true}
	cloud := &fakeCloud{desired: 1}
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{{
		ProviderInstanceID: "source", PoolKind: "elastic", State: "draining",
		DrainReason:  sandboxstore.RuntimeNodeConsolidationReason,
		ActiveLeases: 1, DrainStartedAt: testNow.Add(-31 * time.Minute),
	}}
	w := testWorker(t, store, cloud)
	decision, err := w.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "consolidation_cancelled", decision.Action)
	require.Equal(t, []string{"source"}, store.cancellationCalls)
	require.Empty(t, cloud.sets)
}
