package nodepoolautoscaler

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func TestFixedCarrierInventoryIsNotCappedByElasticReadiness(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.ClusterWorkloadSlots = 30
	store.snapshot.ClusterWorkloadCPU = 4500
	store.snapshot.ClusterWorkloadMemory = 30 * 128 << 20
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, d.TargetElastic)
	require.Empty(t, cloud.sets)
}

func TestElasticCapacityAndReadinessAreIndependent(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	w.config.ElasticSlotsPerNode = 200
	store.snapshot.ClusterFixedUsableSlots = 30
	store.snapshot.DemandSlots = 200
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, d.TargetElastic)
	require.Equal(t, 8, w.config.WarmSlotsPerNode)
}

func TestScaleOutStepDoesNotHideFullTarget(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	w.config.MaxScaleOutStep = 2
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.DemandCPUMillicores = 1_000_000
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Greater(t, d.TargetElastic, 2)
	require.Equal(t, 2, d.AppliedElastic)
	require.Equal(t, []int{2}, cloud.sets)
	require.Equal(t, 2, store.state.DesiredNodes)
}

func TestBoundedScaleInPreservesStabilityAndUsesSeparateCooldown(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 3}
	w := testWorker(t, store, cloud)
	w.config.MaxScaleInStep = 1
	store.snapshot.ClusterFixedUsableSlots = 8
	quiet := testNow.Add(-11 * time.Minute)
	store.state.LowPressureSince = quiet
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, d.AppliedElastic)
	require.Equal(t, quiet, store.state.LowPressureSince)
	d, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, "scale_in_cooldown", d.Action)
	require.Equal(t, []int{2}, cloud.sets)
	store.state.LastScaleInAt = testNow.Add(-time.Minute)
	d, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, "scale_in", d.Action)
	require.Equal(t, []int{2, 1}, cloud.sets)
}

func TestExpiredPressureCannotCancelWarmingNodes(t *testing.T) {
	for _, state := range []string{"unregistered", "enrolling", "active"} {
		t.Run(state, func(t *testing.T) {
			store, cloud := &fakeStore{}, &fakeCloud{desired: 1}
			w := testWorker(t, store, cloud)
			store.snapshot.ClusterFixedUsableSlots = 8
			store.state.LowPressureSince = testNow.Add(-time.Hour)
			if state == "unregistered" {
				store.state.LastScaleOutAt = testNow.Add(-3 * time.Minute)
			} else {
				store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{{PoolKind: "elastic", State: state}}
			}
			d, err := w.Reconcile(t.Context())
			require.NoError(t, err)
			require.Contains(t, d.Action, "scale_in_waiting_for_")
			require.Empty(t, cloud.sets)
		})
	}
}

func TestHistoricalEnrollmentCannotBlockFullyReadyDesiredCapacity(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 2}
	w := testWorker(t, store, cloud)
	w.config.MaxScaleInStep = 1
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{PoolKind: "elastic", State: "enrolling"},
		{PoolKind: "elastic", State: "active", ProviderReady: false},
		{PoolKind: "elastic", State: "active", CapacityLive: true, ProviderReady: true, ActiveLeases: 1},
		{PoolKind: "elastic", State: "active", CapacityLive: true, ProviderReady: true},
	}
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, "scale_in_stabilizing", d.Action)
	require.Empty(t, cloud.sets)
	store.state.LowPressureSince = testNow.Add(-11 * time.Minute)
	d, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, d.TargetElastic)
	require.Equal(t, []int{1}, cloud.sets)
	// Historical records and their cleanup obligations are not revoked or erased.
	require.Equal(t, "enrolling", store.snapshot.Nodes[0].State)
}

func TestBusyElasticNodesCannotBeVirtuallyPackedOntoFixedNode(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 2}
	w := testWorker(t, store, cloud)
	w.config.MaxElasticNodes = 0 // An operator ceiling is not permission to evict.
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{PoolKind: "elastic", State: "active", ActiveLeases: 1, ProviderReady: true},
		{PoolKind: "elastic", State: "active", ActiveLeases: 0, ProviderReady: true},
	}
	store.state.LowPressureSince = testNow.Add(-time.Hour)
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, d.TargetElastic)
	require.Equal(t, []int{1}, cloud.sets)
}

func TestCapacityLimitReportsUnavailableFixedNode(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	w.config.MaxElasticNodes = 0
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.True(t, d.CapacityLimited)
	require.Empty(t, cloud.sets)
}

func TestDemandRecoveryResetsQuietWindowEvenDuringWarmup(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 1}
	w := testWorker(t, store, cloud)
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.DemandSlots = 8
	store.state.LowPressureSince = testNow.Add(-time.Hour)
	store.state.LastScaleOutAt = testNow.Add(-time.Minute)
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, "stable", d.Action)
	require.True(t, store.state.LowPressureSince.IsZero())
}

func TestNewPolicyDefaultsAndValidation(t *testing.T) {
	base := testWorker(t, &fakeStore{}, &fakeCloud{}).config
	base.MaxScaleOutStep, base.MaxScaleInStep = 0, 0
	w, err := New(&fakeStore{}, &fakeCloud{}, base)
	require.NoError(t, err)
	require.Equal(t, 10, w.config.MaxScaleOutStep)
	require.Equal(t, 1, w.config.MaxScaleInStep)
	for _, change := range []func(*Config){
		func(c *Config) { c.ElasticSlotsPerNode = 7 },
		func(c *Config) { c.ElasticSlotsPerNode = 577 },
		func(c *Config) { c.MaxScaleInStep = -1 },
		func(c *Config) { c.MaxScaleOutStep = 300 },
		func(c *Config) { c.MaxPendingNodes = -1 },
		func(c *Config) { c.ScaleInCooldown = -time.Second },
		func(c *Config) { c.ScaleOutWarmup = 2 * time.Hour },
	} {
		c := base
		change(&c)
		_, err := New(&fakeStore{}, &fakeCloud{}, c)
		require.Error(t, err)
	}
}

func TestUnregisteredInstancesConsumePendingBudget(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 2}
	w := testWorker(t, store, cloud)
	w.config.MaxPendingNodes = 2
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.DemandCPUMillicores = 1_000_000
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, "scale_out_pending_budget", d.Action)
	require.Empty(t, cloud.sets)
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{PoolKind: "elastic", State: "active", CapacityLive: true, ProviderReady: true},
	}
	d, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, d.AppliedElastic)
	require.Equal(t, []int{3}, cloud.sets)
}

func TestScaleInWaitsForPreviousLiveDrain(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 2}
	w := testWorker(t, store, cloud)
	store.snapshot.ClusterFixedUsableSlots = 512
	store.state.LowPressureSince = testNow.Add(-time.Hour)
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{{PoolKind: "elastic", State: "draining", CapacityLive: true}}
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, "scale_in_waiting_for_drain", d.Action)
	require.Empty(t, cloud.sets)
}

func TestRenewedPressureResetsQuietWindowWhenScaleOutIsBlocked(t *testing.T) {
	for _, guard := range []string{"cooldown", "pending_budget"} {
		t.Run(guard, func(t *testing.T) {
			store, cloud := &fakeStore{}, &fakeCloud{desired: 2}
			w := testWorker(t, store, cloud)
			store.snapshot.ClusterFixedUsableSlots = 512
			store.snapshot.DemandCPUMillicores = 1_000_000
			store.state.LowPressureSince = testNow.Add(-time.Hour)
			if guard == "cooldown" {
				w.config.ScaleOutCooldown = time.Minute
				store.state.LastScaleOutAt = testNow.Add(-time.Second)
			} else {
				w.config.MaxPendingNodes = 2
			}
			d, err := w.Reconcile(t.Context())
			require.NoError(t, err)
			require.Equal(t, "scale_out_"+guard, d.Action)
			require.True(t, store.state.LowPressureSince.IsZero())
			require.True(t, d.LowPressureAt.IsZero())
			require.Empty(t, cloud.sets)
		})
	}
}

func TestSpareCarriersCannotReplaceExhaustedComputeCapacity(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	store.snapshot.ClusterFixedUsableSlots = 200
	store.snapshot.ClusterWorkloadSlots = 30
	store.snapshot.ClusterWorkloadCPU = w.config.NodeCPUMillicores
	store.snapshot.DemandCPUMillicores = 150
	store.snapshot.DemandSlots = 1
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, d.TargetElastic)
	require.Equal(t, []int{1}, cloud.sets)
}
