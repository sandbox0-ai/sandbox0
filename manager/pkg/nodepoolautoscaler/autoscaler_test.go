package nodepoolautoscaler

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	state    sandboxstore.RuntimeNodePoolState
	snapshot sandboxstore.RuntimeNodePoolSnapshot
	leader   bool
	updates  []sandboxstore.RuntimeNodePoolState
}

func (f *fakeStore) EnsureRuntimeNodePoolState(context.Context, string, string) (*sandboxstore.RuntimeNodePoolState, error) {
	return &f.state, nil
}
func (f *fakeStore) AcquireRuntimeNodePoolControllerLease(context.Context, string, string, time.Duration) (bool, error) {
	return f.leader, nil
}
func (f *fakeStore) GetRuntimeNodePoolSnapshot(context.Context, string) (*sandboxstore.RuntimeNodePoolSnapshot, error) {
	f.snapshot.State = f.state
	return &f.snapshot, nil
}
func (f *fakeStore) UpdateRuntimeNodePoolScaleState(_ context.Context, _ string, desired int, low time.Time, direction string) (*sandboxstore.RuntimeNodePoolState, error) {
	f.state.DesiredNodes = desired
	f.state.LowPressureSince = low
	if direction == "out" {
		f.state.LastScaleOutAt = testNow
	}
	if direction == "in" {
		f.state.LastScaleInAt = testNow
	}
	f.updates = append(f.updates, f.state)
	return &f.state, nil
}

type fakeCloud struct {
	desired int
	sets    []int
}

func (f *fakeCloud) DesiredCapacity(context.Context) (int, error) { return f.desired, nil }
func (f *fakeCloud) SetDesiredCapacity(_ context.Context, desired int) error {
	f.desired = desired
	f.sets = append(f.sets, desired)
	return nil
}

var testNow = time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)

func testWorker(t *testing.T, store *fakeStore, cloud *fakeCloud) *Worker {
	t.Helper()
	store.leader = true
	store.state = sandboxstore.RuntimeNodePoolState{PoolID: "elastic", ClusterID: "nomad", DesiredNodes: cloud.desired}
	worker, err := New(store, cloud, Config{
		PoolID: "elastic", ClusterID: "nomad", OwnerID: "manager-1",
		FixedNodes: 1, MinElasticNodes: 0, MaxElasticNodes: 299,
		NodeCPUMillicores: 14000, NodeMemoryBytes: 56 << 30, WarmSlotsPerNode: 8,
		HeadroomCPUMillicores: 1000, HeadroomMemoryBytes: 1 << 30, HeadroomSlots: 1,
		Interval: time.Second, ControllerLeaseTTL: 3 * time.Second,
		ScaleInStabilization: 10 * time.Minute, Now: func() time.Time { return testNow },
	})
	require.NoError(t, err)
	return worker
}

func TestIdleCapacityFitsFixedNode(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	store.snapshot.ClusterFixedUsableSlots = 8
	decision, err := testWorker(t, store, cloud).Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, decision.RequiredNodes)
	require.Equal(t, 0, decision.TargetElastic)
	require.Empty(t, cloud.sets)
}

func TestDemandScalesOutImmediatelyAndCapsAt299(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	store.snapshot.ClusterFixedUsableSlots = 8
	store.snapshot.DemandCPUMillicores = 9_000_000
	decision, err := testWorker(t, store, cloud).Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 299, decision.TargetElastic)
	require.Equal(t, "scale_out", decision.Action)
	require.Equal(t, []int{299}, cloud.sets)
}

func TestScaleInWaitsForStableLowPressure(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 2}
	store.snapshot.ClusterFixedUsableSlots = 8
	worker := testWorker(t, store, cloud)

	decision, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scale_in_stabilizing", decision.Action)
	require.Empty(t, cloud.sets)

	store.state.LowPressureSince = testNow.Add(-11 * time.Minute)
	decision, err = worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scale_in", decision.Action)
	require.Equal(t, []int{0}, cloud.sets)
}

func TestOnlyLeaseOwnerMutatesCloud(t *testing.T) {
	store, cloud := &fakeStore{leader: false}, &fakeCloud{}
	store.snapshot.ClusterFixedUsableSlots = 8
	worker := testWorker(t, store, cloud)
	store.leader = false
	store.snapshot.DemandCPUMillicores = 20_000
	decision, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "not_leader", decision.Action)
	require.Empty(t, cloud.sets)
}

func TestUnavailableFixedNodeIsReplacedByElasticCapacity(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	decision, err := testWorker(t, store, cloud).Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, decision.RequiredNodes)
	require.Equal(t, 1, decision.TargetElastic)
	require.Equal(t, []int{1}, cloud.sets)
}

func TestClaimedFixedCarrierStillCountsTowardBaseline(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	store.snapshot.ClusterActiveLeases = 1
	store.snapshot.ClusterFixedUsableSlots = 8
	decision, err := testWorker(t, store, cloud).Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, decision.TargetElastic)
	require.Empty(t, cloud.sets)
}

func TestRejectsAnyTopologyOtherThanOnePlusZeroTo299(t *testing.T) {
	_, err := New(&fakeStore{}, &fakeCloud{}, Config{
		PoolID: "elastic", ClusterID: "nomad", OwnerID: "manager-1",
		FixedNodes: 2, MinElasticNodes: 0, MaxElasticNodes: 299,
		NodeCPUMillicores: 1, NodeMemoryBytes: 1, WarmSlotsPerNode: 1,
	})
	require.ErrorContains(t, err, "exactly one fixed")
}
