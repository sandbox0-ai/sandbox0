package nodepoolautoscaler

import (
	"context"
	"fmt"
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
		MaxScaleOutStep: 299, MaxScaleInStep: 299, MaxPendingNodes: 299,
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
	store.snapshot.ClusterWorkloadSlots = 1
	store.snapshot.ClusterFixedUsableSlots = 8
	decision, err := testWorker(t, store, cloud).Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, decision.TargetElastic)
	require.Empty(t, cloud.sets)
}

func TestPartialFixedCapacityCreditsResourcesAndExactUsableSlots(t *testing.T) {
	for _, tc := range []struct {
		name         string
		fixedSlots   int
		activeLeases int
		demandSlots  int
		usedCPU      int64
		demandCPU    int64
		usedMemory   int64
		demandMemory int64
		wantElastic  int
		wantRequired int
	}{
		{name: "one retiring carrier does not erase the fixed node", fixedSlots: 7, activeLeases: 5, wantRequired: 1},
		{name: "remaining slots include exact headroom", fixedSlots: 7, activeLeases: 5, demandSlots: 1, wantRequired: 1},
		{name: "missing slot is not ready capacity", fixedSlots: 7, activeLeases: 5, demandSlots: 2, wantElastic: 1, wantRequired: 2},
		{name: "one elastic node cannot cover the remaining slot deficit", fixedSlots: 7, activeLeases: 5, demandSlots: 10, wantElastic: 2, wantRequired: 3},
		{name: "fixed CPU fits exactly with demand and headroom", fixedSlots: 7, usedCPU: 12000, demandCPU: 1000, wantRequired: 1},
		{name: "CPU deficit still adds an elastic node", fixedSlots: 7, usedCPU: 12000, demandCPU: 1001, wantElastic: 1, wantRequired: 2},
		{name: "fixed memory fits exactly with demand and headroom", fixedSlots: 7, usedMemory: 54 << 30, demandMemory: 1 << 30, wantRequired: 1},
		{name: "memory deficit still adds an elastic node", fixedSlots: 7, usedMemory: 54 << 30, demandMemory: (1 << 30) + 1, wantElastic: 1, wantRequired: 2},
		{name: "unavailable fixed node receives no resource credit", wantElastic: 1, wantRequired: 1},
		{name: "extra slots cannot credit a second fixed node", fixedSlots: 16, demandCPU: 14000, wantElastic: 1, wantRequired: 2},
		{name: "extra fixed slots are real capacity, not elastic readiness", fixedSlots: 16, demandSlots: 8, wantRequired: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, cloud := &fakeStore{}, &fakeCloud{}
			store.snapshot = sandboxstore.RuntimeNodePoolSnapshot{
				ClusterFixedUsableSlots: tc.fixedSlots,
				ClusterWorkloadSlots:    tc.activeLeases, DemandSlots: tc.demandSlots,
				ClusterWorkloadCPU: tc.usedCPU, DemandCPUMillicores: tc.demandCPU,
				ClusterWorkloadMemory: tc.usedMemory, DemandMemoryBytes: tc.demandMemory,
			}
			decision, err := testWorker(t, store, cloud).Reconcile(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.wantRequired, decision.RequiredNodes)
			require.Equal(t, tc.wantElastic, decision.TargetElastic)
			if tc.wantElastic == 0 {
				require.Empty(t, cloud.sets)
			} else {
				require.Equal(t, "scale_out", decision.Action)
				require.Equal(t, []int{tc.wantElastic}, cloud.sets)
			}
		})
	}
}

func TestPartialFixedCapacityTargetIsMinimumSufficientCapacity(t *testing.T) {
	worker := testWorker(t, &fakeStore{}, &fakeCloud{})
	for fixedSlots := 0; fixedSlots <= 8; fixedSlots++ {
		for demandSlots := 0; demandSlots <= 24; demandSlots++ {
			for _, demandCPU := range []int64{0, 13000, 13001, 28000} {
				for _, demandMemory := range []int64{0, 55 << 30, (55 << 30) + 1, 112 << 30} {
					snapshot := &sandboxstore.RuntimeNodePoolSnapshot{
						ClusterFixedUsableSlots: fixedSlots, DemandSlots: demandSlots,
						DemandCPUMillicores: demandCPU, DemandMemoryBytes: demandMemory,
					}
					liveFixed := 0
					if fixedSlots > 0 {
						liveFixed = 1
					}
					// Enumerate capacity rather than duplicating the target's rounding arithmetic.
					want := 0
					for int64(liveFixed+want)*14000 < demandCPU+1000 ||
						int64(liveFixed+want)*(56<<30) < demandMemory+(1<<30) ||
						fixedSlots+want*8 < demandSlots+1 {
						want++
					}
					elastic, required := worker.target(snapshot)
					label := fmt.Sprintf("fixedSlots=%d demandSlots=%d CPU=%d memory=%d", fixedSlots, demandSlots, demandCPU, demandMemory)
					require.Equal(t, want, elastic, label)
					require.Equal(t, liveFixed+want, required, label)
				}
			}
		}
	}
}

func TestPartialFixedCapacityStillRequiresStableScaleIn(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 1}
	store.snapshot.ClusterFixedUsableSlots = 7
	store.snapshot.ClusterWorkloadSlots = 5
	worker := testWorker(t, store, cloud)
	decision, err := worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Zero(t, decision.TargetElastic)
	require.Equal(t, "scale_in_stabilizing", decision.Action)
	require.Empty(t, cloud.sets)

	store.state.LowPressureSince = testNow.Add(-11 * time.Minute)
	decision, err = worker.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, "scale_in", decision.Action)
	require.Equal(t, []int{0}, cloud.sets)
}

func TestRejectsAnyTopologyOtherThanOnePlusZeroTo299(t *testing.T) {
	_, err := New(&fakeStore{}, &fakeCloud{}, Config{
		PoolID: "elastic", ClusterID: "nomad", OwnerID: "manager-1",
		FixedNodes: 2, MinElasticNodes: 0, MaxElasticNodes: 299,
		NodeCPUMillicores: 1, NodeMemoryBytes: 1, WarmSlotsPerNode: 1,
	})
	require.ErrorContains(t, err, "exactly one fixed")
}

func TestOperatorPolicyBoundsElasticCapacityWithinFleetLimit(t *testing.T) {
	for _, test := range []struct {
		name             string
		minimum, maximum int
		demand           int64
		expected         int
	}{
		{"idle-floor", 1, 1, 0, 1},
		{"bounded-under-pressure", 1, 1, 9_000_000, 1},
		{"scale-to-zero", 0, 1, 0, 0},
		{"small-fleet", 0, 3, 9_000_000, 3},
		{"disabled-growth", 0, 0, 9_000_000, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, cloud := &fakeStore{}, &fakeCloud{}
			config := testWorker(t, store, cloud).config
			config.MinElasticNodes, config.MaxElasticNodes = test.minimum, test.maximum
			worker, err := New(store, cloud, config)
			require.NoError(t, err)
			store.snapshot.ClusterFixedUsableSlots = 8
			store.snapshot.DemandCPUMillicores = test.demand
			decision, err := worker.Reconcile(t.Context())
			require.NoError(t, err)
			require.Equal(t, test.expected, decision.TargetElastic)
			require.Equal(t, test.expected, cloud.desired)
			for _, requested := range cloud.sets {
				require.GreaterOrEqual(t, requested, test.minimum)
				require.LessOrEqual(t, requested, test.maximum)
			}
		})
	}
}

func TestOperatorPolicyRejectsInvalidElasticBounds(t *testing.T) {
	config := testWorker(t, &fakeStore{}, &fakeCloud{}).config
	for _, bounds := range [][2]int{{-1, 1}, {2, 1}, {0, 300}, {0, -1}} {
		config.MinElasticNodes, config.MaxElasticNodes = bounds[0], bounds[1]
		_, err := New(&fakeStore{}, &fakeCloud{}, config)
		require.ErrorContains(t, err, "0 <= min <= max <= 299")
	}
}

func TestRetiringLeaseBacklogDoesNotCreateReplacementDemand(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 3}
	store.snapshot = sandboxstore.RuntimeNodePoolSnapshot{
		ClusterFixedUsableSlots: 8,
		ClusterActiveLeases:     84, ClusterUsedCPU: 6229, ClusterUsedMemory: 13287555072,
	}
	worker := testWorker(t, store, cloud)
	decision, err := worker.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, decision.TargetElastic)
	require.Equal(t, "scale_in_stabilizing", decision.Action)
	require.Empty(t, cloud.sets)
	require.Equal(t, 84, store.snapshot.ClusterActiveLeases, "cleanup authority remains unchanged")

	// A real unsatisfied claim still creates demand while those leases drain.
	store.snapshot.DemandSlots = 8
	decision, err = worker.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, decision.TargetElastic)
	require.Equal(t, 84, store.snapshot.ClusterActiveLeases)
}
