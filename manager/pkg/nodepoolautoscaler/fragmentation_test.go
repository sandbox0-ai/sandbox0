package nodepoolautoscaler

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func TestFragmentedFreeCapacityCannotStrandOneLargeRequest(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 1}
	w := testWorker(t, store, cloud)
	w.config.HeadroomCPUMillicores, w.config.HeadroomMemoryBytes, w.config.HeadroomSlots = 0, 0, 0
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.ClusterWorkloadCPU = 24000
	store.snapshot.ClusterWorkloadMemory = 96 << 30
	store.snapshot.ClusterWorkloadSlots = 6
	store.snapshot.DemandCPUMillicores = 4000
	store.snapshot.DemandMemoryBytes = 16 << 30
	store.snapshot.DemandSlots = 1
	store.snapshot.DemandShapes = []sandboxstore.RuntimeNodePoolDemandShape{{CPUMillicores: 4000, MemoryBytes: 16 << 30, Slots: 1}}
	store.snapshot.PlacementNodes = []sandboxstore.RuntimeNodePlacementCapacity{
		{PhysicalCPU: 14000, PhysicalMemory: 56 << 30, FreeCPU: 2000, FreeMemory: 8 << 30, ReadySlots: 509},
		{PhysicalCPU: 14000, PhysicalMemory: 56 << 30, FreeCPU: 2000, FreeMemory: 8 << 30, ReadySlots: 5},
	}
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{PoolKind: "fixed", State: "active", CPUMillicores: 14000, MemoryBytes: 56 << 30,
			UsedCPUMillicores: 12000, UsedMemoryBytes: 48 << 30, ActiveLeases: 3, ReadySlots: 509, CapacityLive: true, ProviderReady: true},
		{PoolKind: "elastic", State: "active", CPUMillicores: 14000, MemoryBytes: 56 << 30,
			UsedCPUMillicores: 12000, UsedMemoryBytes: 48 << 30, ActiveLeases: 3, ReadySlots: 5, CapacityLive: true, ProviderReady: true},
	}
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	// Two separate 8-GiB remainders cannot satisfy one indivisible 16-GiB claim.
	require.Equal(t, 2, d.TargetElastic)
	require.Equal(t, []int{2}, cloud.sets)
	// The same pressure must not buy a third host while the second is booting.
	d, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, d.TargetElastic)
	require.Len(t, cloud.sets, 1)
}

func TestFixedAdmissionBudgetIsNotReplacedByElasticWorkerShape(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	w.config.HeadroomCPUMillicores, w.config.HeadroomMemoryBytes, w.config.HeadroomSlots = 0, 0, 0
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.ClusterFixedCPU, store.snapshot.ClusterFixedMemory = 64000, 70<<30
	store.snapshot.ClusterWorkloadCPU, store.snapshot.ClusterWorkloadMemory = 32000, 64<<30
	store.snapshot.ClusterWorkloadSlots = 4
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, d.TargetElastic)
	require.Empty(t, cloud.sets)
}

func TestPlacementProgressFloorRequiresAnUnplaceableRequest(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{}
	w := testWorker(t, store, cloud)
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.DemandShapes = []sandboxstore.RuntimeNodePoolDemandShape{{CPUMillicores: 4000, MemoryBytes: 16 << 30, Slots: 1}}
	store.snapshot.PlacementNodes = []sandboxstore.RuntimeNodePlacementCapacity{
		{PhysicalCPU: 14000, PhysicalMemory: 56 << 30, FreeCPU: 4000, FreeMemory: 16 << 30, ReadySlots: 1},
	}
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, d.TargetElastic)
	// Admission overcommit does not permit a request larger than the host's
	// physical per-request boundary. Another configured host can satisfy it.
	store.snapshot.PlacementNodes[0].PhysicalMemory = 8 << 30
	d, err = w.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, d.TargetElastic)
}

func TestMixedNodeBudgetsPreserveCPUAndMemoryPairing(t *testing.T) {
	store, cloud := &fakeStore{}, &fakeCloud{desired: 1}
	w := testWorker(t, store, cloud)
	w.config.HeadroomCPUMillicores, w.config.HeadroomMemoryBytes, w.config.HeadroomSlots = 0, 0, 0
	store.snapshot.ClusterFixedUsableSlots = 512
	store.snapshot.ClusterFixedCPU, store.snapshot.ClusterFixedMemory = 64000, 70<<30
	store.snapshot.ClusterWorkloadCPU, store.snapshot.ClusterWorkloadMemory = 44000, 88<<30
	store.snapshot.ClusterWorkloadSlots = 7
	store.snapshot.DemandCPUMillicores, store.snapshot.DemandMemoryBytes, store.snapshot.DemandSlots = 4000, 8<<30, 1
	store.snapshot.DemandShapes = []sandboxstore.RuntimeNodePoolDemandShape{{CPUMillicores: 4000, MemoryBytes: 8 << 30, Slots: 1}}
	store.snapshot.PlacementNodes = []sandboxstore.RuntimeNodePlacementCapacity{
		{PhysicalCPU: 14000, PhysicalMemory: 56 << 30, FreeCPU: 32000, FreeMemory: 6 << 30, ReadySlots: 508},
		{PhysicalCPU: 14000, PhysicalMemory: 56 << 30, FreeCPU: 2000, FreeMemory: 32 << 30, ReadySlots: 5},
	}
	store.snapshot.Nodes = []sandboxstore.RuntimeNodePoolNodeUsage{
		{PoolKind: "elastic", State: "active", ActiveLeases: 3, CapacityLive: true, ProviderReady: true},
	}
	d, err := w.Reconcile(t.Context())
	require.NoError(t, err)
	// Free CPU on the fixed worker cannot be paired with memory on the elastic worker.
	require.Equal(t, 2, d.TargetElastic)
}
