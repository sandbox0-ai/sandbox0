package sandboxstore

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func carrierResizeFixture(t *testing.T) (*PGSandboxStore, *AcquireRuntimeSlotRequest, RuntimeCarrierNode) {
	t.Helper()
	s := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	ctx := t.Context()
	fs, g := runtimeSlotTestGeneration(t, s, "carrier-sandbox", "carrier-operation")
	var registration *RegisterRuntimeSlotRequest
	for _, suffix := range []string{"a", "b"} {
		registration = runtimeSlotTestRegistration("carrier-slot-"+suffix, "carrier-allocation-"+suffix)
		_, err := registerRuntimeSlotWithTestCapacity(t, ctx, s, registration)
		require.NoError(t, err)
		_, err = s.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
			SlotID: registration.SlotID, AllocationID: registration.AllocationID, NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
			RuntimeReadyDigest: bytes.Repeat([]byte{1}, 32), NetworkReadyDigest: bytes.Repeat([]byte{2}, 32), StorageReadyDigest: bytes.Repeat([]byte{3}, 32), HeartbeatTTL: time.Minute,
		})
		require.NoError(t, err)
	}
	request := &AcquireRuntimeSlotRequest{OperationID: "carrier-operation", ClaimID: "carrier-claim", SandboxID: "carrier-sandbox",
		FilesystemID: fs.ID, SourceGenerationID: g.ID, CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources()}
	return s, request, RuntimeCarrierNode{ClusterID: registration.ClusterID, NodeID: registration.NodeID, NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID}
}

func carrierBaseline() []string {
	g := []string{}
	for i := 0; i < 8; i++ {
		g = append(g, fmt.Sprintf("warm-%d", i))
	}
	return g
}

func TestPrivilegedOnlyCarrierResizeRetiresToTwoAnchorsIntegration(t *testing.T) {
	s, _, n := carrierResizeFixture(t)
	privileged := []string{"warm-6", "warm-7", "privileged-2"}
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, privileged,
		[]string{"carrier-allocation-a", "carrier-allocation-b"})
	require.NoError(t, err)
	n.Revision = revision
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))

	_, err = s.pool.Exec(t.Context(), `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason)
		VALUES($1,$2,$3,'revoked','test privileged cleanup')`, n.ClusterID, n.NodeID, n.NodeUID)
	require.NoError(t, err)
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, nodes, 1, "a revoked node with a privileged surplus still needs reconciliation")
	require.True(t, nodes[0].Retiring)
	revision, err = s.BeginRuntimeCarrierResize(t.Context(), nodes[0], 128,
		[]string{"warm-6", "warm-7"}, []string{})
	require.NoError(t, err)
	require.Greater(t, revision, n.Revision)
	n.Revision = revision
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	nodes, err = s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Empty(t, nodes)

	_, err = s.BeginRuntimeCarrierResize(t.Context(), n, 128,
		[]string{"warm-0", "warm-6", "warm-7"}, []string{})
	require.ErrorIs(t, err, ErrRuntimeSlotInvalid, "partial historical anchors cannot be published")
}

func TestCarrierResizeKeepsRetainedCarrierClaimableIntegration(t *testing.T) {
	s, request, n := carrierResizeFixture(t)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{"carrier-allocation-b"})
	require.NoError(t, err)
	n.Revision = revision
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.True(t, nodes[0].Pending)
	slot, err := s.AcquireRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, "carrier-allocation-b", slot.AllocationID, "the retiring carrier must be skipped without fencing the whole node")
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, []string{"carrier-allocation-a"}))
	var retired, expired bool
	require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT carrier_retired,heartbeat_expires_at<=NOW() FROM manager.runtime_slots WHERE slot_id='carrier-slot-a'`).Scan(&retired, &expired))
	require.True(t, retired, "stale ready rows must not become claimable after resize acknowledgement")
	require.True(t, expired)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_slots SET carrier_retired=false,heartbeat_expires_at=NOW()+INTERVAL '1 hour' WHERE slot_id='carrier-slot-a'`)
	require.NoError(t, err)
	require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT carrier_retired,heartbeat_expires_at<=NOW() FROM manager.runtime_slots WHERE slot_id='carrier-slot-a'`).Scan(&retired, &expired))
	require.True(t, retired)
	require.True(t, expired, "late heartbeats cannot revive retired carriers")
	nodes, err = s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.False(t, nodes[0].Pending)
	require.Error(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
}

func TestCarrierResizeRechecksClaimWhichWonPreparationRaceIntegration(t *testing.T) {
	s, request, n := carrierResizeFixture(t)
	slot, err := s.AcquireRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	other := "carrier-allocation-a"
	if slot.AllocationID == other {
		other = "carrier-allocation-b"
	}
	_, err = s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{other})
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	var count int
	require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.runtime_carrier_resizes`).Scan(&count))
	require.Zero(t, count)
	ids, err := s.RuntimeCarrierBusyAllocations(t.Context(), n)
	require.NoError(t, err)
	require.Equal(t, []string{slot.AllocationID}, ids)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{slot.AllocationID, other})
	require.NoError(t, err)
	n.Revision = revision
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	newer, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{slot.AllocationID, other})
	require.NoError(t, err)
	require.Greater(t, newer, revision)
	require.Error(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil), "old completion cannot reopen a newer fence")
}

func TestCarrierResizeRecoverySurvivesCapacityExpiryIntegration(t *testing.T) {
	s, _, n := carrierResizeFixture(t)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{"carrier-allocation-a", "carrier-allocation-b"})
	require.NoError(t, err)
	n.Revision = revision
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_node_capacities SET heartbeat_expires_at=NOW()-INTERVAL '1 minute'`)
	require.NoError(t, err)
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.True(t, nodes[0].Pending)
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	nodes, err = s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Empty(t, nodes)
}

func TestAdaptiveCapacityCreditRequiresHealthyControllerAndBoundedProgressIntegration(t *testing.T) {
	s, _, n := carrierResizeFixture(t)
	_, err := s.EnsureRuntimeNodePoolState(t.Context(), "elastic", n.ClusterID)
	require.NoError(t, err)
	n.Revision, err = s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{"carrier-allocation-a", "carrier-allocation-b"})
	require.NoError(t, err)
	assertSlots := func(expected, adaptiveExpected int) {
		t.Helper()
		snapshot, err := s.GetRuntimeNodePoolSnapshot(t.Context(), "elastic")
		require.NoError(t, err)
		require.Equal(t, expected, snapshot.ClusterFixedUsableSlots)
		require.Equal(t, adaptiveExpected, snapshot.ClusterFixedAdaptiveSlots)
		require.Equal(t, expected, snapshot.PlacementNodes[0].ReadySlots)
	}
	assertSlots(2, 0)
	require.NoError(t, s.HeartbeatRuntimeCarrierController(t.Context(), n.ClusterID, 30*time.Second))
	assertSlots(128, 128)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_carrier_resizes SET updated_at=NOW()-INTERVAL '3 minutes'`)
	require.NoError(t, err)
	assertSlots(2, 0)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_carrier_resizes SET updated_at=NOW(); UPDATE manager.runtime_carrier_controllers SET heartbeat_expires_at=NOW()-INTERVAL '1 second'`)
	require.NoError(t, err)
	assertSlots(2, 0)
	// A completed plan whose actual inventory subsequently disappears also
	// loses speculative credit; healthy controller liveness alone is not enough.
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	require.NoError(t, s.HeartbeatRuntimeCarrierController(t.Context(), n.ClusterID, 30*time.Second))
	assertSlots(128, 128)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_carrier_resizes SET updated_at=NOW()-INTERVAL '3 minutes'`)
	require.NoError(t, err)
	assertSlots(2, 0)
}

func TestCarrierReadinessAndSurplusRequireLiveExactBootIntegration(t *testing.T) {
	s, request, n := carrierResizeFixture(t)
	ready, err := s.RuntimeCarrierReadyAllocations(t.Context(), n)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"carrier-allocation-a", "carrier-allocation-b"}, ready)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), ready)
	require.NoError(t, err)
	n.Revision = revision
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	require.NoError(t, s.RecordRuntimeCarrierSurplus(t.Context(), n.ClusterID, 1))
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.NotNil(t, nodes[0].SurplusSince)
	first := *nodes[0].SurplusSince
	require.NoError(t, s.RecordRuntimeCarrierSurplus(t.Context(), n.ClusterID, 1))
	nodes, err = s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Equal(t, first, *nodes[0].SurplusSince)
	_, err = s.AcquireRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, s.RecordRuntimeCarrierSurplus(t.Context(), n.ClusterID, 1))
	nodes, err = s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Nil(t, nodes[0].SurplusSince, "one busy carrier resets the continuous surplus window")
	ready, err = s.RuntimeCarrierReadyAllocations(t.Context(), n)
	require.NoError(t, err)
	require.Len(t, ready, 2, "an authenticated busy carrier still proves placement")
	n.NodeBootID = "different-boot"
	ready, err = s.RuntimeCarrierReadyAllocations(t.Context(), n)
	require.NoError(t, err)
	require.Empty(t, ready)
}

func TestRevokedNodeCanReplaceInterruptedRefillOnlyWithRetirementIntegration(t *testing.T) {
	s, _, n := carrierResizeFixture(t)
	n.Revision, _ = s.BeginRuntimeCarrierResize(t.Context(), n, 128, append(carrierBaseline(), "warm-8"), []string{"carrier-allocation-a", "carrier-allocation-b"})
	require.Positive(t, n.Revision)
	_, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{})
	require.Error(t, err, "live pending intent cannot be replaced")
	_, err = s.pool.Exec(t.Context(), `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason)
		VALUES($1,$2,$3,'revoked','test cleanup completed')`, n.ClusterID, n.NodeID, n.NodeUID)
	require.NoError(t, err)
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.True(t, nodes[0].Retiring)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{})
	require.NoError(t, err)
	require.Greater(t, revision, n.Revision)
	n.Revision = revision
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	nodes, err = s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Empty(t, nodes, "completed retirement leaves the active reconciliation set")
}

func TestCarrierResizeRebindsAdmittedSuccessorWithoutDiscardingOldCustodyIntegration(t *testing.T) {
	s, request, n := carrierResizeFixture(t)
	_, err := s.AcquireRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	n.Revision, err = s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{"carrier-allocation-a", "carrier-allocation-b"})
	require.NoError(t, err)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_node_capacities SET heartbeat_expires_at=NOW()-INTERVAL '1 minute';
		INSERT INTO manager.runtime_node_capacities(cluster_id,node_id,node_uid,node_boot_id,cpu_millicores,memory_bytes,cpuset_cpus,cpuset_mems,heartbeat_expires_at)
		SELECT cluster_id,node_id,node_uid,'successor-boot',cpu_millicores,memory_bytes,cpuset_cpus,cpuset_mems,NOW()+INTERVAL '1 minute'
		FROM manager.runtime_node_capacities`)
	require.NoError(t, err)
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "successor-boot", nodes[0].NodeBootID)
	require.True(t, nodes[0].Pending)
	require.True(t, nodes[0].StaleIdentity)
	_, err = s.BeginRuntimeCarrierResize(t.Context(), nodes[0], 128, carrierBaseline(), []string{})
	require.ErrorIs(t, err, ErrRuntimeSlotConflict, "reboot alone never releases predecessor custody")
}

func TestEmptyCarrierResizeCanRebindAdmittedSuccessorIntegration(t *testing.T) {
	s, _, n := carrierResizeFixture(t)
	n.Revision, _ = s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{"carrier-allocation-a", "carrier-allocation-b"})
	require.Positive(t, n.Revision)
	_, err := s.pool.Exec(t.Context(), `UPDATE manager.runtime_node_capacities SET heartbeat_expires_at=NOW()-INTERVAL '1 minute';
		INSERT INTO manager.runtime_node_capacities(cluster_id,node_id,node_uid,node_boot_id,cpu_millicores,memory_bytes,cpuset_cpus,cpuset_mems,heartbeat_expires_at)
		SELECT cluster_id,node_id,node_uid,'successor-boot',cpu_millicores,memory_bytes,cpuset_cpus,cpuset_mems,NOW()+INTERVAL '1 minute'
		FROM manager.runtime_node_capacities`)
	require.NoError(t, err)
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), nodes[0], 128, carrierBaseline(), []string{})
	require.NoError(t, err)
	require.Greater(t, revision, n.Revision)
	require.Error(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil), "predecessor completion must remain fenced")
}

func TestCarrierDemandAggregatesOnlyLiveUnfulfilledOperationsIntegration(t *testing.T) {
	s, request, n := carrierResizeFixture(t)
	_, err := s.EnsureRuntimeNodePoolState(t.Context(), "capacity-wait", n.ClusterID)
	require.NoError(t, err)
	for _, operation := range []string{request.OperationID, "waiting-second", "expired"} {
		require.NoError(t, s.RecordRuntimeNodePoolDemand(t.Context(), &RuntimeNodePoolDemandRequest{
			PoolID: "capacity-wait", ClusterID: n.ClusterID, OperationID: operation, CompatibilityDigest: request.CompatibilityDigest,
			CPUMillicores: request.Resources.CPUMillicores, MemoryBytes: request.Resources.MemoryBytes, Slots: 1, TTL: time.Minute,
		}))
	}
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_node_pool_demands SET expires_at=NOW()-INTERVAL '1 second' WHERE operation_id='expired'`)
	require.NoError(t, err)
	shapes, err := s.ListRuntimeCarrierDemand(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, shapes, 1)
	require.Equal(t, 2, shapes[0].Slots)
	// Repeated pressure for the same operation does not multiply its capacity.
	require.NoError(t, s.RecordRuntimeNodePoolDemand(t.Context(), &RuntimeNodePoolDemandRequest{PoolID: "capacity-wait", ClusterID: n.ClusterID, OperationID: request.OperationID, CompatibilityDigest: request.CompatibilityDigest, CPUMillicores: request.Resources.CPUMillicores, MemoryBytes: request.Resources.MemoryBytes, Slots: 1, TTL: time.Minute}))
	_, err = s.AcquireRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	shapes, err = s.ListRuntimeCarrierDemand(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, shapes, 1)
	require.Equal(t, 1, shapes[0].Slots)
	shapes, err = s.ListRuntimeCarrierDemand(t.Context(), "other-cluster")
	require.NoError(t, err)
	require.Empty(t, shapes)
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Positive(t, nodes[0].PhysicalCPU)
	require.Positive(t, nodes[0].PhysicalMemory)
}

func TestCarrierPartialAdmissionKeepsResizePendingIntegration(t *testing.T) {
	s, request, n := carrierResizeFixture(t)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{})
	require.NoError(t, err)
	n.Revision = revision
	_, err = s.AcquireRuntimeSlot(t.Context(), request)
	require.Error(t, err, "unpublished ready carriers remain fenced")
	var started time.Time
	require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT updated_at FROM manager.runtime_carrier_resizes`).Scan(&started))
	for i := 0; i < 2; i++ {
		require.NoError(t, s.AdmitRuntimeCarrierReadyAllocations(t.Context(), n, []string{"carrier-allocation-a"}))
	}
	nodes, err := s.ListRuntimeCarrierNodes(t.Context(), n.ClusterID)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.True(t, nodes[0].Pending)
	require.Equal(t, []string{"carrier-allocation-a"}, nodes[0].RetainedAllocations)
	var after time.Time
	require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT updated_at FROM manager.runtime_carrier_resizes`).Scan(&after))
	require.Equal(t, started, after, "partial progress must not renew speculative provisioning credit")
	slot, err := s.AcquireRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, "carrier-allocation-a", slot.AllocationID)
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	require.ErrorIs(t, s.AdmitRuntimeCarrierReadyAllocations(t.Context(), n, []string{"carrier-allocation-b"}), ErrRuntimeSlotConflict)
}

func TestCarrierPartialAdmissionRejectsStaleProofIntegration(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		change    func(*RuntimeCarrierNode)
	}{
		{name: "revision", change: func(n *RuntimeCarrierNode) { n.Revision++ }},
		{name: "boot", change: func(n *RuntimeCarrierNode) { n.NodeBootID = "stale-boot" }},
		{name: "expired_slot", sql: `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 minute' WHERE allocation_id='carrier-allocation-b'`},
		{name: "retired_slot", sql: `UPDATE manager.runtime_slots SET carrier_retired=true WHERE allocation_id='carrier-allocation-b'`},
		{name: "expired_node", sql: `UPDATE manager.runtime_node_capacities SET heartbeat_expires_at=NOW()-INTERVAL '1 minute'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, n := carrierResizeFixture(t)
			revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{})
			require.NoError(t, err)
			n.Revision = revision
			if tc.sql != "" {
				_, err = s.pool.Exec(t.Context(), tc.sql)
				require.NoError(t, err)
			}
			if tc.change != nil {
				tc.change(&n)
			}
			require.ErrorIs(t, s.AdmitRuntimeCarrierReadyAllocations(t.Context(), n, []string{"carrier-allocation-a", "carrier-allocation-b"}), ErrRuntimeSlotConflict)
			var count int
			require.NoError(t, s.pool.QueryRow(t.Context(), `SELECT cardinality(retained_allocations) FROM manager.runtime_carrier_resizes`).Scan(&count))
			require.Zero(t, count, "an invalid member must reject the entire publication")
		})
	}
}

func TestCarrierPartialAdmissionPreservesNodeDrainFenceIntegration(t *testing.T) {
	s, _, n := carrierResizeFixture(t)
	revision, err := s.BeginRuntimeCarrierResize(t.Context(), n, 128, carrierBaseline(), []string{})
	require.NoError(t, err)
	n.Revision = revision
	_, err = s.pool.Exec(t.Context(), `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason) VALUES($1,$2,$3,'draining','test drain')`, n.ClusterID, n.NodeID, n.NodeUID)
	require.NoError(t, err)
	require.ErrorIs(t, s.AdmitRuntimeCarrierReadyAllocations(t.Context(), n, []string{"carrier-allocation-a"}), ErrRuntimeSlotConflict)
}
