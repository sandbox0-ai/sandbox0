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
	assertSlots := func(expected int) {
		t.Helper()
		snapshot, err := s.GetRuntimeNodePoolSnapshot(t.Context(), "elastic")
		require.NoError(t, err)
		require.Equal(t, expected, snapshot.ClusterFixedUsableSlots)
		require.Equal(t, expected, snapshot.PlacementNodes[0].ReadySlots)
	}
	assertSlots(2)
	require.NoError(t, s.HeartbeatRuntimeCarrierController(t.Context(), n.ClusterID, 30*time.Second))
	assertSlots(128)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_carrier_resizes SET updated_at=NOW()-INTERVAL '3 minutes'`)
	require.NoError(t, err)
	assertSlots(2)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_carrier_resizes SET updated_at=NOW(); UPDATE manager.runtime_carrier_controllers SET heartbeat_expires_at=NOW()-INTERVAL '1 second'`)
	require.NoError(t, err)
	assertSlots(2)
	// A completed plan whose actual inventory subsequently disappears also
	// loses speculative credit; healthy controller liveness alone is not enough.
	require.NoError(t, s.CompleteRuntimeCarrierResize(t.Context(), n, nil))
	require.NoError(t, s.HeartbeatRuntimeCarrierController(t.Context(), n.ClusterID, 30*time.Second))
	assertSlots(128)
	_, err = s.pool.Exec(t.Context(), `UPDATE manager.runtime_carrier_resizes SET updated_at=NOW()-INTERVAL '3 minutes'`)
	require.NoError(t, err)
	assertSlots(2)
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
