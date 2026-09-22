package sandboxstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func abortRegistrationFixture(registration *RegisterRuntimeSlotRequest) protocol.RegistrationAbortRequest {
	return protocol.RegistrationAbortRequest{SlotID: registration.SlotID, ClusterID: registration.ClusterID, AllocationID: registration.AllocationID, NodeID: registration.NodeID, NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID, NetNSIdentity: registration.NetNSIdentity}
}

func TestRegistrationAbortFencesLateRegistrationIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	registration := runtimeSlotTestRegistration("abort-slot", "abort-allocation")
	request := abortRegistrationFixture(registration)
	first, err := store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.ValidateFor(request))
	require.False(t, first.Registered)
	require.False(t, first.Completed)
	retry, err := store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, first, retry)
	_, err = store.RegisterRuntimeSlot(t.Context(), registration)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	changed := request
	changed.NodeBootID = "another-boot"
	_, err = store.AbortRuntimeSlotRegistration(t.Context(), changed)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	cleanup := *first.Cleanup
	proof := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: cleanup.OperationID, SlotID: cleanup.SlotID, ClusterID: cleanup.ClusterID, AllocationID: cleanup.AllocationID, NodeID: cleanup.NodeID, NodeUID: cleanup.NodeUID, NodeBootID: cleanup.NodeBootID, NetNSIdentity: cleanup.NetNSIdentity, RunscContainerID: cleanup.RunscContainerID, RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true}
	proof.ProofDigest, err = proof.Digest()
	require.NoError(t, err)
	request.Proof = &proof
	finished, err := store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.NoError(t, err)
	require.True(t, finished.Completed)
	again, err := store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, finished, again)
	_, err = store.RegisterRuntimeSlot(t.Context(), registration)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict, "completion must retain the insertion fence")
	_, err = store.GetRuntimeSlot(t.Context(), request.SlotID)
	require.ErrorIs(t, err, ErrRuntimeSlotNotFound)
	var leases int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM manager.runtime_resource_leases WHERE slot_id=$1`, request.SlotID).Scan(&leases))
	require.Zero(t, leases)
}

func TestRegistrationAbortPreservesRegisteredAuthorityIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	registration := runtimeSlotTestRegistration("known-slot", "known-allocation")
	slot, err := store.RegisterRuntimeSlot(t.Context(), registration)
	require.NoError(t, err)
	request := abortRegistrationFixture(registration)
	response, err := store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.NoError(t, err)
	require.True(t, response.Registered)
	require.Nil(t, response.Cleanup)
	stored, err := store.GetRuntimeSlot(t.Context(), slot.ID)
	require.NoError(t, err)
	require.Equal(t, slot.Revision, stored.Revision)
	require.Equal(t, slot.State, stored.State)
	var fences int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM manager.runtime_slot_registration_aborts WHERE slot_id=$1`, slot.ID).Scan(&fences))
	require.Zero(t, fences)
	request.NodeUID = "another-node"
	_, err = store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	request = abortRegistrationFixture(registration)
	cleanup, err := request.CleanupRequest()
	require.NoError(t, err)
	payload, err := json.Marshal(cleanup)
	require.NoError(t, err)
	_, err = store.pool.Exec(t.Context(), `INSERT INTO manager.runtime_slot_registration_aborts(slot_id,cleanup_request) VALUES($1,$2)`, slot.ID, payload)
	require.Error(t, err, "database exclusion must also protect direct inserts")
}

func TestRegistrationAbortDatabaseFenceSerializesLegacyInsertIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	// Bound the concurrent fence exercise, independently of schema setup.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	registration := runtimeSlotTestRegistration("race-slot", "race-allocation")
	request := abortRegistrationFixture(registration)
	cleanup, err := request.CleanupRequest()
	require.NoError(t, err)
	payload, err := json.Marshal(cleanup)
	require.NoError(t, err)
	tx, err := store.pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO manager.runtime_slot_registration_aborts(slot_id,cleanup_request) VALUES($1,$2)`, request.SlotID, payload)
	require.NoError(t, err)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() { close(started); _, err := store.RegisterRuntimeSlot(ctx, registration); done <- err }()
	<-started
	select {
	case err := <-done:
		t.Fatalf("legacy INSERT escaped the uncommitted database fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))
	require.ErrorIs(t, <-done, ErrRuntimeSlotConflict)
	_, err = store.GetRuntimeSlot(ctx, request.SlotID)
	require.ErrorIs(t, err, ErrRuntimeSlotNotFound)
}

func TestRegistrationAbortCannotReleaseClaimedResourcesIntegration(t *testing.T) {
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	_, slot := sandboxRuntimeClaimSlotFixture(t, store, "abort-resource-boundary")
	require.NotEmpty(t, slot.ResourceLease.LeaseID)
	request := protocol.RegistrationAbortRequest{
		SlotID: slot.ID, ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
		NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, NetNSIdentity: slot.NetNSIdentity,
	}
	readLease := func() []byte {
		t.Helper()
		var payload []byte
		require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT to_jsonb(l) FROM manager.runtime_resource_leases l WHERE lease_id=$1`, slot.ResourceLease.LeaseID).Scan(&payload))
		return payload
	}
	before := readLease()
	response, err := store.AbortRuntimeSlotRegistration(t.Context(), request)
	require.NoError(t, err)
	require.True(t, response.Registered)
	require.Nil(t, response.Cleanup)
	require.Equal(t, before, readLease())
	after, err := store.GetRuntimeSlot(t.Context(), slot.ID)
	require.NoError(t, err)
	require.Equal(t, slot.State, after.State)
	require.Equal(t, slot.ClaimID, after.ClaimID)
	require.Equal(t, slot.ResourceLease.LeaseID, after.ResourceLease.LeaseID)
	require.Equal(t, slot.Revision, after.Revision)
}
