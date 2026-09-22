package sandboxstore

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationFinalizationStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, protocol.MigrationAdoptionRequest, protocol.MigrationAdoptionProof) {
	t.Helper()
	f, restored, ready := migrationHandoverStoreFixture(t, suffix)
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, ready.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "restore alone cannot authorize destruction")
	handover, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *handover, migrationHandoverResponse(t, *handover)))
	slot, err := f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, ready.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "committed generation still requires target adoption")
	adoption := *slot.MigrationAdoption
	d, err := adoption.Digest()
	require.NoError(t, err)
	return f, adoption, protocol.MigrationAdoptionProof{RequestDigest: d, ImageAbsent: true}
}

func migrationFinalizationStoreProof(t *testing.T, request protocol.MigrationSourceFinalizeRequest) protocol.MigrationSourceFinalizeProof {
	t.Helper()
	rootRequest, err := request.RootFSRequest()
	require.NoError(t, err)
	root := rootfshandoff.MigrationRootFSFinalizeProof{Request: rootRequest, Parent: request.SourceProof.RootFS.Session.Parent, BindingDigest: request.SourceProof.RootFS.Session.BindingDigest, BranchAbsent: true, MountDirectoriesAbsent: true}
	root.Digest, err = root.ProofDigest()
	require.NoError(t, err)
	c := request.Cleanup
	cleanup := protocol.NodeCleanupControlProof{Version: protocol.NodeCleanupProofVersion, OperationID: c.OperationID, WriterOperationID: c.WriterOperationID, WriterRetireKind: c.WriterRetireKind,
		SlotID: c.SlotID, ClusterID: c.ClusterID, AllocationID: c.AllocationID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, NetNSIdentity: c.NetNSIdentity,
		RunscContainerID: c.RunscContainerID, WriterGrantID: c.WriterGrantID, WriterAuthorityDigest: c.WriterAuthorityDigest, RootFSOperationID: c.WriterOperationID, RootFSProofDigest: root.Digest,
		Resources: c.Resources, ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: c.ResourceLeaseDigest, RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true, ResourceCgroupAbsent: true}
	cleanup.ProofDigest, err = cleanup.Digest()
	require.NoError(t, err)
	d, err := request.Digest()
	require.NoError(t, err)
	result := protocol.MigrationSourceFinalizeProof{RequestDigest: d, RootFS: root, ImageAbsent: true, Cleanup: cleanup}
	require.NoError(t, result.ValidateFor(request))
	return result
}

func TestNomadMigrationSourceFinalizationRetainsBothLeasesAfterExpiryIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "source-finalization")
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating',hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	const workers = 8
	requests := make([]*protocol.MigrationSourceFinalizeRequest, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			requests[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
		})
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, requests[0], requests[i])
	}
	request := *requests[0]
	pending, err := f.store.GetNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.Nil(t, pending)
	pending, err = f.store.GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Nil(t, pending)
	require.Equal(t, protocol.MigrationAdoptionReceipt{Request: adoption, Proof: adopted}, request.Adoption)
	require.Equal(t, f.slotID, request.Cleanup.SlotID)
	proof := migrationFinalizationStoreProof(t, request)
	for i := range workers {
		wg.Go(func() {
			errs[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationSourceFinalization(f.ctx, request, proof)
		})
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	var payload []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT source_finalization_receipt FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, adoption.OperationID).Scan(&payload))
	var actual protocol.MigrationSourceFinalizeProof
	require.NoError(t, json.Unmarshal(payload, &actual))
	require.Equal(t, proof, actual)
	receipt, err := NewPGSandboxStore(f.pool).GetNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.Equal(t, &protocol.MigrationSourceFinalizationReceipt{Request: request, Proof: proof}, receipt)
	bySlot, err := NewPGSandboxStore(f.pool).GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, receipt, bySlot)
	other, err := f.store.GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, adoption.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, other, "a destination cannot expose its predecessor's cleanup receipt")
	_, err = f.store.GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, "absent-source")
	require.ErrorIs(t, err, ErrRuntimeSlotNotFound)
	var active int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
	require.Equal(t, 2, active, "node proof cannot replace allocation absence and release capacity")
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, adoption.OperationID).Scan(&phase))
	require.Equal(t, SandboxLifecyclePhaseCommitting, phase)
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, source.State)
	visible, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, adoption.RuntimeGeneration, visible.RuntimeGeneration)
	require.Equal(t, SandboxDesiredStateTerminating, visible.DesiredState)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///replacement/control.sock' WHERE slot_id=$1`, f.slotID)
	require.NoError(t, err)
	replayed, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.Equal(t, request, *replayed, "retry keeps the authorized incarnation, not the latest endpoint observation")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET source_finalization_receipt=NULL WHERE operation_id=$1`, adoption.OperationID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET source_finalization_request=NULL,source_finalization_digest=NULL WHERE operation_id=$1`, adoption.OperationID)
	require.Error(t, err)
}

func TestNomadMigrationSourceFinalizationRejectsChangedEvidenceIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "source-finalization-conflict")
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	request, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	proof := migrationFinalizationStoreProof(t, *request)
	for _, mutate := range []func(*protocol.MigrationSourceFinalizeProof){
		func(p *protocol.MigrationSourceFinalizeProof) { p.ImageAbsent = false },
		func(p *protocol.MigrationSourceFinalizeProof) { p.Cleanup.ResourceCgroupAbsent = false },
		func(p *protocol.MigrationSourceFinalizeProof) { p.RootFS.BranchAbsent = false },
	} {
		changed := proof
		mutate(&changed)
		require.ErrorIs(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *request, changed), ErrNomadSandboxMigrationConflict)
	}
	changed := *request
	changed.Adoption.Request.CommandReadyDigest = strings.Repeat("e", 64)
	changed.Adoption.Proof.RequestDigest, err = changed.Adoption.Request.Digest()
	require.NoError(t, err)
	changedProof := migrationFinalizationStoreProof(t, changed)
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, changed, changedProof), ErrNomadSandboxMigrationConflict)
	var absent bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT source_finalization_receipt IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, adoption.OperationID).Scan(&absent))
	require.True(t, absent)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *request, proof))
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, changed, changedProof), ErrNomadSandboxMigrationConflict)
}
