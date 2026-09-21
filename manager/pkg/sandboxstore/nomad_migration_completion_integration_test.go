package sandboxstore

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNomadMigrationCompletionRequiresBothPhysicalProofsAndReleasesOnlySourceIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "source-completion")
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	command, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, adoption.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "command intent is not physical absence")
	proof := migrationFinalizationStoreProof(t, *command)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *command, proof))
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, adoption.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "node cleanup cannot substitute for allocation absence")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='committed' WHERE txn_id=$1`, adoption.OperationID)
	require.Error(t, err, "generic lifecycle updates cannot bypass source resource release")
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	_, err = f.store.FinalizeRuntimeSlot(f.ctx, &FinalizeRuntimeSlotRequest{SlotID: source.ID, OperationID: source.ClaimOperationID, ClaimID: source.ClaimID, Reason: "migration_source", ProofDigest: bytes.Repeat([]byte{1}, 32), ResourceLeaseID: source.ResourceLease.LeaseID, ResourceLeaseDigest: source.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	require.ErrorIs(t, err, ErrRuntimeSlotConflict, "ordinary finalization cannot partially complete migration")
	_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x83}, 32)})
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating',hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	targetBefore, err := f.store.GetRuntimeSlot(f.ctx, adoption.Target.SlotID)
	require.NoError(t, err)
	var head string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT head_generation_id FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, source.FilesystemID).Scan(&head))
	// A failed phase commit must roll back the source slot and lease release.
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_migration_completion() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.kind='migrate' AND NEW.phase='committed' THEN RAISE EXCEPTION 'injected completion failure'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_migration_completion BEFORE UPDATE ON manager.sandbox_lifecycle_txns FOR EACH ROW EXECUTE FUNCTION manager.reject_test_migration_completion()`)
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, adoption.OperationID)
	require.ErrorContains(t, err, "injected completion failure")
	retained, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateOrphaned, retained.State)
	require.Equal(t, RuntimeResourceLeaseActive, retained.ResourceLeaseState)
	require.True(t, retained.ResourceLeaseReleasedAt.IsZero())
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_migration_completion ON manager.sandbox_lifecycle_txns`)
	require.NoError(t, err)
	const workers = 8
	results := make([]*RuntimeSlot, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			results[i], errs[i] = NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationSource(f.ctx, adoption.OperationID)
		})
	}
	wg.Wait()
	historical, err := NewPGSandboxStore(f.pool).GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, *command, historical.Request)
	require.Equal(t, proof, historical.Proof)
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, RuntimeSlotStateTerminal, results[i].State)
		require.Equal(t, "migration_source", results[i].TerminalReason)
		require.Equal(t, RuntimeResourceLeaseReleased, results[i].ResourceLeaseState)
		require.Equal(t, results[0].ResourceLeaseReleasedAt, results[i].ResourceLeaseReleasedAt)
		require.Equal(t, results[0].TerminalProofDigest, results[i].TerminalProofDigest)
	}
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, adoption.OperationID).Scan(&phase))
	require.Equal(t, SandboxLifecyclePhaseCommitted, phase)
	after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before, after, "cleanup must not change routing, desired state or TTL")
	targetAfter, err := f.store.GetRuntimeSlot(f.ctx, adoption.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, targetBefore.ResourceLease, targetAfter.ResourceLease)
	require.Equal(t, RuntimeResourceLeaseActive, targetAfter.ResourceLeaseState)
	require.Equal(t, targetBefore.WriterGrantID, targetAfter.WriterGrantID)
	var headAfter string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT head_generation_id FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, source.FilesystemID).Scan(&headAfter))
	require.Equal(t, head, headAfter)
	active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active)
}

func TestNomadMigrationCompletionRejectsAllocationProofWithoutNodeReceiptIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "completion-node-proof")
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x84}, 32)})
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, adoption.OperationID)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	retained, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseActive, retained.ResourceLeaseState)
}
