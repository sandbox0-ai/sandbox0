package sandboxstore

import (
	"bytes"
	"sync"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointFinalizationStoreFixture(t *testing.T, name string) (*nomadPauseStoreFixture, *protocol.MigrationSourceFinalizeRequest) {
	t.Helper()
	f, request, proof := checkpointFenceStoreFixture(t, name)
	operation := request.PublicationRequest.Capture.Request.OperationID
	_, err := f.store.CommitNomadCheckpointPublication(f.ctx, request.PublicationRequest, request.Publication)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadCheckpointSourceFinalization(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "retention without source fencing is insufficient")
	_, err = f.store.AuthorizeNomadCheckpointSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFence(f.ctx, request, proof))
	cleanup, err := f.store.AuthorizeNomadCheckpointSourceFinalization(f.ctx, operation)
	require.NoError(t, err)
	require.NotNil(t, cleanup.Checkpoint)
	require.Empty(t, cleanup.Adoption)
	require.Nil(t, cleanup.Failure)
	require.Empty(t, cleanup.Destination())
	return f, cleanup
}

func markCheckpointAllocationMissing(t *testing.T, f *nomadPauseStoreFixture) {
	t.Helper()
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{
		SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		ObservationDigest: bytes.Repeat([]byte{0x93}, 32)})
	require.NoError(t, err)
}

func TestNomadCheckpointCompletionAtomicallyPausesAndReleasesCapacityIntegration(t *testing.T) {
	f, request := checkpointFinalizationStoreFixture(t, "checkpoint-completion")
	operation := request.Checkpoint.CheckpointID
	_, err := f.store.CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	proof := migrationFinalizationStoreProof(t, *request)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFinalization(f.ctx, *request, proof))
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "node proof cannot replace allocation absence")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='committed' WHERE txn_id=$1`, operation)
	require.Error(t, err, "generic lifecycle completion must not bypass capacity release")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints
		SET capture_upload_reservation_released_at=clock_timestamp() WHERE operation_id=$1`, operation)
	require.Error(t, err, "tentative upload capacity cannot be released before physical source cleanup")
	markCheckpointAllocationMissing(t, f)
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_checkpoint_completion() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.kind='pause' AND NEW.phase='committed' THEN RAISE EXCEPTION 'injected completion failure'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_checkpoint_completion BEFORE UPDATE ON manager.sandbox_lifecycle_txns
        FOR EACH ROW EXECUTE FUNCTION manager.reject_test_checkpoint_completion()`)
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.ErrorContains(t, err, "injected completion failure")
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateOrphaned, slot.State)
	require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState)
	owner, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, owner.DesiredState)
	require.NotEmpty(t, owner.RuntimeID, "failed transaction must roll back routing changes")
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_checkpoint_completion ON manager.sandbox_lifecycle_txns`)
	require.NoError(t, err)
	const workers = 8
	results := make([]*RuntimeSlot, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			results[i], errs[i] = NewPGSandboxStore(f.pool).CompleteNomadSandboxMemoryPause(f.ctx, operation)
		})
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, RuntimeSlotStateTerminal, results[i].State)
		require.Equal(t, "memory_pause", results[i].TerminalReason)
		require.Equal(t, RuntimeResourceLeaseReleased, results[i].ResourceLeaseState)
		require.Equal(t, results[0].ResourceLeaseReleasedAt, results[i].ResourceLeaseReleasedAt)
		require.Equal(t, results[0].TerminalProofDigest, results[i].TerminalProofDigest)
	}
	owner, err = f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, owner.DesiredState)
	require.Empty(t, owner.RuntimeID)
	require.Empty(t, owner.RuntimeNamespace)
	require.Equal(t, int64(1), owner.RuntimeGeneration)
	require.True(t, owner.ExpiresAt.IsZero())
	active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&count))
	require.Equal(t, 1, count, "completed pause retains its reusable memory and matching disk image")
	var reservationReleased, imageReclaimed bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT capture_upload_reservation_released_at IS NOT NULL,image_gc_completed_at IS NOT NULL
		FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, operation).Scan(&reservationReleased, &imageReclaimed))
	require.True(t, reservationReleased, "terminal pause releases temporary upload capacity")
	require.False(t, imageReclaimed, "retained owners still pin the checkpoint image")
	var reservedBytes int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')),0)
		FROM manager.sandbox_runtime_checkpoints
		WHERE capture_upload_gc_completed_at IS NULL AND capture_upload_reservation_released_at IS NULL`).Scan(&reservedBytes))
	require.Zero(t, reservedBytes)
	history, err := NewPGSandboxStore(f.pool).GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, *request, history.Request)
	require.Equal(t, proof, history.Proof)
}

func TestNomadCheckpointCleanupSurvivesTerminationAndRejectsChangedProofIntegration(t *testing.T) {
	f, request := checkpointFinalizationStoreFixture(t, "checkpoint-termination")
	operation := request.Checkpoint.CheckpointID
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating',hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///changed/control.sock' WHERE slot_id=$1`, f.slotID)
	require.NoError(t, err)
	retry, err := NewPGSandboxStore(f.pool).AuthorizeNomadCheckpointSourceFinalization(f.ctx, operation)
	require.NoError(t, err)
	require.Equal(t, request, retry, "retry must preserve its original node command")
	markCheckpointAllocationMissing(t, f)
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "allocation absence cannot replace node cleanup")
	proof := migrationFinalizationStoreProof(t, *request)
	wrong := proof
	wrong.Cleanup.ResourceCgroupAbsent = false
	require.ErrorIs(t, f.store.CommitNomadCheckpointSourceFinalization(f.ctx, *request, wrong), ErrNomadCheckpointConflict)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFinalization(f.ctx, *request, proof))
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=evidence-'finalized' WHERE operation_id=$1`, operation)
	require.Error(t, err)
	terminal, err := f.store.CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
	owner, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateTerminating, owner.DesiredState, "cleanup must never reverse termination")
	require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadCheckpointSourceFinalization(f.ctx, *request, proof))
}

func TestNomadCheckpointCleanupAfterOwnerDeletionKeepsHistoricalProofIntegration(t *testing.T) {
	f, request := checkpointFinalizationStoreFixture(t, "checkpoint-deleted")
	operation := request.Checkpoint.CheckpointID
	markCheckpointAllocationMissing(t, f)
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandboxes
		SET desired_state='deleted',deleted_at=NOW(),runtime_id='',runtime_namespace=''
		WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err, "deleted owner no longer requires a future restore")
	proof := migrationFinalizationStoreProof(t, *request)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFinalization(f.ctx, *request, proof))
	terminal, err := f.store.CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
	history, err := NewPGSandboxStore(f.pool).GetNomadSandboxMigrationSourceFinalizationForSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, proof, history.Proof)
	var state string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT desired_state FROM manager.sandboxes WHERE sandbox_id=$1`, f.sandboxID).Scan(&state))
	require.Equal(t, SandboxDesiredStateDeleted, state)
	retry, err := NewPGSandboxStore(f.pool).CompleteNomadSandboxMemoryPause(f.ctx, operation)
	require.NoError(t, err)
	require.Equal(t, terminal.TerminalProofDigest, retry.TerminalProofDigest)
}
