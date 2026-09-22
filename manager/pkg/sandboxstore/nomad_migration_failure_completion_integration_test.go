package sandboxstore

import (
	"bytes"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationFailureCompletionConsumesGenerationAndRequiresBothReleasesIntegration(t *testing.T) {
	for _, disposition := range []string{"active", "terminating", "hard-expired"} {
		t.Run(disposition, func(t *testing.T) {
			f, _, ready := migrationHandoverStoreFixture(t, "failure-terminal-"+disposition)
			_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID)
			require.NoError(t, err)
			failure, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, ready.OperationID)
			require.NoError(t, err)
			want, err := failure.Digest()
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationFailureStop(f.ctx, *failure, protocol.MigrationFailureStopProof{RequestDigest: want, ContainerID: protocol.NomadRunscContainerID(ready.SlotID), ContainerAbsent: true}))
			command, err := f.store.AuthorizeNomadSandboxMigrationFailureCleanup(f.ctx, ready.OperationID)
			require.NoError(t, err)
			proof := migrationFailureCleanupStoreProof(t, *command)
			require.NoError(t, f.store.CommitNomadSandboxMigrationFailureCleanup(f.ctx, *command, proof))
			final, err := f.store.AuthorizeNomadSandboxMigrationFailureFinalization(f.ctx, ready.OperationID)
			require.NoError(t, err)
			want, err = final.Digest()
			require.NoError(t, err)
			finalProof := protocol.MigrationFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}
			require.NoError(t, f.store.CommitNomadSandboxMigrationFailureFinalization(f.ctx, *final, finalProof))
			failed, receipt, err := f.store.GetNomadMigrationFailureForSlot(f.ctx, ready.SlotID)
			require.NoError(t, err)
			require.True(t, failed)
			require.Equal(t, *final, receipt.Request)
			require.Equal(t, finalProof, receipt.Proof)
			gc := protocol.MigrationSourceGCRequest{Target: failure.Restore.Image.Target, FinalizationDigest: want, CleanupProofDigest: proof.Cleanup.ProofDigest, AllocationAbsenceDigest: hex.EncodeToString(bytes.Repeat([]byte{0x67}, 32))}
			gcDigest, err := gc.Digest()
			require.NoError(t, err)
			ack := protocol.MigrationSourceGCAcknowledgement{RequestDigest: gcDigest}
			_, err = f.store.CompleteNomadSandboxMigrationFailure(f.ctx, ready.OperationID, gc, ack)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "artifact finalization is not allocation absence")
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted' WHERE txn_id=$1`, ready.OperationID)
			require.Error(t, err, "generic abort cannot release failure custody")
			target, err := f.store.GetRuntimeSlot(f.ctx, ready.SlotID)
			require.NoError(t, err)
			_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: target.ID, AllocationID: target.AllocationID,
				NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x67}, 32)})
			require.NoError(t, err)
			_, err = f.store.CompleteNomadSandboxMigrationFailure(f.ctx, ready.OperationID, gc, ack)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "target absence cannot substitute for source release")
			sourceCommand, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, ready.OperationID)
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *sourceCommand, migrationFinalizationStoreProof(t, *sourceCommand)))
			source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID,
				NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x68}, 32)})
			require.NoError(t, err)
			_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, ready.OperationID)
			require.NoError(t, err)
			switch disposition {
			case "terminating":
				_, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete failed migration")
			case "hard-expired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			}
			require.NoError(t, err)
			before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_failed_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.failure_completed_at IS NOT NULL THEN RAISE EXCEPTION 'injected failure completion error'; END IF; RETURN NEW; END; $$; CREATE TRIGGER reject_test_failed_completion BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_failed_completion()`)
			require.NoError(t, err)
			_, err = f.store.CompleteNomadSandboxMigrationFailure(f.ctx, ready.OperationID, gc, ack)
			require.ErrorContains(t, err, "injected failure completion error")
			target, err = f.store.GetRuntimeSlot(f.ctx, target.ID)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateOrphaned, target.State)
			require.Equal(t, RuntimeResourceLeaseActive, target.ResourceLeaseState)
			after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_failed_completion ON manager.sandbox_runtime_migrations; DROP FUNCTION manager.reject_test_failed_completion()`)
			require.NoError(t, err)
			var replicas sync.WaitGroup
			replies := make(chan error, 4)
			for range 4 {
				replicas.Add(1)
				go func() {
					defer replicas.Done()
					_, err := NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationFailure(f.ctx, ready.OperationID, gc, ack)
					replies <- err
				}()
			}
			replicas.Wait()
			close(replies)
			for err := range replies {
				require.NoError(t, err)
			}
			target, err = NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationFailure(f.ctx, ready.OperationID, gc, ack)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateTerminal, target.State)
			require.Equal(t, RuntimeResourceLeaseReleased, target.ResourceLeaseState)
			after, err = f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.EqualValues(t, 2, after.RuntimeGeneration)
			require.Empty(t, after.RuntimeID)
			require.Empty(t, after.RuntimeNamespace)
			life, err := f.store.GetLifecycleTxn(f.ctx, ready.OperationID)
			require.NoError(t, err)
			require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
			require.Contains(t, life.Error, "cannot be replayed")
			if disposition == "active" {
				require.Equal(t, SandboxDesiredStatePaused, after.DesiredState)
				resume, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: after.TeamID})
				require.NoError(t, err)
				resumeLife, err := f.store.GetLifecycleTxn(f.ctx, resume.OperationID)
				require.NoError(t, err)
				require.EqualValues(t, 2, resumeLife.FromGeneration)
				require.EqualValues(t, 3, resumeLife.ToGeneration)
			} else {
				require.Equal(t, SandboxDesiredStateTerminating, after.DesiredState)
				require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending, "staging release is still independently required")
				for range 2 {
					release, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, ready.OperationID)
					require.NoError(t, err)
					require.NotNil(t, release)
					want, err := release.Digest()
					require.NoError(t, err)
					require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *release, protocol.MigrationStagingReleased{RequestDigest: want}))
				}
				require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()))
			}
		})
	}
}
