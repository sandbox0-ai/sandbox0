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

func TestNomadMigrationCaptureFailureFencesOnlyUnpublishedSourceIntegration(t *testing.T) {
	for _, disposition := range []string{"active", "terminating", "hard-expired"} {
		t.Run(disposition, func(t *testing.T) {
			f, reservation, publication := migrationPublicationStoreFixture(t, "capture-failure-"+disposition)
			capture := publication.Capture
			capture.State, capture.RootFS = protocol.MigrationCaptureUncertain, nil
			id := capture.Request.OperationID
			var err error
			switch disposition {
			case "terminating":
				_, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete uncertain capture")
			case "hard-expired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			}
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_capture_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.capture_failure_request IS NOT NULL THEN RAISE EXCEPTION 'injected capture failure intent error'; END IF; RETURN NEW; END; $$; CREATE TRIGGER reject_test_capture_failure BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_capture_failure()`)
			require.NoError(t, err)
			_, err = f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, capture)
			require.ErrorContains(t, err, "injected capture failure intent error")
			g, err := f.store.GetRootFSWriterGrant(f.ctx, reservation.SourceWriterGrantID)
			require.NoError(t, err)
			require.Equal(t, RootFSWriterGrantStateConsumed, g.State)
			var active int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
			require.Equal(t, 2, active, "lost transaction cannot release reserved capacity")
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_capture_failure ON manager.sandbox_runtime_migrations; DROP FUNCTION manager.reject_test_capture_failure()`)
			require.NoError(t, err)
			var wg sync.WaitGroup
			commands := make([]*protocol.MigrationCaptureFailureRequest, 4)
			errs := make([]error, len(commands))
			for i := range commands {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					commands[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, capture)
				}(i)
			}
			wg.Wait()
			for i := range commands {
				require.NoError(t, errs[i])
				require.Equal(t, commands[0], commands[i])
			}
			command := *commands[0]
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
			require.Equal(t, 1, active, "only the unattached target reservation may be released")
			g, err = f.store.GetRootFSWriterGrant(f.ctx, reservation.SourceWriterGrantID)
			require.NoError(t, err)
			require.Equal(t, RootFSWriterGrantStateRetiring, g.State)
			require.Equal(t, command.Cleanup.WriterOperationID, g.RetireOperationID)
			_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			work, err := f.store.GetNomadMigrationSourceRecovery(f.ctx, id)
			require.NoError(t, err)
			require.Nil(t, work)
			_, err = f.store.AuthorizeNomadSandboxMigrationCaptureFailureFinalization(f.ctx, id)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			cd, err := command.Digest()
			require.NoError(t, err)
			proof := protocol.MigrationCaptureFailureProof{RequestDigest: cd, Cleanup: migrationNodeCleanupStoreProof(t, command.Cleanup)}
			bad := proof
			bad.Cleanup.ResourceCgroupAbsent = false
			require.Error(t, f.store.CommitNomadSandboxMigrationCaptureFailureCleanup(f.ctx, command, bad))
			require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureCleanup(f.ctx, command, proof))
			require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationCaptureFailureCleanup(f.ctx, command, proof))
			final, err := f.store.AuthorizeNomadSandboxMigrationCaptureFailureFinalization(f.ctx, id)
			require.NoError(t, err)
			fd, err := final.Digest()
			require.NoError(t, err)
			finalProof := protocol.MigrationCaptureFailureFinalizeProof{RequestDigest: fd, RootFSArtifactsAbsent: true, ImageAbsent: true}
			require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureFinalization(f.ctx, *final, finalProof))
			require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureFinalization(f.ctx, *final, finalProof))
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_failure_request=NULL,capture_failure_digest=NULL,capture_failure_cleanup_receipt=NULL,capture_failure_finalization_receipt=NULL WHERE operation_id=$1`, id)
			require.Error(t, err)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',aborted_at=NOW() WHERE txn_id=$1`, id)
			require.Error(t, err, "artifact receipt is not allocation absence or lifecycle completion")
			fs, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
			require.NoError(t, err)
			require.Equal(t, f.initialGenerationID, fs.HeadGenerationID)
			var gen int64
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT runtime_generation FROM manager.sandboxes WHERE sandbox_id=$1`, f.sandboxID).Scan(&gen))
			require.EqualValues(t, 1, gen)
			failed, receipt, err := f.store.GetNomadMigrationCaptureFailureForSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.True(t, failed)
			require.NoError(t, receipt.Validate())
			gc := protocol.MigrationSourceGCRequest{Target: capture.Request.Target, FinalizationDigest: fd, CleanupProofDigest: proof.Cleanup.ProofDigest,
				AllocationAbsenceDigest: hex.EncodeToString(bytes.Repeat([]byte{0x76}, 32))}
			gd, err := gc.Digest()
			require.NoError(t, err)
			ack := protocol.MigrationSourceGCAcknowledgement{RequestDigest: gd}
			_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			target := reservation.TargetSlot
			_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: target.ID, AllocationID: target.AllocationID,
				NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x77}, 32)})
			require.NoError(t, err)
			_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "source allocation is still retained")
			source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID,
				NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x76}, 32)})
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_capture_complete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.capture_failure_completed_at IS NOT NULL THEN RAISE EXCEPTION 'injected capture completion error'; END IF; RETURN NEW; END; $$; CREATE TRIGGER reject_test_capture_complete BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_capture_complete()`)
			require.NoError(t, err)
			_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.ErrorContains(t, err, "injected capture completion error")
			source, err = f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateOrphaned, source.State)
			require.Equal(t, RuntimeResourceLeaseActive, source.ResourceLeaseState)
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_capture_complete ON manager.sandbox_runtime_migrations; DROP FUNCTION manager.reject_test_capture_complete()`)
			require.NoError(t, err)
			for i := range errs {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_, errs[i] = NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
				}(i)
			}
			wg.Wait()
			for _, err := range errs {
				require.NoError(t, err)
			}
			source, err = f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseReleased, source.ResourceLeaseState)
			after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.EqualValues(t, 2, after.RuntimeGeneration)
			if disposition == "active" {
				require.Equal(t, SandboxDesiredStatePaused, after.DesiredState)
				resume, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: after.TeamID})
				require.NoError(t, err)
				life, err := f.store.GetLifecycleTxn(f.ctx, resume.OperationID)
				require.NoError(t, err)
				require.EqualValues(t, 3, life.ToGeneration)
			} else {
				require.Equal(t, SandboxDesiredStateTerminating, after.DesiredState)
				require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
				for range 2 {
					release, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, id)
					require.NoError(t, err)
					require.NotNil(t, release)
					digest, err := release.Digest()
					require.NoError(t, err)
					require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *release, protocol.MigrationStagingReleased{RequestDigest: digest}))
				}
				require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()))
			}
		})
	}
}

func TestNomadMigrationCaptureFailureAndPublicationAreMutuallyExclusiveIntegration(t *testing.T) {
	f, _, publication := migrationPublicationStoreFixture(t, "capture-publication-race")
	capture := publication.Capture
	capture.State, capture.RootFS = protocol.MigrationCaptureUncertain, nil
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, capture)
	}()
	wg.Wait()
	require.NotEqual(t, errs[0] == nil, errs[1] == nil, "exactly one irreversible branch may acquire authority")
	var branches int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT (publication_request IS NOT NULL)::int+(capture_failure_request IS NOT NULL)::int FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, publication.Assignment.OperationID).Scan(&branches))
	require.Equal(t, 1, branches)
	if errs[0] == nil {
		_, err := f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, capture)
		require.Error(t, err, "a missing publication reply cannot revoke publication authority")
	}
}
