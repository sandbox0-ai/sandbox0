package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadCheckpointUncertainCaptureUsesSharedPhysicalFailureRecoveryIntegration(t *testing.T) {
	for _, disposition := range []string{"active", "terminating", "expired"} {
		t.Run(disposition, func(t *testing.T) {
			n, id := newCheckpointPauseWorkerFixture(t, "checkpoint-failure-"+disposition)
			f := n.f
			advanceCheckpointPauseWorker(t, n, 3)
			work, err := f.store.GetNomadCheckpointPauseWork(f.ctx, id)
			require.NoError(t, err)
			require.NotNil(t, work.Staging)
			scope, err := work.Staging.CaptureUpload.Scope(work.Staging.Source)
			require.NoError(t, err)
			objects := objectstore.NewMemoryStore("")
			imageStore, err := runtimecheckpoint.New(objects, 1<<20)
			require.NoError(t, err)
			stage, err := imageStore.OpenCaptureStaging(f.ctx, scope, work.Staging.CaptureUpload.MaxBytes)
			require.NoError(t, err)
			_, err = stage.StageChunk(f.ctx, []byte("failed checkpoint capture"))
			require.NoError(t, err)
			blocked, err := f.store.AuthorizeNomadCheckpointCaptureUploadGC(f.ctx, id)
			require.NoError(t, err)
			require.Nil(t, blocked)
			source := n.checkpoint.Evidence.Preflight.Source
			digest, err := source.Digest()
			require.NoError(t, err)
			n.capture = &protocol.MigrationCapture{Request: source, RequestDigest: digest, State: protocol.MigrationCaptureUncertain}
			switch disposition {
			case "terminating":
				_, err = f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete uncertain memory capture")
			case "expired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			}
			require.NoError(t, err)
			advanceCheckpointPauseWorker(t, n, 1)
			command, err := f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, *n.capture)
			require.NoError(t, err)
			require.Equal(t, source, command.Capture.Request)
			require.ErrorIs(t, f.store.AuthorizeNomadCheckpointCaptureDispatch(f.ctx, source), ErrNomadCheckpointConflict)
			_, err = f.store.AuthorizeNomadCheckpointPublication(f.ctx, checkpointWorkerPublication(t, f, n.checkpoint, n.source, source).Capture)
			require.ErrorIs(t, err, ErrNomadCheckpointConflict)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',aborted_at=NOW() WHERE txn_id=$1`, id)
			require.Error(t, err, "uncertain execution cannot abort without cleanup")
			advanceCheckpointPauseWorker(t, n, 2) // Same cleanup/finalization steps used for migration.
			failed, receipt, err := f.store.GetNomadMigrationCaptureFailureForSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.True(t, failed)
			require.Nil(t, receipt, "staging release must precede allocation purge")
			advanceCheckpointPauseWorker(t, n, 1)
			failed, receipt, err = f.store.GetNomadMigrationCaptureFailureForSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.True(t, failed)
			require.NotNil(t, receipt)
			require.NoError(t, receipt.Validate())
			pending, err := f.store.ListNomadCheckpointPauses(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, pending)
			require.Zero(t, n.captureCalls, "recovery cannot recapture or start a guest")
			require.Zero(t, n.publishCalls)
			require.Zero(t, n.fenceCalls)
			fs, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
			require.NoError(t, err)
			require.Equal(t, f.initialGenerationID, fs.HeadGenerationID)
			proofDigest := bytes.Repeat([]byte{0x76}, 32)
			gc := protocol.MigrationSourceGCRequest{Target: source.Target, FinalizationDigest: receipt.Proof.RequestDigest, CleanupProofDigest: receipt.Request.Proof.Cleanup.ProofDigest, AllocationAbsenceDigest: hex.EncodeToString(proofDigest)}
			gd, err := gc.Digest()
			require.NoError(t, err)
			ack := protocol.MigrationSourceGCAcknowledgement{RequestDigest: gd}
			_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.Error(t, err, "allocation still physically present")
			slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState)
			_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ObservationDigest: proofDigest})
			require.NoError(t, err)
			// Fail the last deferred invariant after the transaction has attempted
			// lease release: none of the earlier updates may escape rollback.
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_checkpoint_failure_test() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'injected checkpoint completion failure'; END; $$;
			CREATE CONSTRAINT TRIGGER reject_checkpoint_failure_test AFTER UPDATE OF phase ON manager.sandbox_lifecycle_txns
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.phase='aborted') EXECUTE FUNCTION manager.reject_checkpoint_failure_test();`)
			require.NoError(t, err)
			_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.ErrorContains(t, err, "injected checkpoint completion failure")
			stillHeld, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseActive, stillHeld.ResourceLeaseState)
			require.Equal(t, RuntimeSlotStateOrphaned, stillHeld.State)
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_checkpoint_failure_test ON manager.sandbox_lifecycle_txns; DROP FUNCTION manager.reject_checkpoint_failure_test();`)
			require.NoError(t, err)
			slot, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateTerminal, slot.State)
			require.Equal(t, RuntimeResourceLeaseReleased, slot.ResourceLeaseState)
			require.Equal(t, "migration_capture_failed", slot.TerminalReason)
			_, err = NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
			require.NoError(t, err)
			life, err := f.store.GetLifecycleTxn(f.ctx, id)
			require.NoError(t, err)
			require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
			collector, err := runtimecheckpoint.NewCollector(objects)
			require.NoError(t, err)
			captureGC, err := nomadmigration.NewCheckpointCaptureUploadGC(f.store, collector)
			require.NoError(t, err)
			for i := 0; i < 4; i++ {
				pass, err := captureGC.RunOnce(f.ctx)
				require.NoError(t, err)
				if pass.Advanced == 1 {
					break
				}
				require.Less(t, i, 3, "failed capture objects must be collected")
			}
			require.NoError(t, f.store.CompleteNomadCheckpointCaptureUploadGC(f.ctx, scope))
			var generation int64
			var state, runtime string
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT runtime_generation,desired_state,runtime_id FROM manager.sandboxes WHERE sandbox_id=$1`, f.sandboxID).Scan(&generation, &state, &runtime))
			require.Equal(t, source.SourceGeneration, generation, "source-only failure cannot invent or regress a generation")
			require.Empty(t, runtime)
			if disposition == "active" {
				require.Equal(t, string(SandboxDesiredStatePaused), state)
			} else {
				require.Equal(t, string(SandboxDesiredStateTerminating), state)
			}
			_, err = f.store.RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
			require.Error(t, err, "failed capture must never masquerade as retained memory")
			if disposition == "active" {
				owner, err := f.store.GetSandbox(f.ctx, f.sandboxID)
				require.NoError(t, err)
				failed, err := f.store.NomadCheckpointFailed(f.ctx, owner.ID, owner.RuntimeGeneration, owner.LifecycleEpoch)
				require.NoError(t, err)
				require.True(t, failed, "public polling must not report a successful memory pause")
				// A deliberate filesystem-only recovery is a separate operation.
				// Late physical receipts must not mutate that new lifecycle.
				resume, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
					SandboxID: f.sandboxID, ExpectedTeamID: "team-slot",
				})
				require.NoError(t, err)
				require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureCleanup(f.ctx, receipt.Request.Request, receipt.Request.Proof))
				require.NoError(t, f.store.CommitNomadSandboxMigrationCaptureFailureFinalization(f.ctx, receipt.Request, receipt.Proof))
				c, err := loadCheckpointFailureTestEvidence(f, id)
				require.NoError(t, err)
				require.NoError(t, f.store.CommitNomadCheckpointFailedStagingRelease(f.ctx, *c.Staging, *c.CaptureFailureStagingReleased))
				_, err = f.store.CompleteNomadSandboxMigrationCaptureFailure(f.ctx, id, gc, ack)
				require.NoError(t, err)
				current, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
				require.NoError(t, err)
				require.Equal(t, resume.OperationID, current.ID)
				require.Equal(t, SandboxLifecyclePhasePreparing, current.Phase)
				failed, err = f.store.NomadCheckpointFailed(f.ctx, owner.ID, owner.RuntimeGeneration, current.Epoch)
				require.NoError(t, err)
				require.False(t, failed, "historical failure must not hide a new recovery")
			} else if disposition == "terminating" {
				require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now().UTC()))
				var released bool
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT source_writer_grant_ref IS NULL AND storage_released_at IS NOT NULL
					FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&released))
				require.True(t, released, "failed capture history must release terminal source storage")
			}
		})
	}
}

func TestNomadCheckpointCaptureFailureWorkerRecoversLostStoreAndNodeRepliesIntegration(t *testing.T) {
	for _, boundary := range []string{"cleanup-node", "cleanup-store", "final-node", "final-store"} {
		t.Run(boundary, func(t *testing.T) {
			n, _ := newCheckpointPauseWorkerFixture(t, "checkpoint-failure-retry")
			f := n.f
			advanceCheckpointPauseWorker(t, n, 3)
			source := n.checkpoint.Evidence.Preflight.Source
			d, err := source.Digest()
			require.NoError(t, err)
			capture := protocol.MigrationCapture{Request: source, RequestDigest: d, State: protocol.MigrationCaptureUncertain}
			_, err = f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, capture)
			require.NoError(t, err)
			store := &captureFailureTestStore{PGSandboxStore: f.store, lost: boundary}
			node := &captureFailureTestNode{t: t, lost: boundary}
			failures := 0
			for range 4 {
				work, err := f.store.GetNomadCheckpointPauseWork(f.ctx, source.OperationID)
				require.NoError(t, err)
				if work.FailureFinalized != nil {
					break
				}
				// This common coordinator step is also invoked by the memory pause lane.
				workerStore := &checkpointFailureWorkAdapter{captureFailureTestStore: store, work: work.Failure}
				worker, err := nomadmigration.NewCaptureFailure(workerStore, node)
				require.NoError(t, err)
				_, err = worker.RunOnce(f.ctx)
				if err != nil {
					failures++
				}
				store.PGSandboxStore = NewPGSandboxStore(f.pool)
			}
			require.Equal(t, 1, failures)
			work, err := f.store.GetNomadCheckpointPauseWork(f.ctx, source.OperationID)
			require.NoError(t, err)
			require.NotNil(t, work.FailureFinalized)
			slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState)
		})
	}
}

type checkpointFailureWorkAdapter struct {
	*captureFailureTestStore
	work *nomadmigration.CaptureFailureWork
}

func (s *checkpointFailureWorkAdapter) ListNomadMigrationCaptureFailures(_ context.Context, after string, _ int) ([]string, error) {
	if after != "" {
		return nil, nil
	}
	return []string{s.work.Request.Capture.Request.OperationID}, nil
}
func (s *checkpointFailureWorkAdapter) GetNomadMigrationCaptureFailure(context.Context, string) (*nomadmigration.CaptureFailureWork, error) {
	return s.work, nil
}

func TestNomadCheckpointPublicationAndUncertainFailureHaveOneWinnerIntegration(t *testing.T) {
	f, publication := checkpointPublicationFixture(t, "capture-failure-race")
	uncertain := publication.Capture
	uncertain.State, uncertain.RootFS = protocol.MigrationCaptureUncertain, nil
	var publishErr, failureErr error
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		_, publishErr = f.store.AuthorizeNomadCheckpointPublication(f.ctx, publication.Capture)
	}()
	go func() {
		defer group.Done()
		<-start
		_, failureErr = f.store.AuthorizeNomadSandboxMigrationCaptureFailure(f.ctx, uncertain)
	}()
	close(start)
	group.Wait()
	var published, failed bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT evidence ? 'publication',evidence ? 'capture_failure'
		FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, uncertain.Request.OperationID).Scan(&published, &failed))
	require.NotEqual(t, published, failed)
	if published {
		require.NoError(t, publishErr)
		require.ErrorIs(t, failureErr, ErrNomadCheckpointConflict)
	} else {
		require.NoError(t, failureErr)
		require.ErrorIs(t, publishErr, ErrNomadCheckpointConflict)
	}
}

func loadCheckpointFailureTestEvidence(f *nomadPauseStoreFixture, id string) (*NomadCheckpointEvidence, error) {
	var payload []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&payload); err != nil {
		return nil, err
	}
	var evidence NomadCheckpointEvidence
	if err := json.Unmarshal(payload, &evidence); err != nil {
		return nil, err
	}
	return &evidence, evidence.validate()
}
