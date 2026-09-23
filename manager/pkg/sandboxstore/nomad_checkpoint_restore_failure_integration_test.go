package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadCheckpointRestoreFailureReusesMigrationCleanupAndRetainsImageIntegration(t *testing.T) {
	for _, fork := range []bool{false, true} {
		for _, mode := range []string{"issued", "consumed", "deleted", "expired"} {
			t.Run(fmt.Sprintf("fork=%t/%s", fork, mode), func(t *testing.T) {
				f, candidate, stage, issue, start := checkpointExecutionStoreFixture(t, "restore-failure-"+mode, fork)
				id := candidate.OperationID
				restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, start.SlotID, stage)
				require.NoError(t, err)
				if mode != "issued" {
					consumeCheckpointRestoreWriter(t, f, stage, issue)
					start.MigrationRestoreDigest, err = restore.Digest()
					require.NoError(t, err)
					_, err = f.store.StartRuntimeSlot(f.ctx, start)
					require.NoError(t, err)
				}
				request, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, id)
				require.NoError(t, err)
				require.Nil(t, request, "healthy restore must remain retryable")
				known, receipt, err := f.store.GetNomadMigrationFailureForSlot(f.ctx, start.SlotID)
				require.NoError(t, err)
				require.True(t, known, "generic cleanup cannot race failure authorization")
				require.Nil(t, receipt)
				switch mode {
				case "deleted":
					cleanup, err := f.store.RequestSandboxRuntimeClaimCleanup(f.ctx, f.sandboxID, "delete restoring memory sandbox")
					require.NoError(t, err)
					require.Nil(t, cleanup)
				case "expired":
					_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
				default:
					// Advance claim history only in this isolated database. Production
					// deadlines remain immutable, including while cleanup is pending.
					tx, err := f.pool.Begin(f.ctx)
					require.NoError(t, err)
					defer tx.Rollback(f.ctx)
					_, err = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots DISABLE TRIGGER runtime_checkpoint_claim_budget_guard`)
					require.NoError(t, err)
					_, err = tx.Exec(f.ctx, `UPDATE manager.runtime_slots SET claimed_at=claimed_at-INTERVAL '10 minutes',
					claim_lease_expires_at=claim_lease_expires_at-INTERVAL '10 minutes' WHERE slot_id=$1`, start.SlotID)
					require.NoError(t, err)
					_, err = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots ENABLE TRIGGER runtime_checkpoint_claim_budget_guard`)
					require.NoError(t, err)
					require.NoError(t, tx.Commit(f.ctx))
				}
				require.NoError(t, err)
				worker, err := nomadmigration.NewFailure(f.store)
				require.NoError(t, err)
				_, err = worker.RunOnce(f.ctx)
				require.NoError(t, err)
				request, err = f.store.GetNomadMigrationFailure(f.ctx, id)
				require.NoError(t, err)
				require.NotNil(t, request)
				require.Equal(t, *restore, request.Restore)
				_, err = f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, start.SlotID, stage)
				require.Error(t, err, "fenced execution cannot be dispatched again")
				resumes, err := f.store.ListNomadCheckpointResumes(f.ctx, "", 32)
				require.NoError(t, err)
				require.Empty(t, resumes)
				_, err = f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, id, "bypass physical cleanup")
				require.Error(t, err)
				stopCalls := 0
				stopNode := migrationFailureStopNode(func(_ context.Context, r protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error) {
					require.Equal(t, *request, r)
					stopCalls++
					if stopCalls == 1 {
						return nil, errors.New("lost stop response")
					}
					d, err := r.Digest()
					require.NoError(t, err)
					return &protocol.MigrationFailureStopProof{RequestDigest: d, ContainerID: protocol.NomadRunscContainerID(start.SlotID), ContainerAbsent: true}, nil
				})
				stop, err := nomadmigration.NewFailureStop(f.store, stopNode)
				require.NoError(t, err)
				_, err = stop.RunOnce(f.ctx)
				require.ErrorContains(t, err, "lost stop response")
				stop, err = nomadmigration.NewFailureStop(NewPGSandboxStore(f.pool), stopNode)
				require.NoError(t, err)
				_, err = stop.RunOnce(f.ctx)
				require.NoError(t, err)
				require.Equal(t, 2, stopCalls)
				cleanupNode := migrationFailureCleanupNode(func(_ context.Context, r protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error) {
					require.Equal(t, *request, r.Failure.Request)
					p := migrationFailureCleanupStoreProof(t, r)
					return &p, nil
				})
				cleanupWorker, err := nomadmigration.NewFailureCleanup(f.store, cleanupNode)
				require.NoError(t, err)
				_, err = cleanupWorker.RunOnce(f.ctx)
				require.NoError(t, err)
				finalNode := migrationFailureFinalizeNode(func(_ context.Context, r protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error) {
					d, err := r.Digest()
					require.NoError(t, err)
					return &protocol.MigrationFailureFinalizeProof{RequestDigest: d, RootFSArtifactsAbsent: true, ImageAbsent: true}, nil
				})
				finalWorker, err := nomadmigration.NewFailureFinalization(f.store, finalNode)
				require.NoError(t, err)
				_, err = finalWorker.RunOnce(f.ctx)
				require.NoError(t, err)
				known, receipt, err = f.store.GetNomadMigrationFailureForSlot(f.ctx, start.SlotID)
				require.NoError(t, err)
				require.True(t, known)
				require.NotNil(t, receipt)
				require.NoError(t, receipt.Validate())
				absent := bytes.Repeat([]byte{0x47}, 32)
				gc := protocol.MigrationSourceGCRequest{Target: restore.Image.Target, FinalizationDigest: receipt.Proof.RequestDigest, CleanupProofDigest: receipt.Request.Proof.Cleanup.ProofDigest, AllocationAbsenceDigest: hex.EncodeToString(absent)}
				gd, err := gc.Digest()
				require.NoError(t, err)
				ack := protocol.MigrationSourceGCAcknowledgement{RequestDigest: gd}
				_, err = f.store.CompleteNomadSandboxMigrationFailure(f.ctx, id, gc, ack)
				require.Error(t, err, "allocation absence is separate from node cleanup")
				slot, err := f.store.GetRuntimeSlot(f.ctx, start.SlotID)
				require.NoError(t, err)
				require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState)
				_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ObservationDigest: absent})
				require.NoError(t, err)
				// The final deferred invariant fails after every release/rebase was
				// attempted. Capacity, custody and owner generation must roll back together.
				_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_restore_failure_test() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'injected restore completion failure'; END; $$;
			CREATE CONSTRAINT TRIGGER reject_restore_failure_test AFTER UPDATE OF phase ON manager.sandbox_lifecycle_txns
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.phase='aborted') EXECUTE FUNCTION manager.reject_restore_failure_test();`)
				require.NoError(t, err)
				_, err = f.store.CompleteNomadSandboxMigrationFailure(f.ctx, id, gc, ack)
				require.ErrorContains(t, err, "injected restore completion failure")
				stillHeld, err := f.store.GetRuntimeSlot(f.ctx, start.SlotID)
				require.NoError(t, err)
				require.Equal(t, RuntimeResourceLeaseActive, stillHeld.ResourceLeaseState)
				require.Equal(t, RuntimeSlotStateOrphaned, stillHeld.State)
				var beforeOwner, beforeRef int64
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT s.runtime_generation,r.runtime_generation FROM manager.sandboxes s
			JOIN manager.sandbox_runtime_checkpoint_refs r ON r.sandbox_id=s.sandbox_id WHERE s.sandbox_id=$1`, f.sandboxID).Scan(&beforeOwner, &beforeRef))
				require.Equal(t, candidate.RuntimeGeneration-1, beforeOwner)
				require.Equal(t, beforeOwner, beforeRef)
				_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_restore_failure_test ON manager.sandbox_lifecycle_txns; DROP FUNCTION manager.reject_restore_failure_test();`)
				require.NoError(t, err)
				terminal, err := f.store.CompleteNomadSandboxMigrationFailure(f.ctx, id, gc, ack)
				require.NoError(t, err)
				require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
				require.Equal(t, "migration_failed", terminal.TerminalReason)
				owner, err := f.store.GetSandbox(f.ctx, f.sandboxID)
				require.NoError(t, err)
				require.Empty(t, owner.RuntimeID)
				require.Equal(t, candidate.RuntimeGeneration, owner.RuntimeGeneration, "failed attempted generation is consumed")
				failed, err := f.store.NomadCheckpointFailed(f.ctx, owner.ID, owner.RuntimeGeneration, owner.LifecycleEpoch)
				require.NoError(t, err)
				require.True(t, failed, "public waiters must observe failure after physical resolution")
				var retained string
				var generation int64
				require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT checkpoint_id,runtime_generation FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, owner.ID).Scan(&retained, &generation))
				require.Equal(t, candidate.Checkpoint.Retained.CheckpointID, retained)
				require.Equal(t, owner.RuntimeGeneration, generation)
				life, err := f.store.GetLifecycleTxn(f.ctx, id)
				require.NoError(t, err)
				require.Equal(t, SandboxLifecyclePhaseAborted, life.Phase)
				if mode == "issued" || mode == "consumed" {
					next, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: owner.ID, ExpectedTeamID: owner.TeamID, Memory: true})
					require.NoError(t, err)
					require.NotEqual(t, id, next.OperationID)
					require.Equal(t, owner.RuntimeGeneration+1, next.RuntimeGeneration)
					require.Equal(t, retained, next.Checkpoint.Retained.CheckpointID)
					require.Equal(t, owner.RuntimeGeneration, next.Checkpoint.Assignment.PreviousGeneration())
					require.Equal(t, candidate.Checkpoint.Assignment.Capture, next.Checkpoint.Assignment.Capture)
					retry, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: owner.ID, ExpectedTeamID: owner.TeamID, Memory: true})
					require.NoError(t, err)
					require.Equal(t, next.Checkpoint, retry.Checkpoint)
					failed, err := f.store.NomadCheckpointFailed(f.ctx, owner.ID, owner.RuntimeGeneration, next.Checkpoint.LifecycleEpoch)
					require.NoError(t, err)
					require.False(t, failed, "old failure must not hide the new explicit attempt")
					completeCheckpointFailureRetry(t, f, next)
				} else {
					require.Equal(t, SandboxDesiredStateTerminating, owner.DesiredState)
				}
				require.NoError(t, f.store.CommitNomadSandboxMigrationFailureCleanup(f.ctx, receipt.Request.Request, receipt.Request.Proof))
				require.NoError(t, f.store.CommitNomadSandboxMigrationFailureFinalization(f.ctx, receipt.Request, receipt.Proof))
				_, err = NewPGSandboxStore(f.pool).CompleteNomadSandboxMigrationFailure(f.ctx, id, gc, ack)
				require.NoError(t, err, "historical retry cannot alter a new resume")
			})
		}
	}
}

func completeCheckpointFailureRetry(t *testing.T, f *nomadPauseStoreFixture, candidate *NomadSandboxResumeCandidate) {
	t.Helper()
	target := acquireCheckpointRestoreTarget(t, f, candidate, "explicit-retry")
	stage, issue, start := checkpointExecutionForTarget(t, f, candidate, target)
	restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, target.ID, stage)
	require.NoError(t, err)
	consumeCheckpointRestoreWriter(t, f, stage, issue)
	start.MigrationRestoreDigest, err = restore.Digest()
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.NoError(t, err)
	handover, err := f.store.AuthorizeNomadCheckpointHandover(f.ctx, protocol.MigrationRestoreObservation{
		Request: *restore, RequestDigest: start.MigrationRestoreDigest, State: protocol.MigrationRestoreComplete})
	require.NoError(t, err)
	digest, err := handover.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointHandover(f.ctx, *handover, procdapi.RuntimeCheckpointResponse{
		InstanceID: handover.InstanceID, RequestDigest: digest, RuntimeGeneration: candidate.RuntimeGeneration, State: "ready"}))
	address, err := protocol.NomadProcdAddress(stage.ExpectedPolicyToken.SourceIP)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: candidate.OperationID, ClaimID: target.ClaimID, MigrationRestoreDigest: start.MigrationRestoreDigest,
		ProcdInstanceID: handover.InstanceID, ProcdAddress: address, CommandReadyDigest: bytes.Repeat([]byte{0x98}, 32)})
	require.NoError(t, err)
	owner, err := f.store.CompleteNomadSandboxResume(f.ctx, &CompleteNomadSandboxResumeRequest{
		SandboxID: candidate.SandboxID, OperationID: candidate.OperationID, SlotID: target.ID, AllocationID: target.AllocationID,
		AllocationNamespace: target.AllocationNamespace, ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest})
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, owner.DesiredState)
	require.Equal(t, candidate.RuntimeGeneration, owner.RuntimeGeneration)
	var refs int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, owner.ID).Scan(&refs))
	require.Zero(t, refs, "successful retry releases this owner's paused image reference")
}
