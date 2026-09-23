package sandboxstore

import (
	"bytes"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadCheckpointRestoreCancellationRequiresNodeAbsenceBeforeTerminalIntegration(t *testing.T) {
	f, candidate, target := checkpointRestoreTargetFixture(t, "checkpoint-cancel-image")
	a := *candidate.Checkpoint
	cpu, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, *cpu, a, migrationCPUStoreResult(t, f, *cpu)))
	image, err := f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, a, target.ID)
	require.NoError(t, err)

	aborted, err := f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, candidate.OperationID, "cancel pre-execution image")
	require.NoError(t, err)
	require.True(t, aborted)
	request, proof, err := f.store.GetNomadCheckpointRestoreCancellation(f.ctx, candidate.OperationID)
	require.NoError(t, err)
	require.Nil(t, proof)
	require.Equal(t, *image, request.Image)
	known, receipt, err := f.store.GetNomadMigrationFailureForSlot(f.ctx, target.ID)
	require.NoError(t, err)
	require.True(t, known)
	require.Nil(t, receipt)
	err = f.store.CommitNomadCheckpointRestoreImage(f.ctx, *image, protocol.MigrationImagePrepared{})
	require.Error(t, err)
	_, err = f.store.FinalizeRuntimeSlot(f.ctx, &FinalizeRuntimeSlotRequest{SlotID: target.ID, OperationID: candidate.OperationID,
		ClaimID: target.ClaimID, Reason: "prelaunch_abort", ProofDigest: bytes.Repeat([]byte{0xc1}, 32),
		ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	require.Error(t, err, "a lease cannot be released while a delayed image may still arrive")
	digest, err := request.Digest()
	require.NoError(t, err)
	proof = &protocol.CheckpointImageCancelProof{RequestDigest: digest, ImageAbsent: true}
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCancellation(f.ctx, candidate.OperationID, *request, *proof))
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCancellation(f.ctx, candidate.OperationID, *request, *proof))
	known, receipt, err = f.store.GetNomadMigrationFailureForSlot(f.ctx, target.ID)
	require.NoError(t, err)
	require.False(t, known)
	require.Nil(t, receipt)
	terminal, err := f.store.FinalizeRuntimeSlot(f.ctx, &FinalizeRuntimeSlotRequest{SlotID: target.ID, OperationID: candidate.OperationID,
		ClaimID: target.ClaimID, Reason: "prelaunch_abort", ProofDigest: bytes.Repeat([]byte{0xc1}, 32),
		ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	aborted, err = f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, candidate.OperationID, "cancel pre-execution image")
	require.NoError(t, err)
	require.True(t, aborted)
	work, err := f.store.ListNomadCheckpointRestoreCancellations(f.ctx, "", 32)
	require.NoError(t, err)
	require.Empty(t, work)
	var generation int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT runtime_generation FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&generation))
	require.Equal(t, candidate.RuntimeGeneration-1, generation, "no execution authority consumed the generation")
}

func TestNomadCheckpointRestoreCancellationWithoutImageClosesExpiredAttemptIntegration(t *testing.T) {
	f, candidate, target := checkpointRestoreTargetFixture(t, "checkpoint-cancel-no-image")
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	work, err := f.store.ListNomadCheckpointRestoreCancellations(f.ctx, "", 32)
	require.NoError(t, err)
	require.Equal(t, candidate.OperationID, work[0].OperationID)
	aborted, err := f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, candidate.OperationID, "expired before image authorization")
	require.NoError(t, err)
	require.True(t, aborted)
	request, proof, err := f.store.GetNomadCheckpointRestoreCancellation(f.ctx, candidate.OperationID)
	require.NoError(t, err)
	require.Nil(t, request)
	require.Nil(t, proof)
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT phase FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, candidate.OperationID).Scan(&phase))
	require.Equal(t, SandboxLifecyclePhaseAborted, phase)
	quiescing, err := f.store.GetRuntimeSlot(f.ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, quiescing.State)
}
