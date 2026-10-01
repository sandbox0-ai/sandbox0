package sandboxstore

import (
	"bytes"
	"sync"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestCheckpointResumeFallbackAdmitsOneColdLifecycleAfterDriftIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "fallback-drift")
	memory, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.NoError(t, err)
	const concurrency = 8
	var wg sync.WaitGroup
	operations := make([]string, concurrency)
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Go(func() {
			var handled bool
			operations[i], handled, errs[i] = NewPGSandboxStore(f.pool).ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, memory.OperationID, "runtime upgraded", nil, true)
			if !handled && errs[i] == nil {
				errs[i] = ErrNomadCheckpointConflict
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.NotEmpty(t, operations[i])
		require.Equal(t, operations[0], operations[i])
	}
	cold, found, err := f.store.RetryNomadSandboxResume(f.ctx, &RetryNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.NoError(t, err)
	require.True(t, found)
	require.Nil(t, cold.Checkpoint)
	require.Equal(t, memory.SourceGenerationID, cold.SourceGenerationID, "fallback preserves the committed RootFS")
	require.NotEqual(t, memory.OperationID, cold.OperationID)
	require.Equal(t, memory.RuntimeGeneration, cold.RuntimeGeneration, "pre-execution drift did not consume a generation")
	work, err := f.store.ListNomadCheckpointResumes(f.ctx, "", 32)
	require.NoError(t, err)
	require.Equal(t, []NomadCheckpointResumeWork{{OperationID: memory.OperationID, SandboxID: f.sandboxID}}, work)
	pending, err := f.store.NomadCheckpointResumeFallbackPending(f.ctx, f.sandboxID, cold.Record.RuntimeGeneration, cold.Record.LifecycleEpoch)
	require.NoError(t, err)
	require.True(t, pending)
	// A transient cold-start failure is also retried durably after cleanup.
	_, err = f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, cold.OperationID, "transient node outage")
	require.NoError(t, err)
	next, handled, err := NewPGSandboxStore(f.pool).ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, memory.OperationID, "", nil, true)
	require.NoError(t, err)
	require.True(t, handled)
	require.NotEqual(t, cold.OperationID, next)
	// A newer independent lifecycle supersedes queued work completely.
	_, err = f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, next, "user starts another operation")
	require.NoError(t, err)
	_, err = f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.NoError(t, err)
	next, handled, err = f.store.ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, memory.OperationID, "", nil, true)
	require.NoError(t, err)
	require.False(t, handled)
	require.Empty(t, next)
}

func TestCheckpointResumeFallbackWaitsForExactImageAndLeaseCleanupIntegration(t *testing.T) {
	f, candidate, target := checkpointRestoreTargetFixture(t, "fallback-image")
	authority := *candidate.Checkpoint
	cpu, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, authority, target.ID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, *cpu, authority, migrationCPUStoreResult(t, f, *cpu)))
	_, err = f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, authority, target.ID)
	require.NoError(t, err)
	next, handled, err := f.store.ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, candidate.OperationID, "image is incompatible", nil, true)
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, next, "no new writer may race a delayed image download")
	cancel, proof, err := f.store.GetNomadCheckpointRestoreCancellation(f.ctx, candidate.OperationID)
	require.NoError(t, err)
	require.NotNil(t, cancel)
	require.Nil(t, proof)
	digest, err := cancel.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCancellation(f.ctx, candidate.OperationID, *cancel, protocol.CheckpointImageCancelProof{RequestDigest: digest, ImageAbsent: true}))
	next, handled, err = NewPGSandboxStore(f.pool).ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, candidate.OperationID, "", nil, true)
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, next, "image absence alone does not release the carrier lease")
	_, err = f.store.FinalizeRuntimeSlot(f.ctx, &FinalizeRuntimeSlotRequest{SlotID: target.ID, OperationID: candidate.OperationID, ClaimID: target.ClaimID,
		Reason: "prelaunch_abort", ProofDigest: bytes.Repeat([]byte{0xc1}, 32), ResourceLeaseID: target.ResourceLease.LeaseID,
		ResourceLeaseDigest: target.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	require.NoError(t, err)
	next, handled, err = NewPGSandboxStore(f.pool).ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, candidate.OperationID, "", nil, true)
	require.NoError(t, err)
	require.True(t, handled)
	require.NotEmpty(t, next)
}

func TestCheckpointResumeFallbackDoesNotAbortAuthorizedExecutionIntegration(t *testing.T) {
	f, candidate, stage, _, start := checkpointExecutionStoreFixture(t, "fallback-executed", false)
	_, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, start.SlotID, stage)
	require.NoError(t, err)
	next, handled, err := f.store.ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, candidate.OperationID, "restore failed", nil, true)
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, next)
	life, err := f.store.GetLifecycleTxn(f.ctx, candidate.OperationID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhasePreparing, life.Phase, "physical failure resolution still owns execution")
	failure, err := f.store.AuthorizeNomadSandboxMigrationFailure(f.ctx, candidate.OperationID)
	require.NoError(t, err)
	require.NotNil(t, failure)
}

func TestCheckpointResumeFallbackRespectsQuotaAndExpirationIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "fallback-quota")
	memory, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.NoError(t, err)
	next, handled, err := f.store.ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, memory.OperationID, "runtime upgraded", nil, false)
	require.ErrorIs(t, err, ErrNomadCheckpointFallbackQuotaRequired)
	require.True(t, handled)
	require.Empty(t, next)
	limit := int64(0)
	_, _, err = f.store.ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, memory.OperationID, "", &limit, true)
	var quota *ActiveSandboxQuotaExceededError
	require.ErrorAs(t, err, &quota)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	next, handled, err = f.store.ResolveNomadCheckpointResumeFallback(f.ctx, f.sandboxID, memory.OperationID, "", nil, true)
	require.NoError(t, err)
	require.False(t, handled)
	require.Empty(t, next)
	work, err := f.store.ListNomadCheckpointResumes(f.ctx, "", 32)
	require.NoError(t, err)
	require.Empty(t, work, "expired identities must not be resurrected")
}
