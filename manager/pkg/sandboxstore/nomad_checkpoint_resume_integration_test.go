package sandboxstore

import (
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

func completedCheckpointStoreFixture(t *testing.T, name string) *nomadPauseStoreFixture {
	t.Helper()
	f, request := checkpointFinalizationStoreFixture(t, name)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFinalization(f.ctx, *request, migrationFinalizationStoreProof(t, *request)))
	markCheckpointAllocationMissing(t, f)
	_, err := f.store.CompleteNomadSandboxMemoryPause(f.ctx, request.Checkpoint.CheckpointID)
	require.NoError(t, err)
	return f
}

func TestNomadCheckpointResumeAdmissionPinsImageAndModeIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "memory-resume-admission")
	request := &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true}
	const workers = 8
	results := make([]*NomadSandboxResumeCandidate, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() { results[i], errs[i] = NewPGSandboxStore(f.pool).RequestNomadSandboxResume(f.ctx, request) })
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, results[0].Checkpoint, results[i].Checkpoint)
	}
	candidate := results[0]
	require.NotNil(t, candidate.Checkpoint)
	require.Equal(t, runtimecontrol.CheckpointResume, candidate.Checkpoint.Assignment.Kind)
	require.Equal(t, int64(2), candidate.RuntimeGeneration)
	require.NotEqual(t, candidate.OperationID, candidate.Checkpoint.Retained.CheckpointID)
	require.Equal(t, candidate.OperationID, candidate.Checkpoint.Assignment.OperationID)
	require.NotEmpty(t, candidate.CheckpointCompatibilityDigest)
	retry, found, err := f.store.RetryNomadSandboxResume(f.ctx, &RetryNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, candidate.Checkpoint, retry.Checkpoint)
	_, _, err = f.store.RetryNomadSandboxResume(f.ctx, &RetryNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "retry must not silently discard captured memory")
	request.Memory = false
	_, err = f.store.RequestNomadSandboxResume(f.ctx, request)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores
		SET authority=jsonb_set(authority,'{lifecycle_epoch}','999') WHERE operation_id=$1`, candidate.OperationID)
	require.Error(t, err, "restore authority is immutable")
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, candidate.OperationID)
	require.Error(t, err, "removing the mode record must not enable a cold fallback")
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID)
	require.Error(t, err, "pending resume must retain its image")
	var leases int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Zero(t, leases, "admission does not revive the source carrier or allocate target capacity")
	aborted, err := f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, candidate.OperationID, "target capacity unavailable")
	require.NoError(t, err)
	require.True(t, aborted)
	request.Memory = true
	next, err := f.store.RequestNomadSandboxResume(f.ctx, request)
	require.NoError(t, err)
	require.NotEqual(t, candidate.OperationID, next.OperationID)
	require.Equal(t, candidate.Checkpoint.Retained, next.Checkpoint.Retained)
	require.Greater(t, next.Checkpoint.LifecycleEpoch, candidate.Checkpoint.LifecycleEpoch)
}

func TestNomadCheckpointResumeMissingImageRollsBackAdmissionIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "memory-resume-no-image")
	terminalizeNomadPauseFixture(t, f)
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.ErrorIs(t, err, ErrNomadCheckpointNotRetained)
	active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active, "unsupported memory resume must not consume an active quota reservation")
	after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before.LifecycleEpoch, after.LifecycleEpoch)
	ordinary, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.NoError(t, err)
	require.Nil(t, ordinary.Checkpoint)
	_, err = f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "an existing filesystem resume cannot change modes")
}

func TestNomadCheckpointResumeFilesystemDefaultKeepsExistingBehaviorIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "memory-resume-default")
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.NoError(t, err)
	require.Nil(t, candidate.Checkpoint, "even a retained memory image does not change the default")
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_restores`).Scan(&count))
	require.Zero(t, count)
}
