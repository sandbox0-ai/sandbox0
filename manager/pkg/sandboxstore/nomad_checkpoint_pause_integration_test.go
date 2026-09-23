package sandboxstore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNomadMemoryPauseAdmissionUsesLaunchInputsAndReusesConcurrentIntentIntegration(t *testing.T) {
	f, a := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "memory-admission", "")
	const count = 8
	results := make([]*NomadSandboxMemoryPauseCandidate, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() {
			results[i], errs[i] = NewPGSandboxStore(f.pool).RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
		})
	}
	wg.Wait()
	for i := range count {
		require.NoError(t, errs[i])
		require.Equal(t, results[0], results[i])
		require.False(t, results[i].AlreadyPaused)
	}
	work, err := f.store.GetNomadCheckpointPauseWork(f.ctx, results[0].OperationID)
	require.NoError(t, err)
	require.Equal(t, a.SourceRevision, work.Preflight.Source.AssignmentRevision)
	preparation, err := f.store.AuthorizeNomadCheckpointPreparation(f.ctx, results[0].OperationID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "admission cannot bypass CPU and staging checks")
	require.Nil(t, preparation)
	_, err = f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
	require.ErrorIs(t, err, ErrNomadSandboxPauseConflict)
}

func TestNomadMemoryPauseAdmissionDoesNotReconstructMissingLaunchInputsIntegration(t *testing.T) {
	f, _, _, _ := checkpointStoreFixture(t, "admission-legacy")
	_, err := f.store.RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	lifecycle, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, lifecycle)
	var checkpoints int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoints`).Scan(&checkpoints))
	require.Zero(t, checkpoints)
}

func TestNomadMemoryPauseAdmissionRequiresRetainedMemoryForPausedIdempotencyIntegration(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		f := completedCheckpointStoreFixture(t, "admission-paused")
		candidate, err := f.store.RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
		require.NoError(t, err)
		require.True(t, candidate.AlreadyPaused)
		require.NotEmpty(t, candidate.OperationID)
	})
	t.Run("filesystem", func(t *testing.T) {
		f := newNomadPauseStoreFixture(t, "admission-disk-only")
		terminalizeNomadPauseFixture(t, f)
		_, err := f.store.RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
		require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	})
	t.Run("pending-cleanup", func(t *testing.T) {
		f, cleanup := checkpointFinalizationStoreFixture(t, "admission-pending")
		candidate, err := f.store.RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
		require.NoError(t, err)
		require.False(t, candidate.AlreadyPaused)
		require.Equal(t, cleanup.Checkpoint.CheckpointID, candidate.OperationID)
	})
	t.Run("filesystem-pause-in-progress", func(t *testing.T) {
		f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "admission-cold-pending", "")
		_, err := f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
		require.NoError(t, err)
		_, err = f.store.RequestNomadSandboxMemoryPause(f.ctx, f.sandboxID)
		require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	})
}
