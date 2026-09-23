package sandboxstore

import (
	"fmt"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCaptureUploadSharesRegionalMigrationBudgetIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	f := newNomadPauseStoreFixtureOnNode(t, "checkpoint-capture-budget", pool, "checkpoint-capture-node", func(r *AcquireRuntimeSlotRequest) {
		r.Resources.MemoryBytes = 8 << 30
	})
	a := migrationAssignmentFixture(t, f, "checkpoint-capture-budget")
	source := a.Target
	source.RuntimeGeneration = a.SourceGeneration
	id := "checkpoint-capture-budget-pause"
	c, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, id, source, migrationSourcePolicy(source.SandboxID, source.TeamID))
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointCPU(f.ctx, c.Evidence.Preflight, migrationCPUStoreResult(t, f, c.Evidence.Preflight)))
	staging, err := f.store.AuthorizeNomadCheckpointStaging(f.ctx, id)
	require.NoError(t, err)
	require.Equal(t, 1, staging.CaptureUpload.Version)

	var used int64
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')
		FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, id).Scan(&used))
	require.Equal(t, staging.CaptureUpload.ReservedBytes(), used)
	for i := range 3 {
		name := fmt.Sprintf("checkpoint-capture-migration-%d", i)
		mf := newNomadPauseStoreFixtureOnNode(t, name, pool, name, func(r *AcquireRuntimeSlotRequest) {
			r.Resources.MemoryBytes = 8 << 30
		})
		assignment := migrationAssignmentFixture(t, mf, name)
		migrationReadyTarget(t, mf, name, name+"-target")
		_, err := mf.store.ReserveNomadSandboxMigration(mf.ctx, assignment)
		require.NoError(t, err)
		retainMigrationCPUFixture(t, mf, assignment)
		request, err := mf.store.AuthorizeNomadSandboxMigrationStaging(mf.ctx, assignment)
		require.NoError(t, err)
		if i < 2 {
			require.Equal(t, 1, request.CaptureUpload.Version)
			used += request.CaptureUpload.ReservedBytes()
		} else {
			require.Zero(t, request.CaptureUpload.Version, "migration remains available without tentative upload")
		}
	}
	require.LessOrEqual(t, used, nomadMigrationCaptureUploadRegionBytes)
	var actual int64
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT
		(SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(staging_request)),0)
		 FROM manager.sandbox_runtime_migrations WHERE capture_upload_gc_completed_at IS NULL)
		+
		(SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')),0)
		 FROM manager.sandbox_runtime_checkpoints WHERE capture_upload_gc_completed_at IS NULL)`).Scan(&actual))
	require.Equal(t, used, actual)

	blocked, err := f.store.AuthorizeNomadCheckpointCaptureUploadGC(f.ctx, id)
	require.NoError(t, err)
	require.Nil(t, blocked)
	_, err = pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints
		SET capture_upload_gc_scope_digest=evidence->'staging'->'capture_upload'->>'scope_digest' WHERE operation_id=$1`, id)
	require.Error(t, err, "SQL rejects reclaiming a live source grant")
}

func TestConcurrentCheckpointCaptureUploadReservationsRemainBoundedAndRecoverAfterCancellationIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	const count = 4
	fixtures := make([]*nomadPauseStoreFixture, count)
	ids := make([]string, count)
	for i := range fixtures {
		name := fmt.Sprintf("checkpoint-concurrent-budget-%d", i)
		f := newNomadPauseStoreFixtureOnNode(t, name, pool, name, func(r *AcquireRuntimeSlotRequest) {
			r.Resources.MemoryBytes = 8 << 30
		})
		fixtures[i], ids[i] = f, name+"-pause"
		a := migrationAssignmentFixture(t, f, name)
		source := a.Target
		source.RuntimeGeneration = a.SourceGeneration
		checkpoint, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, ids[i], source,
			migrationSourcePolicy(source.SandboxID, source.TeamID))
		require.NoError(t, err)
		require.NoError(t, f.store.CommitNomadCheckpointCPU(f.ctx, checkpoint.Evidence.Preflight,
			migrationCPUStoreResult(t, f, checkpoint.Evidence.Preflight)))
	}

	start := make(chan struct{})
	var group sync.WaitGroup
	requests := make([]*protocol.MigrationStagingRequest, count)
	errs := make([]error, count)
	for i := range fixtures {
		group.Go(func() {
			<-start
			requests[i], errs[i] = NewPGSandboxStore(pool).AuthorizeNomadCheckpointStaging(fixtures[i].ctx, ids[i])
		})
	}
	close(start)
	group.Wait()
	granted, waiting := -1, -1
	var used int64
	for i := range fixtures {
		if errs[i] != nil {
			require.ErrorIs(t, errs[i], ErrNomadCheckpointConflict)
			require.Nil(t, requests[i])
			require.Equal(t, -1, waiting)
			waiting = i
			continue
		}
		require.Equal(t, 1, requests[i].CaptureUpload.Version)
		used += requests[i].CaptureUpload.ReservedBytes()
		granted = i
	}
	require.NotEqual(t, -1, waiting)
	require.NotEqual(t, -1, granted)
	require.LessOrEqual(t, used, nomadMigrationCaptureUploadRegionBytes)
	var actual int64
	require.NoError(t, pool.QueryRow(fixtures[0].ctx, `SELECT COALESCE(SUM(manager.runtime_migration_capture_upload_reserved_bytes(evidence->'staging')),0)
		FROM manager.sandbox_runtime_checkpoints
		WHERE capture_upload_gc_completed_at IS NULL AND capture_upload_reservation_released_at IS NULL`).Scan(&actual))
	require.Equal(t, used, actual)

	f, id, request := fixtures[granted], ids[granted], requests[granted]
	expirePreparedMigration(t, f, id)
	cancel, err := f.store.AuthorizeNomadCheckpointCancellation(f.ctx, id)
	require.NoError(t, err)
	require.NotNil(t, cancel)
	require.Nil(t, cancel.Preparation)
	digest, err := request.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointCanceledStaging(f.ctx, *request,
		protocol.MigrationStagingReleased{RequestDigest: digest}))
	require.NoError(t, f.store.CompleteNomadCheckpointCancellation(f.ctx, id))
	scope, err := request.CaptureUpload.Scope(request.Source)
	require.NoError(t, err)
	collector, err := runtimecheckpoint.NewCollector(objectstore.NewMemoryStore(""))
	require.NoError(t, err)
	gc, err := f.store.AuthorizeNomadCheckpointCaptureUploadGC(f.ctx, id)
	require.NoError(t, err)
	require.NotNil(t, gc)
	done, err := collector.CollectCapture(f.ctx, *gc)
	require.NoError(t, err)
	require.True(t, done)
	require.NoError(t, f.store.CompleteNomadCheckpointCaptureUploadGC(f.ctx, scope))
	retry, err := fixtures[waiting].store.AuthorizeNomadCheckpointStaging(fixtures[waiting].ctx, ids[waiting])
	require.NoError(t, err)
	require.Equal(t, 1, retry.CaptureUpload.Version)
}
