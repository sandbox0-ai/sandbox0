package sandboxstore

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/stretchr/testify/require"
)

func TestNomadMeteringMigrationDownAndUpIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	record := rootFSTestSandboxRecord("sandbox-metering-migration", "team-metering")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	record.ClaimedAt = time.Now().UTC()
	require.NoError(t, NewPGSandboxStore(pool).UpsertSandbox(ctx, record))

	_, err := pool.Exec(ctx, `
		UPDATE manager.sandboxes SET resource_millicpu = -1 WHERE sandbox_id = $1
	`, record.ID)
	require.Error(t, err)

	require.NoError(t, migrate.Down(ctx, pool, ".",
		migrate.WithBaseFS(storemigrations.FS),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}),
		migrate.WithSchema(sandboxStoreSchemaName),
	))
	assertSandboxStoreColumnExists(t, ctx, pool, "resource_millicpu", false)
	assertSandboxStoreColumnExists(t, ctx, pool, "resource_memory_mib", false)
	var queueExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT to_regclass('manager.sandbox_metering_projection_queue') IS NOT NULL
	`).Scan(&queueExists))
	require.False(t, queueExists)

	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	assertSandboxStoreColumnExists(t, ctx, pool, "resource_millicpu", true)
	assertSandboxStoreColumnExists(t, ctx, pool, "resource_memory_mib", true)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT to_regclass('manager.sandbox_metering_projection_queue') IS NOT NULL
	`).Scan(&queueExists))
	require.True(t, queueExists)
}

func TestNomadMeteringResourceBackfillSerializesHACompletionIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-metering-ha", "team-metering")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	record.ClaimedAt = time.Now().UTC()
	require.NoError(t, store.UpsertSandbox(ctx, record))

	resolverEntered := make(chan struct{})
	releaseResolver := make(chan struct{})
	firstResult := make(chan struct {
		updated int64
		err     error
	}, 1)
	go func() {
		updated, err := store.BackfillNomadMeteringResources(ctx, func(*SandboxRecord) (int64, int64, error) {
			close(resolverEntered)
			<-releaseResolver
			return 1000, 1024, nil
		})
		firstResult <- struct {
			updated int64
			err     error
		}{updated: updated, err: err}
	}()
	<-resolverEntered

	secondResult := make(chan struct {
		updated int64
		err     error
	}, 1)
	go func() {
		updated, err := store.BackfillNomadMeteringResources(ctx, func(*SandboxRecord) (int64, int64, error) {
			return 2000, 2048, nil
		})
		secondResult <- struct {
			updated int64
			err     error
		}{updated: updated, err: err}
	}()

	select {
	case result := <-secondResult:
		t.Fatalf("second backfill completed before the first commit: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseResolver)

	first := <-firstResult
	require.NoError(t, first.err)
	require.Equal(t, int64(1), first.updated)
	second := <-secondResult
	require.NoError(t, second.err)
	require.Equal(t, int64(0), second.updated)

	loaded, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1000), loaded.ResourceMillicpu)
	require.Equal(t, int64(1024), loaded.ResourceMemoryMiB)
}

func TestNomadMeteringResourceBackfillAndLifecycleQueueIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-metering-backfill", "team-metering")
	record.RuntimeBackend = SandboxRuntimeBackendNomad
	record.ClaimedAt = time.Now().UTC()
	require.NoError(t, store.UpsertSandbox(ctx, record))

	var initialRevision int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT revision
		FROM manager.sandbox_metering_projection_queue
		WHERE sandbox_id = $1
	`, record.ID).Scan(&initialRevision))

	updated, err := store.BackfillNomadMeteringResources(ctx, func(source *SandboxRecord) (int64, int64, error) {
		require.Equal(t, record.ID, source.ID)
		return 1000, 1024, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), updated)
	loaded, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1000), loaded.ResourceMillicpu)
	require.Equal(t, int64(1024), loaded.ResourceMemoryMiB)

	var backfilledRevision int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT revision
		FROM manager.sandbox_metering_projection_queue
		WHERE sandbox_id = $1
	`, record.ID).Scan(&backfilledRevision))
	require.Greater(t, backfilledRevision, initialRevision)

	_, err = pool.Exec(ctx, `
		DELETE FROM manager.sandbox_metering_projection_queue WHERE sandbox_id = $1
	`, record.ID)
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, record.ID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "metering-pause", SandboxID: record.ID,
			Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePreparing,
		})
	}))
	assertNomadMeteringQueueCount(t, ctx, pool, record.ID, 0)

	require.NoError(t, store.WithSandboxLock(ctx, record.ID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		return tx.CommitLifecycleTxn(lockCtx, "metering-pause", "")
	}))
	assertNomadMeteringQueueCount(t, ctx, pool, record.ID, 1)
}

func assertNomadMeteringQueueCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sandboxID string, want int64) {
	t.Helper()
	var count int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.sandbox_metering_projection_queue
		WHERE sandbox_id = $1
	`, sandboxID).Scan(&count))
	require.Equal(t, want, count)
}
