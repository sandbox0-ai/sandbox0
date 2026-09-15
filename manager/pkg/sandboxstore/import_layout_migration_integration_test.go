package sandboxstore

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestRootFSImportLayoutMigrationRollbackRequiresNoPolicyRowsIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPoolAt(t, 55)
	// The layout rollback contract is migration 54, not whichever migration
	// happens to be latest. Remove the empty mapping extension first.
	require.NoError(t, migrate.Down(ctx, pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{})))
	down := func() error {
		return migrate.Down(ctx, pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{}))
	}
	require.NoError(t, down())
	var columns int
	query := `SELECT count(*) FROM information_schema.columns WHERE table_schema='manager' AND column_name='data_layout_policy'`
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Zero(t, columns)
	applySandboxStoreMigrationsThrough(t, pool, 55)
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Equal(t, 2, columns)
	store := NewPGSandboxStore(pool)
	begin, _, _ := layoutImportFixture(t, "layout-rollback", rootfsimporter.XFSFileRangesV1)
	_, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	// Keep the layout-bearing operation while removing only migration 55.
	require.NoError(t, down())
	require.ErrorContains(t, down(), "Cannot remove RootFS layout provenance")
	applySandboxStoreMigrationsThrough(t, pool, 55)
	reloaded, err := store.GetRootFSImportOperation(ctx, begin.OperationID)
	require.NoError(t, err)
	require.Equal(t, begin.Spec, reloaded.Spec)
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Equal(t, 2, columns)
}

func TestRootFSImportLayoutMigrationRollbackWaitsForConcurrentPublisherIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	pool := newSandboxStoreIntegrationPoolAt(t, 55)
	// Give the migrator its own manager-scoped connection while the publisher
	// and lock observer use the fixture pool concurrently.
	config := pool.Config()
	config.MaxConns = 1
	config.ConnConfig.RuntimeParams["search_path"] = sandboxStoreSchemaName
	migrationPool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(migrationPool.Close)
	down := func() error {
		return migrate.Down(ctx, migrationPool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{}))
	}
	// Remove the empty mapping extension so the real migrator rolls back 54.
	require.NoError(t, down())
	publisher, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = publisher.Rollback(cleanup)
	}()
	_, err = publisher.Exec(ctx, "LOCK TABLE manager.rootfs_import_operations IN ROW EXCLUSIVE MODE")
	require.NoError(t, err)

	rollback := make(chan error, 1)
	go func() { rollback <- down() }()
	// Wait for the actual DDL lock request, not a timing guess. Without the
	// upfront lock, the empty-row guard has already passed at this boundary.
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE relation='manager.rootfs_import_operations'::regclass
					AND mode='AccessExclusiveLock' AND NOT granted
					AND $1::integer = ANY(pg_blocking_pids(pid))
			)
		`, int32(publisher.Conn().PgConn().PID())).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "rollback must wait for the in-flight publisher")

	begin, _, _ := layoutImportFixture(t, "layout-rollback-concurrent", rootfsimporter.XFSFileRangesV1)
	spec := begin.Spec
	source, err := rootfsimporter.PinnedSourceDigest(spec.SourceOCIRef)
	require.NoError(t, err)
	// Use the schema-54 publisher contract: the current store also writes the
	// mapping_group_policy column that was removed with migration 55 above.
	_, err = publisher.Exec(ctx, `INSERT INTO manager.rootfs_import_operations (
		operation_id, source_oci_ref, source_oci_digest,
		oci_os, oci_architecture, oci_variant, format_generation,
		procd_protocol, procd_digest, logical_size_bytes,
		block_data_range_bytes, block_pack_bytes, block_page_entries,
		object_prefix, data_layout_policy, state
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'pending')`,
		begin.OperationID, spec.SourceOCIRef, source.String(),
		spec.Platform.OS, spec.Platform.Architecture, spec.Platform.Variant,
		spec.FormatGeneration, spec.ProcdProtocol, spec.ProcdDigest, spec.LogicalSizeBytes,
		spec.BlockOptions.DataRangeBytes, spec.BlockOptions.PackBytes, spec.BlockOptions.PageEntries,
		spec.BlockOptions.ObjectPrefix, spec.DataLayoutPolicy)
	require.NoError(t, err)
	require.NoError(t, publisher.Commit(ctx))
	select {
	case err := <-rollback:
		require.ErrorContains(t, err, "Cannot remove RootFS layout provenance")
	case <-ctx.Done():
		t.Fatal("rollback did not finish after the publisher committed")
	}
	var columns int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema='manager' AND column_name='data_layout_policy'`).Scan(&columns))
	require.Equal(t, 2, columns)
	var policy string
	require.NoError(t, pool.QueryRow(ctx, `SELECT data_layout_policy FROM manager.rootfs_import_operations
		WHERE operation_id=$1`, begin.OperationID).Scan(&policy))
	require.Equal(t, spec.DataLayoutPolicy, policy, "failed rollback must preserve committed provenance")
}
