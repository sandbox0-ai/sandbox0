package sandboxstore

import (
	"testing"

	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

func TestRootFSImportLayoutMigrationRollbackRequiresNoPolicyRowsIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
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
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Equal(t, 2, columns)
	store := NewPGSandboxStore(pool)
	begin, _, _ := layoutImportFixture(t, "layout-rollback", rootfsimporter.XFSFileRangesV1)
	_, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	// Keep the layout-bearing operation while removing only migration 55.
	require.NoError(t, down())
	require.ErrorContains(t, down(), "Cannot remove RootFS layout provenance")
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	reloaded, err := store.GetRootFSImportOperation(ctx, begin.OperationID)
	require.NoError(t, err)
	require.Equal(t, begin.Spec, reloaded.Spec)
	require.NoError(t, pool.QueryRow(ctx, query).Scan(&columns))
	require.Equal(t, 2, columns)
}
