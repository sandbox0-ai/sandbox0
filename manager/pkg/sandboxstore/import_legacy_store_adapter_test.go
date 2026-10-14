package sandboxstore

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Historical layout/mapping rollback gates pin migrations 54/55. The current
// store also reads a newer geometry column. Supply only its fixed-size default
// in those fixtures, without installing sparse geometry, attestations or fences.
// A CHECK forbids sparse rows, so these gates cannot substitute for migration
// 120 acceptance. Relevant historical policy columns still roll back normally.
func prepareLegacyRootFSImportStoreAdapter(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `
		ALTER TABLE manager.rootfs_import_operations
			ADD COLUMN initial_filesystem_size_bytes bigint NOT NULL DEFAULT 0 CHECK (initial_filesystem_size_bytes = 0);
		ALTER TABLE manager.rootfs_base_artifacts
			ADD COLUMN initial_filesystem_size_bytes bigint NOT NULL DEFAULT 0 CHECK (initial_filesystem_size_bytes = 0);
	`)
	require.NoError(t, err)
}
