package sandboxstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Retired import tools can leave foreign keys outside the runtime schema.
// They must preserve their objects without aborting an entire GC transaction.
func TestRootFSExternalObjectCustodyDoesNotAbortGCIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	ctx := t.Context()
	_, err := pool.Exec(ctx, `CREATE SCHEMA retained_import;
 CREATE TABLE retained_import.objects(object_key text PRIMARY KEY REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT);
 INSERT INTO manager.rootfs_materialization_objects(object_key,object_kind,object_size,checksum,uploaded_at)
 VALUES ('rootfs/external-held','data_pack',1,'sha256:'||repeat('1',64),NOW()),('rootfs/free','data_pack',1,'sha256:'||repeat('1',64),NOW());
 INSERT INTO retained_import.objects VALUES ('rootfs/external-held');`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, cleanupErr := pool.Exec(context.Background(), "DROP SCHEMA retained_import CASCADE")
		require.NoError(t, cleanupErr)
	})
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	released, err := releaseUnreferencedRootFSMaterializationObject(ctx, tx, "rootfs/external-held", "team-1")
	require.NoError(t, err)
	require.False(t, released)
	released, err = releaseUnreferencedRootFSMaterializationObject(ctx, tx, "rootfs/free", "team-1")
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, tx.Commit(ctx))
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM manager.rootfs_object_deletions WHERE object_key='rootfs/external-held'`).Scan(&count))
	require.Zero(t, count)
	_, err = pool.Exec(ctx, `DELETE FROM retained_import.objects`)
	require.NoError(t, err)
	tx, err = pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	released, err = releaseUnreferencedRootFSMaterializationObject(ctx, tx, "rootfs/external-held", "team-1")
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, tx.Commit(ctx))
}
