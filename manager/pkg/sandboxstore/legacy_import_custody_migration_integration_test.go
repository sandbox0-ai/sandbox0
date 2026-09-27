package sandboxstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveCompletedLegacyImportCustodyMigrationIntegration(t *testing.T) {
	for _, scenario := range []string{"committed", "unfinished-session", "unfinished-build", "active-lease", "unpublished-object"} {
		t.Run(scenario, func(t *testing.T) {
			pool := newSandboxStoreIntegrationPoolAt(t, 113)
			ctx := t.Context()
			_, err := pool.Exec(ctx, `CREATE SCHEMA legacy_ack_migration;
   CREATE TABLE legacy_ack_migration.sessions(session_id text PRIMARY KEY,state text,commit_digest text,committed_at timestamptz);
   CREATE TABLE legacy_ack_migration.builds(build_id text PRIMARY KEY,session_id text REFERENCES legacy_ack_migration.sessions(session_id),state text,lease_owner text,lease_token text,lease_expires_at timestamptz);
   CREATE TABLE legacy_ack_migration.build_objects(build_id text REFERENCES legacy_ack_migration.builds(build_id),object_key text REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,upload_state text,result_object boolean);
   INSERT INTO manager.rootfs_materialization_objects(object_key,object_kind,object_size,checksum,uploaded_at) VALUES ('rootfs/legacy-import','data_pack',1,'sha256:'||repeat('1',64),NOW());
   INSERT INTO legacy_ack_migration.sessions VALUES('session','committed','sha256:'||repeat('2',64),NOW());
   INSERT INTO legacy_ack_migration.builds VALUES('ready','session','ready',NULL,NULL,NULL),('unused-pending','session','pending',NULL,NULL,NULL);
   INSERT INTO legacy_ack_migration.build_objects VALUES('ready','rootfs/legacy-import','published',TRUE);`)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, cleanupErr := pool.Exec(context.Background(), "DROP SCHEMA legacy_ack_migration CASCADE")
				require.NoError(t, cleanupErr)
			})
			mutations := map[string]string{
				"unfinished-session": `UPDATE legacy_ack_migration.sessions SET state='importing',commit_digest=NULL,committed_at=NULL`,
				"unfinished-build":   `UPDATE legacy_ack_migration.builds SET state='building' WHERE build_id='ready'`,
				"active-lease":       `UPDATE legacy_ack_migration.builds SET lease_owner='old-worker',lease_token='token',lease_expires_at=NOW() WHERE build_id='unused-pending'`,
				"unpublished-object": `UPDATE legacy_ack_migration.build_objects SET upload_state='prepared',result_object=FALSE`,
			}
			if sql := mutations[scenario]; sql != "" {
				_, err = pool.Exec(ctx, sql)
				require.NoError(t, err)
			}
			require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
			var retained bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='legacy_ack_migration.build_objects'::regclass AND confrelid='manager.rootfs_materialization_objects'::regclass AND contype='f')`).Scan(&retained))
			require.Equal(t, scenario != "committed", retained)
			var count int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM legacy_ack_migration.build_objects`).Scan(&count))
			require.Equal(t, 1, count, "archive evidence must remain")
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			released, err := releaseUnreferencedRootFSMaterializationObject(ctx, tx, "rootfs/legacy-import", "team-1")
			require.NoError(t, err)
			require.Equal(t, scenario == "committed", released)
			require.NoError(t, tx.Commit(ctx))
		})
	}
}
