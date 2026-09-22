package sandboxstore

import (
	"bytes"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationDeletedStoragePreservesHistoryIntegration(t *testing.T) {
	for _, staged := range []bool{false, true} {
		name := "unprepared"
		if staged {
			name = "staged"
		}
		t.Run(name, func(t *testing.T) {
			f, assignment := migrationStoreFixture(t, "storage-"+name)
			target := migrationReadyTarget(t, f, "storage-"+name, "b")
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
			require.NoError(t, err)
			if staged {
				retainMigrationEligibilityFixture(t, f, assignment)
			}
			assertBlocked := func() {
				t.Helper()
				before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
				require.NoError(t, err)
				require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
				after, err := f.store.GetSandbox(f.ctx, f.sandboxID)
				require.NoError(t, err)
				require.Equal(t, before, after, "blocked deletion must not change routing or desired state")
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations
                    SET source_writer_grant_ref=NULL,storage_released_at=clock_timestamp() WHERE operation_id=$1`, assignment.OperationID)
				require.Error(t, err, "database guard must reject bypassing physical cleanup")
			}
			assertBlocked()
			require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, assignment.OperationID, "test cancellation"))
			assertBlocked() // Releasing unused capacity does not release either carrier.
			pause, err := f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceAuto)
			require.NoError(t, err)
			f.publishPlannedPause(t, pause.OperationID)
			source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
			require.NoError(t, err)
			_, err = f.store.FinalizeRuntimeSlot(f.ctx, &FinalizeRuntimeSlotRequest{
				SlotID: source.ID, OperationID: source.ClaimOperationID, ClaimID: source.ClaimID,
				Reason: "planned_retire", ProofDigest: bytes.Repeat([]byte{0xa1}, 32),
				ResourceLeaseID: source.ResourceLease.LeaseID, ResourceLeaseDigest: source.ResourceLeaseDigest,
				ResourceCgroupAbsent: true,
			})
			require.NoError(t, err)
			assertBlocked() // Source cleanup cannot substitute for target absence.
			_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{
				SlotID: target.SlotID, AllocationID: target.AllocationID, NodeUID: target.NodeUID,
				NodeBootID: target.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0xa2}, 32),
			})
			require.NoError(t, err)
			if staged {
				for range 2 {
					assertBlocked()
					request, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, assignment.OperationID)
					require.NoError(t, err)
					require.NotNil(t, request)
					digest, err := request.Digest()
					require.NoError(t, err)
					require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *request,
						protocol.MigrationStagingReleased{RequestDigest: digest}))
				}
			}
			var historyBefore []byte
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT to_jsonb(m)-'source_writer_grant_ref'-'storage_released_at'
                FROM manager.sandbox_runtime_migrations m WHERE operation_id=$1`, assignment.OperationID).Scan(&historyBefore))
			_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_storage_delete() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.desired_state='deleted' THEN RAISE EXCEPTION 'injected deletion failure'; END IF; RETURN NEW; END; $$;
                CREATE TRIGGER reject_test_storage_delete BEFORE UPDATE ON manager.sandboxes
                FOR EACH ROW EXECUTE FUNCTION manager.reject_test_storage_delete()`)
			require.NoError(t, err)
			require.ErrorContains(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), "injected deletion failure")
			var retained bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT source_writer_grant_ref=source_writer_grant_id AND storage_released_at IS NULL
                FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&retained))
			require.True(t, retained, "later deletion failure must roll back storage release")
			_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_storage_delete ON manager.sandboxes; DROP FUNCTION manager.reject_test_storage_delete()`)
			require.NoError(t, err)
			require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()))
			var historyAfter []byte
			var detached bool
			var released time.Time
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT to_jsonb(m)-'source_writer_grant_ref'-'storage_released_at',
                source_writer_grant_ref IS NULL,storage_released_at FROM manager.sandbox_runtime_migrations m
                WHERE operation_id=$1`, assignment.OperationID).Scan(&historyAfter, &detached, &released))
			require.JSONEq(t, string(historyBefore), string(historyAfter))
			require.True(t, detached)
			var remaining int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, f.filesystem.ID).Scan(&remaining))
			require.Zero(t, remaining, "terminal migration history must not retain deleted RootFS storage")
			proof, err := f.store.GetRootFSWriterTerminalProof(f.ctx, f.issue.GrantID)
			require.NoError(t, err)
			require.Equal(t, f.issue.GrantID, proof.GrantID)
			require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()))
			var retryReleased time.Time
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT storage_released_at FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&retryReleased))
			require.Equal(t, released, retryReleased)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET storage_released_at=clock_timestamp() WHERE operation_id=$1`, assignment.OperationID)
			require.Error(t, err, "storage release receipt is immutable")
			_, err = f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
			require.Error(t, err, "historical identity cannot authorize another execution")
		})
	}
}

func TestNomadMigrationCapturedDeletionRetainsStorageIntegration(t *testing.T) {
	f, assignment := migrationStoreFixture(t, "storage-captured")
	migrationReadyTarget(t, f, "storage-captured", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, assignment)
	prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
	require.NoError(t, err)
	// Even an erroneous historical phase change does not establish that an
	// authorized capture or target execution has been physically fenced.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',aborted_at=NOW() WHERE txn_id=$1`, assignment.OperationID)
	require.NoError(t, err)
	require.ErrorIs(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now()), ErrSandboxClaimCleanupPending)
	var retained bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT source_writer_grant_ref=source_writer_grant_id AND storage_released_at IS NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&retained))
	require.True(t, retained)
}
