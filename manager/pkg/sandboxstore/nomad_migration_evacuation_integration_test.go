package sandboxstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/stretchr/testify/require"
)

func fenceMigrationSource(t *testing.T, f *nomadPauseStoreFixture, state string) {
	t.Helper()
	_, err := f.pool.Exec(f.ctx, `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason)
 SELECT cluster_id,node_id,node_uid,$2,'test evacuation' FROM manager.runtime_slots WHERE slot_id=$1
 ON CONFLICT(cluster_id,node_id,node_uid) DO UPDATE SET state=EXCLUDED.state`, f.slotID, state)
	require.NoError(t, err)
}

func TestNomadMigrationEvacuationDiscoversAndExecutesIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "evacuate", "")
	migrationReadyTarget(t, f, "evacuate", "b")
	w, err := nomadmigration.NewEvacuation(f.store)
	require.NoError(t, err)
	for _, state := range []string{"", "warming", "revoked"} {
		if state != "" {
			fenceMigrationSource(t, f, state)
		}
		result, err := w.RunOnce(f.ctx)
		require.NoError(t, err)
		require.Zero(t, result.Candidates)
	}
	fenceMigrationSource(t, f, "draining")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET config='{"envVars":{"MODE":"changed"}}'::jsonb WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	result, err := w.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	life, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.NotNil(t, life)
	require.Equal(t, SandboxLifecycleKindMigrate, life.Kind)
	require.Equal(t, SandboxLifecycleSourceAuto, life.Source)
	require.False(t, life.Cancelable)
	work, err := f.store.GetNomadMigrationCPUPreflightWork(f.ctx, life.ID)
	require.NoError(t, err)
	require.Equal(t, "original", work.Assignment.Target.EnvVars["MODE"])
	// Run the actual regional worker sequence with controlled node evidence.
	// Discovery itself must neither freeze procd nor publish a generation.
	cpu := &cpuWorkNode{t: t, f: f, calls: map[bool]int{}}
	cw, err := nomadmigration.NewCPUPreflight(f.store, cpu)
	require.NoError(t, err)
	for range 2 {
		_, err = cw.RunOnce(f.ctx)
		require.NoError(t, err)
	}
	staging := &stagingWorkNode{t: t, f: f, calls: map[string]int{}}
	sw, err := nomadmigration.NewStaging(f.store, staging)
	require.NoError(t, err)
	for range 2 {
		_, err = sw.RunOnce(f.ctx)
		require.NoError(t, err)
	}
	source := &sourceExecutionNode{t: t, f: f}
	ew, err := nomadmigration.NewSourceExecution(f.store, source, source, source)
	require.NoError(t, err)
	for range 3 {
		_, err = ew.RunOnce(f.ctx)
		require.NoError(t, err)
	}
	require.Equal(t, 1, source.captureCalls)
	record, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, int64(1), record.RuntimeGeneration)
	result, err = w.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Zero(t, result.Candidates)
}

func TestNomadMigrationEvacuationSerializesNodesAndRetriesIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	f, _ := migrationExecutionSource(t, pool, "first", "")
	other, _ := migrationExecutionSource(t, pool, "second", "")
	migrationReadyTarget(t, f, "first", "b")
	migrationReadyTarget(t, f, "second", "b")
	fenceMigrationSource(t, f, "draining")
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			w, err := nomadmigration.NewEvacuation(NewPGSandboxStore(pool))
			require.NoError(t, err)
			_, err = w.RunOnce(f.ctx)
			require.NoError(t, err)
		}()
	}
	group.Wait()
	var count int
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_migrations`).Scan(&count))
	require.Equal(t, 1, count, "one operation per physical node")
	var sandbox, id string
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT sandbox_id,txn_id FROM manager.sandbox_lifecycle_txns WHERE kind='migrate'`).Scan(&sandbox, &id))
	activeFixture := f
	if sandbox == other.sandboxID {
		activeFixture = other
	}
	work, err := f.store.GetNomadMigrationCPUPreflightWork(f.ctx, id)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, activeFixture, work.Assignment)
	require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, sandbox, id, "incompatible test node"))
	// Backoff alone cannot release a node's durable staging custody.
	_, err = pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET aborted_at=NOW()-INTERVAL '6 minutes' WHERE txn_id=$1`, id)
	require.NoError(t, err)
	ids, err := f.store.ListNomadMigrationEvacuations(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	node := &stagingWorkNode{t: t, f: activeFixture, calls: map[string]int{}}
	release, err := nomadmigration.NewStagingRelease(f.store, node)
	require.NoError(t, err)
	for range 2 {
		_, err = release.RunOnce(f.ctx)
		require.NoError(t, err)
	}
	_, err = pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET aborted_at=NOW() WHERE txn_id=$1`, id)
	require.NoError(t, err)
	ids, err = f.store.ListNomadMigrationEvacuations(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids, "cleanup cannot bypass node retry delay")
	_, err = pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns SET aborted_at=NOW()-INTERVAL '6 minutes' WHERE txn_id=$1`, id)
	require.NoError(t, err)
	ids, err = f.store.ListNomadMigrationEvacuations(f.ctx, "", 8)
	require.NoError(t, err)
	require.Len(t, ids, 2)
	worker, err := nomadmigration.NewEvacuation(NewPGSandboxStore(pool))
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_migrations`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestNomadMigrationEvacuationExcludesBusyDestinationIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	first, _ := migrationExecutionSource(t, pool, "busy-first", "a")
	second, _ := migrationExecutionSource(t, pool, "busy-second", "c")
	migrationReadyTarget(t, first, "b-first", "b")
	migrationReadyTarget(t, first, "b-second", "b")
	migrationReadyTarget(t, first, "d", "d")
	fenceMigrationSource(t, first, "draining")
	fenceMigrationSource(t, second, "draining")
	advanced, err := first.store.ReserveNomadMigrationEvacuation(first.ctx, first.sandboxID)
	require.NoError(t, err)
	require.True(t, advanced)
	advanced, err = second.store.ReserveNomadMigrationEvacuation(second.ctx, second.sandboxID)
	require.NoError(t, err)
	require.True(t, advanced)
	var uid string
	require.NoError(t, pool.QueryRow(second.ctx, `SELECT r.node_uid FROM manager.sandbox_runtime_migrations m
 JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id JOIN manager.runtime_slots r ON r.slot_id=m.target_slot_id
 WHERE l.sandbox_id=$1`, second.sandboxID).Scan(&uid))
	require.Equal(t, "node-d", uid, "skip additional warm carriers on busy destination")
}

func TestNomadMigrationEvacuationRechecksAuthorityAndCapacityIntegration(t *testing.T) {
	for _, change := range []string{"no-capacity", "fence-removed", "terminated", "expired", "writer-expired", "legacy"} {
		t.Run(change, func(t *testing.T) {
			f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), change, "")
			if change != "no-capacity" {
				migrationReadyTarget(t, f, change, "b")
			}
			fenceMigrationSource(t, f, "draining")
			ids, err := f.store.ListNomadMigrationEvacuations(f.ctx, "", 8)
			require.NoError(t, err)
			require.Equal(t, []string{f.sandboxID}, ids)
			switch change {
			case "fence-removed":
				_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.runtime_node_fences`)
			case "terminated":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating' WHERE sandbox_id=$1`, f.sandboxID)
			case "expired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
			case "writer-expired":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE grant_id=$1`, f.issue.GrantID)
			case "legacy":
				tx, e := f.pool.Begin(f.ctx)
				require.NoError(t, e)
				defer tx.Rollback(f.ctx)
				_, e = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots DISABLE TRIGGER runtime_slot_claim_inputs_guard`)
				require.NoError(t, e)
				_, e = tx.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_runtime_assignment=NULL WHERE slot_id=$1`, f.slotID)
				require.NoError(t, e)
				_, e = tx.Exec(f.ctx, `ALTER TABLE manager.runtime_slots ENABLE TRIGGER runtime_slot_claim_inputs_guard`)
				require.NoError(t, e)
				require.NoError(t, tx.Commit(f.ctx))
			}
			require.NoError(t, err)
			advanced, err := f.store.ReserveNomadMigrationEvacuation(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.False(t, advanced)
			var count int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_migrations`).Scan(&count))
			require.Zero(t, count)
			record, err := f.store.GetSandbox(f.ctx, f.sandboxID)
			require.NoError(t, err)
			require.Equal(t, int64(1), record.RuntimeGeneration)
			var leases int
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
			require.Equal(t, 1, leases)
		})
	}
}

type evacuationLostReplyStore struct {
	*PGSandboxStore
	lose bool
}

func (s *evacuationLostReplyStore) ReserveNomadMigrationEvacuation(ctx context.Context, id string) (bool, error) {
	advanced, err := s.PGSandboxStore.ReserveNomadMigrationEvacuation(ctx, id)
	if err == nil && advanced && s.lose {
		s.lose = false
		return false, errors.New("lost reservation commit response")
	}
	return advanced, err
}
func TestNomadMigrationEvacuationLostCommitReplyIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "lost-reply", "")
	migrationReadyTarget(t, f, "lost-reply", "b")
	fenceMigrationSource(t, f, "draining")
	w, err := nomadmigration.NewEvacuation(&evacuationLostReplyStore{PGSandboxStore: f.store, lose: true})
	require.NoError(t, err)
	_, err = w.RunOnce(f.ctx)
	require.Error(t, err)
	w, err = nomadmigration.NewEvacuation(NewPGSandboxStore(f.pool))
	require.NoError(t, err)
	result, err := w.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Zero(t, result.Candidates)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_migrations`).Scan(&count))
	require.Equal(t, 1, count)
}
