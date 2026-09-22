package sandboxstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNomadMigrationDrainNotificationsFollowCommitIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "drain-wakeup", "")
	ctx, cancel := context.WithCancel(f.ctx)
	awakened := make(chan struct{}, 8)
	done := make(chan error, 1)
	go func() { done <- f.store.WatchNomadMigrationDrains(ctx, func() { awakened <- struct{}{} }) }()
	t.Cleanup(func() { cancel(); <-done })
	waitWake := func() {
		t.Helper()
		select {
		case <-awakened:
		case <-time.After(5 * time.Second):
			t.Fatal("committed drain did not wake the listener")
		}
	}
	noWake := func() {
		t.Helper()
		select {
		case <-awakened:
			t.Fatal("uncommitted or irrelevant change woke the listener")
		case <-time.After(50 * time.Millisecond):
		}
	}
	waitWake() // LISTEN must commit before the initial catch-up scan.
	query := `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason)
 SELECT cluster_id,node_id,node_uid,'draining','notification integration' FROM manager.runtime_slots WHERE slot_id=$1
 ON CONFLICT(cluster_id,node_id,node_uid) DO UPDATE SET state=EXCLUDED.state,reason=EXCLUDED.reason`
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, query, f.slotID)
	require.NoError(t, err)
	noWake()
	require.NoError(t, tx.Rollback(f.ctx))
	noWake()
	_, err = f.pool.Exec(f.ctx, query, f.slotID)
	require.NoError(t, err)
	waitWake()
	_, err = f.pool.Exec(f.ctx, query, f.slotID)
	require.NoError(t, err)
	noWake() // An unchanged drain fence is not new work.
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.runtime_node_fences WHERE node_uid=(SELECT node_uid FROM manager.runtime_slots WHERE slot_id=$1)`, f.slotID)
	require.NoError(t, err)
	waitWake() // Removing a fence can make destination capacity eligible again.
}
