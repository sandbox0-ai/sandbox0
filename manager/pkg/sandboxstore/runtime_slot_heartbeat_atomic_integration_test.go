package sandboxstore

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSlotHeartbeatReturnsCommittedStateAndCannotReviveTerminalIntegration(t *testing.T) {
	authority := newSandboxStoreIntegrationPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	config := authority.Config().Copy()
	config.MaxConns, config.MinConns = 1, 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store, observer := NewPGSandboxStore(pool), NewPGSandboxStore(authority)
	registration := runtimeSlotTestRegistration("slot-heartbeat-atomic", "allocation-heartbeat-atomic")
	_, err = registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x71}, 32)
	ready, err := store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
		HeartbeatTTL: time.Second,
	})
	require.NoError(t, err)
	request := &HeartbeatRuntimeSlotRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID, TTL: time.Minute,
	}
	heartbeat, err := store.HeartbeatRuntimeSlot(ctx, request)
	require.NoError(t, err)
	require.Equal(t, ready.State, heartbeat.State)
	require.Equal(t, ready.Revision, heartbeat.Revision)
	require.Equal(t, ready.RuntimeReadyDigest, heartbeat.RuntimeReadyDigest)
	require.True(t, heartbeat.HeartbeatExpiresAt.After(ready.HeartbeatExpiresAt))
	committed, err := observer.GetRuntimeSlot(ctx, ready.ID)
	require.NoError(t, err)
	require.Equal(t, heartbeat.HeartbeatExpiresAt, committed.HeartbeatExpiresAt)
	for _, field := range []string{"allocation", "node", "boot"} {
		wrong := *request
		switch field {
		case "allocation":
			wrong.AllocationID = "different-allocation"
		case "node":
			wrong.NodeUID = "different-node"
		case "boot":
			wrong.NodeBootID = "different-boot"
		}
		_, err = store.HeartbeatRuntimeSlot(ctx, &wrong)
		require.ErrorIs(t, err, ErrRuntimeSlotConflict)
	}
	unchanged, err := observer.GetRuntimeSlot(ctx, ready.ID)
	require.NoError(t, err)
	require.Equal(t, committed.HeartbeatExpiresAt, unchanged.HeartbeatExpiresAt)

	connection, err := pool.Acquire(ctx)
	require.NoError(t, err)
	pid := connection.Conn().PgConn().PID()
	connection.Release()
	blocker, err := authority.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	var workers sync.WaitGroup
	defer func() { cancel(); _ = blocker.Rollback(context.Background()); workers.Wait() }()
	var terminalHeartbeat time.Time
	require.NoError(t, blocker.QueryRow(ctx, `UPDATE manager.runtime_slots
		SET state='terminal', revision=revision+1, heartbeat_expires_at=NOW()-INTERVAL '1 second'
		WHERE slot_id=$1 RETURNING heartbeat_expires_at`, ready.ID).Scan(&terminalHeartbeat))
	result := make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); _, err := store.HeartbeatRuntimeSlot(ctx, request); result <- err }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := authority.QueryRow(ctx, `SELECT cardinality(pg_blocking_pids($1))>0`, pid).Scan(&waiting)
		return err == nil && waiting
	}, 3*time.Second, 5*time.Millisecond, "heartbeat must wait behind the terminal transition")
	require.NoError(t, blocker.Commit(ctx))
	require.ErrorIs(t, <-result, ErrRuntimeSlotInvalid)
	terminal, err := observer.GetRuntimeSlot(ctx, ready.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateTerminal, terminal.State)
	require.Equal(t, ready.Revision+1, terminal.Revision)
	require.Equal(t, terminalHeartbeat, terminal.HeartbeatExpiresAt)
}
