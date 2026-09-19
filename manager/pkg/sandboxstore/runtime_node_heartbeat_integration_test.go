package sandboxstore

import (
	"context"
	"testing"
	"time"

	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/stretchr/testify/require"
)

func TestRuntimeNodeLifecycleHeartbeatBudgetSurvivesConcurrentReplicasIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPoolAt(t, 60)
	store := NewPGSandboxStore(pool)
	_, err := store.EnsureRuntimeNodePoolState(ctx, "elastic", "nomad")
	require.NoError(t, err)
	request := &ObserveRuntimeNodeLifecycleActionRequest{
		Token: "heartbeat-budget", PoolID: "elastic", LifecycleHookID: "hook-out",
		ProviderInstanceIDs: []string{"i-1"}, Transition: "scale_out",
		RecoveryDeadline: time.Now().Add(20 * time.Minute),
	}
	_, err = store.ObserveRuntimeNodeLifecycleAction(ctx, request)
	require.NoError(t, err)
	reserved, err := store.ReserveRuntimeNodeLifecycleHeartbeat(ctx, "another-pool", request.Token, time.Minute)
	require.NoError(t, err)
	require.False(t, reserved)
	type result struct {
		reserved bool
		err      error
	}
	results := make(chan result, 32)
	for range 32 {
		go func() {
			reserved, err := NewPGSandboxStore(pool).ReserveRuntimeNodeLifecycleHeartbeat(ctx, "elastic", request.Token, time.Minute)
			results <- result{reserved, err}
		}()
	}
	winners := 0
	for range 32 {
		r := <-results
		require.NoError(t, r.err)
		if r.reserved {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	// Re-observation and a replacement process preserve the consumed attempt
	// even when the previous process died before recording provider success.
	_, err = store.ObserveRuntimeNodeLifecycleAction(ctx, request)
	require.NoError(t, err)
	reserved, err = NewPGSandboxStore(pool).ReserveRuntimeNodeLifecycleHeartbeat(ctx, "elastic", request.Token, time.Minute)
	require.NoError(t, err)
	require.False(t, reserved)
	for attempt := 1; attempt < RuntimeNodeLifecycleHeartbeatMaxAttempts; attempt++ {
		_, err = pool.Exec(ctx, `UPDATE manager.runtime_node_lifecycle_actions SET heartbeat_not_before = NOW() - INTERVAL '1 second' WHERE lifecycle_action_token = $1`, request.Token)
		require.NoError(t, err)
		reserved, err = NewPGSandboxStore(pool).ReserveRuntimeNodeLifecycleHeartbeat(ctx, "elastic", request.Token, time.Minute)
		require.NoError(t, err)
		require.True(t, reserved)
	}
	_, err = pool.Exec(ctx, `UPDATE manager.runtime_node_lifecycle_actions SET heartbeat_not_before = NOW() - INTERVAL '1 second' WHERE lifecycle_action_token = $1`, request.Token)
	require.NoError(t, err)
	reserved, err = store.ReserveRuntimeNodeLifecycleHeartbeat(ctx, "elastic", request.Token, time.Minute)
	require.NoError(t, err)
	require.False(t, reserved)
	var attempts int
	require.NoError(t, pool.QueryRow(ctx, `SELECT heartbeat_attempts FROM manager.runtime_node_lifecycle_actions WHERE lifecycle_action_token = $1`, request.Token).Scan(&attempts))
	require.Equal(t, RuntimeNodeLifecycleHeartbeatMaxAttempts, attempts)
	err = migrate.Down(ctx, pool, ".", migrate.WithBaseFS(storemigrations.FS),
		migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{}))
	require.ErrorContains(t, err, "Durable lifecycle recovery receipts cannot be rolled back")
	require.NoError(t, pool.QueryRow(ctx, `SELECT heartbeat_attempts FROM manager.runtime_node_lifecycle_actions WHERE lifecycle_action_token = $1`, request.Token).Scan(&attempts))
	require.Equal(t, RuntimeNodeLifecycleHeartbeatMaxAttempts, attempts)
	// Exhaustion must not prevent the controller from completing cleanup.
	require.NoError(t, store.CompleteRuntimeNodeLifecycleAction(ctx, request.Token, "abandoned"))
	reserved, err = store.ReserveRuntimeNodeLifecycleHeartbeat(ctx, "elastic", request.Token, time.Minute)
	require.NoError(t, err)
	require.False(t, reserved)
}
