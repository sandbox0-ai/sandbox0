package sandboxstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDurableLifecycleRecoveryMigrationBackfillsLegacyActionsIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPoolAt(t, 59)
	store := NewPGSandboxStore(pool)
	_, err := store.EnsureRuntimeNodePoolState(ctx, "elastic", "nomad")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.runtime_node_lifecycle_actions
			(lifecycle_action_token,pool_id,lifecycle_hook_id,transition,state,first_observed_at,completed_at)
		VALUES
			('legacy-completed','elastic','out','scale_out','completed',NOW()-INTERVAL '2 hours',NOW()-INTERVAL '1 hour'),
			('legacy-pending','elastic','out','scale_out','pending',NOW()-INTERVAL '2 hours',NULL);
		INSERT INTO manager.runtime_node_lifecycle_action_instances
			(lifecycle_action_token,provider_instance_id)
		VALUES
			('legacy-completed','i-completed'),
			('legacy-pending','i-pending');
	`)
	require.NoError(t, err)
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))

	rows, err := pool.Query(ctx, `
		SELECT state, recovery_deadline_at IS NOT NULL,
			COALESCE(convergence_proof ? 'legacy', FALSE),
			COALESCE(convergence_proof ->> 'state' = state, FALSE)
		FROM manager.runtime_node_lifecycle_actions
		WHERE lifecycle_action_token IN ('legacy-completed','legacy-pending')
		ORDER BY lifecycle_action_token
	`)
	require.NoError(t, err)
	defer rows.Close()
	var results [2]bool
	index := 0
	for rows.Next() {
		var state string
		var deadline, legacy, proofMatches bool
		require.NoError(t, rows.Scan(&state, &deadline, &legacy, &proofMatches))
		require.True(t, deadline)
		if state == "completed" {
			require.True(t, legacy)
			require.True(t, proofMatches)
		} else {
			require.Equal(t, "pending", state)
			require.False(t, legacy)
		}
		results[index] = true
		index++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, [2]bool{true, true}, results)
}
