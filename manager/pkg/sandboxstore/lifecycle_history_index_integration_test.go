package sandboxstore

import (
	"encoding/json"
	"testing"
	"testing/fstest"

	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/stretchr/testify/require"
)

// Exercise the real concurrent Goose migration against a history-sized fixture.
// A partial active-lifecycle index cannot serve this terminal-history read.
func TestLifecycleHistoryIndexPlanAndRollbackIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationDatabase(t)
	ctx := t.Context()
	_, err := pool.Exec(ctx, `CREATE SCHEMA manager;
		CREATE TABLE manager.sandbox_lifecycle_txns (
			txn_id text PRIMARY KEY, sandbox_id text NOT NULL,
			kind text NOT NULL DEFAULT 'pause', phase text NOT NULL,
			source text NOT NULL DEFAULT 'manual', epoch bigint NOT NULL,
			error text NOT NULL DEFAULT '', committed_at timestamptz, aborted_at timestamptz,
			resource_millicpu bigint NOT NULL DEFAULT 500, resource_memory_mib bigint NOT NULL DEFAULT 128
		);
		INSERT INTO manager.sandbox_lifecycle_txns (txn_id,sandbox_id,phase,epoch)
		SELECT 'txn-'||i, 'sandbox-'||(i/2), CASE WHEN i%2=0 THEN 'committed' ELSE 'aborted' END, i%2+1
		FROM generate_series(1,25000) i;
		INSERT INTO manager.sandbox_lifecycle_txns (txn_id,sandbox_id,phase,epoch) VALUES
			('old','target','committed',1), ('z','target','committed',2),
			('a','target','aborted',2), ('pending','target','preparing',2), ('next','target','committed',3);
		ANALYZE manager.sandbox_lifecycle_txns;`)
	require.NoError(t, err)
	const query = `SELECT txn_id,kind,phase,source,epoch,error,committed_at,aborted_at,resource_millicpu,resource_memory_mib
		FROM manager.sandbox_lifecycle_txns
		WHERE sandbox_id=$1 AND epoch>$2 AND phase IN ($3,$4) ORDER BY epoch,txn_id`
	plan := func() string {
		t.Helper()
		var raw json.RawMessage
		require.NoError(t, pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+query,
			"target", 1, "committed", "aborted").Scan(&raw))
		return string(raw)
	}
	require.Contains(t, plan(), "Seq Scan")
	payload, err := storemigrations.FS.ReadFile("00119_lifecycle_history_index.sql")
	require.NoError(t, err)
	options := []migrate.Option{
		migrate.WithBaseFS(fstest.MapFS{"00119_lifecycle_history_index.sql": {Data: payload}}),
		migrate.WithSchema(sandboxStoreSchemaName),
	}
	for range 2 {
		require.NoError(t, migrate.Up(ctx, pool, ".", options...))
	}
	require.Contains(t, plan(), "idx_sandbox_lifecycle_txns_history")
	var valid bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT indisvalid AND indisready FROM pg_index
		WHERE indexrelid='manager.idx_sandbox_lifecycle_txns_history'::regclass`).Scan(&valid))
	require.True(t, valid)
	var ids []string
	require.NoError(t, pool.QueryRow(ctx, `SELECT array_agg(txn_id ORDER BY epoch,txn_id)
		FROM manager.sandbox_lifecycle_txns WHERE sandbox_id='target' AND epoch>1
		AND phase IN ('committed','aborted')`).Scan(&ids))
	require.Equal(t, []string{"a", "z", "next"}, ids)
	require.NoError(t, migrate.Down(ctx, pool, ".", options...))
	require.Contains(t, plan(), "Seq Scan")
	var count int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM manager.sandbox_lifecycle_txns").Scan(&count))
	require.Equal(t, 25005, count)
}
