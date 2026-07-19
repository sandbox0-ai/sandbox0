package service

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSandboxLifecycleHistoryRetentionBoundsTerminalRows(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxRetentionIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, sandboxRetentionRecord(
		"sandbox-history",
		"team-history",
	)))

	for index := 0; index < maxTerminalLifecycleTxns+5; index++ {
		txnID := fmt.Sprintf("txn-%03d", index)
		require.NoError(t, store.WithSandboxLock(
			ctx,
			"sandbox-history",
			func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
				if err := tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
					ID:        txnID,
					SandboxID: "sandbox-history",
					Kind:      SandboxLifecycleKindPause,
				}); err != nil {
					return err
				}
				return tx.CommitLifecycleTxn(lockCtx, txnID, "")
			},
		))
	}
	assertManagerRetentionCount(t, pool, `
		SELECT COUNT(*)
		FROM manager.sandbox_lifecycle_txns
		WHERE sandbox_id = 'sandbox-history'
			AND phase IN ('committed', 'aborted')
	`, maxTerminalLifecycleTxns)

	_, err := pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET committed_at = NOW() - INTERVAL '48 hours',
			updated_at = NOW() - INTERVAL '48 hours'
		WHERE sandbox_id = 'sandbox-history'
	`)
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(
		ctx,
		"sandbox-history",
		func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
			txnID := "txn-fresh"
			if err := tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
				ID:        txnID,
				SandboxID: "sandbox-history",
				Kind:      SandboxLifecycleKindPause,
			}); err != nil {
				return err
			}
			return tx.AbortLifecycleTxn(lockCtx, txnID, "test")
		},
	))
	assertManagerRetentionCount(t, pool, `
		SELECT COUNT(*)
		FROM manager.sandbox_lifecycle_txns
		WHERE sandbox_id = 'sandbox-history'
			AND phase IN ('committed', 'aborted')
	`, 1)

	require.NoError(t, store.WithSandboxLock(
		ctx,
		"sandbox-history",
		func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
			return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
				ID:        "txn-active",
				SandboxID: "sandbox-history",
				Kind:      SandboxLifecycleKindPause,
			})
		},
	))
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `
		SELECT 1
		FROM manager.sandboxes
		WHERE sandbox_id = 'sandbox-history'
		FOR UPDATE
	`)
	require.NoError(t, err)
	require.NoError(t, pruneSandboxLifecycleHistory(ctx, tx, "sandbox-history"))
	require.NoError(t, tx.Commit(ctx))
	assertManagerRetentionCount(t, pool, `
		SELECT COUNT(*)
		FROM manager.sandbox_lifecycle_txns
		WHERE sandbox_id = 'sandbox-history'
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`, 1)
}

func TestDeletedSandboxRetentionRequiresCleanupAndDependencyFences(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxRetentionIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	installSandboxRetentionQuotaStub(t, pool)
	teamID := "team-deleted-history"

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandboxes (
			sandbox_id, team_id, user_id, template_id, template_name,
			template_namespace, status, deleted_at, cleanup_completed_at
		)
		SELECT
			'eligible-' || value::TEXT,
			$1,
			'user-1',
			'template-1',
			'template-1',
			'default',
			$2,
			NOW(),
			NOW()
		FROM generate_series(1, $3::BIGINT) AS value
	`, teamID, SandboxStatusDeleted, maxDeletedSandboxesPerTeam+5)
	require.NoError(t, err)

	blockedIDs := []string{
		"active-blocked",
		"binding-blocked",
		"filesystem-blocked",
		"layer-blocked",
		"quota-blocked",
	}
	for _, sandboxID := range append([]string{"cleanup-pending"}, blockedIDs...) {
		require.NoError(t, store.UpsertSandbox(
			ctx,
			sandboxRetentionRecord(sandboxID, teamID),
		))
		require.NoError(t, store.MarkSandboxDeleted(
			ctx,
			sandboxID,
			time.Now().Add(-48*time.Hour),
		))
		if sandboxID != "cleanup-pending" {
			_, err := pool.Exec(ctx, `
				UPDATE manager.sandboxes
				SET cleanup_completed_at = NOW() - INTERVAL '48 hours',
					current_pod_namespace = '',
					current_pod_name = ''
				WHERE sandbox_id = $1
			`, sandboxID)
			require.NoError(t, err)
		}
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.sandbox_lifecycle_txns (
			txn_id, sandbox_id, kind, phase, epoch
		) VALUES (
			'active-blocker',
			'active-blocked',
			'pause',
			'preparing',
			1
		)
	`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id
		) VALUES
			('binding-fs', $1),
			('filesystem-blocked', $1)
	`, teamID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_bindings (
			sandbox_id, filesystem_id, team_id
		) VALUES (
			'binding-blocked',
			'binding-fs',
			$1
		)
	`, teamID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_layers (
			layer_id, source_sandbox_id, team_id,
			runtime_generation, diff_digest, diff_object_key
		) VALUES (
			'layer-blocker',
			'layer-blocked',
			$1,
			1,
			'sha256:blocker',
			'rootfs/blocker.tar'
		)
	`, teamID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO quota.allocations (
			allocation_id, team_id, owner_kind, owner_id
		) VALUES (
			'quota-blocker',
			$1,
			'sandbox',
			'quota-blocked'
		)
	`, teamID)
	require.NoError(t, err)

	triggerIDs := make([]string, 8)
	for index := range triggerIDs {
		triggerIDs[index] = fmt.Sprintf("trigger-%d", index)
		require.NoError(t, store.UpsertSandbox(
			ctx,
			sandboxRetentionRecord(triggerIDs[index], teamID),
		))
		require.NoError(t, store.MarkSandboxDeleted(
			ctx,
			triggerIDs[index],
			time.Now(),
		))
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(triggerIDs))
	for _, sandboxID := range triggerIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			errs <- store.MarkSandboxCleanupCompleted(ctx, id, time.Now().UTC())
		}(sandboxID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.LessOrEqual(
		t,
		managerRetentionCount(t, pool, `
			SELECT COUNT(*)
			FROM manager.sandboxes
			WHERE team_id = 'team-deleted-history'
				AND cleanup_completed_at IS NOT NULL
				AND sandbox_id LIKE ANY (ARRAY['eligible-%', 'trigger-%'])
		`),
		int64(maxDeletedSandboxesPerTeam),
	)
	for _, sandboxID := range append([]string{"cleanup-pending"}, blockedIDs...) {
		assertManagerRetentionCount(t, pool, `
			SELECT COUNT(*)
			FROM manager.sandboxes
			WHERE sandbox_id = $1
		`, 1, sandboxID)
	}

	_, err = pool.Exec(ctx, `
		UPDATE manager.sandboxes
		SET deleted_at = NOW() - INTERVAL '48 hours',
			cleanup_completed_at = NOW() - INTERVAL '48 hours'
		WHERE team_id = $1
			AND cleanup_completed_at IS NOT NULL
			AND sandbox_id LIKE ANY (ARRAY['eligible-%', 'trigger-%'])
	`, teamID)
	require.NoError(t, err)
	require.NoError(t, store.UpsertSandbox(
		ctx,
		sandboxRetentionRecord("recent-trigger", teamID),
	))
	require.NoError(t, store.MarkSandboxDeleted(
		ctx,
		"recent-trigger",
		time.Now(),
	))
	require.NoError(t, store.MarkSandboxCleanupCompleted(
		ctx,
		"recent-trigger",
		time.Now(),
	))
	assertManagerRetentionCount(t, pool, `
		SELECT COUNT(*)
		FROM manager.sandboxes
		WHERE team_id = 'team-deleted-history'
			AND cleanup_completed_at IS NOT NULL
			AND sandbox_id LIKE ANY (ARRAY['eligible-%', 'trigger-%'])
	`, 0)

	for _, sandboxID := range append([]string{"cleanup-pending"}, blockedIDs...) {
		assertManagerRetentionCount(t, pool, `
			SELECT COUNT(*)
			FROM manager.sandboxes
			WHERE sandbox_id = $1
		`, 1, sandboxID)
	}

	_, err = pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = 'aborted',
			aborted_at = NOW(),
			updated_at = NOW()
		WHERE txn_id = 'active-blocker';
		DELETE FROM manager.sandbox_rootfs_bindings
		WHERE sandbox_id = 'binding-blocked';
		DELETE FROM manager.rootfs_filesystems
		WHERE filesystem_id IN ('binding-fs', 'filesystem-blocked');
		DELETE FROM manager.rootfs_layers
		WHERE layer_id = 'layer-blocker';
		DELETE FROM quota.allocations
		WHERE allocation_id = 'quota-blocker'
	`)
	require.NoError(t, err)
	require.NoError(t, store.MarkSandboxCleanupCompleted(
		ctx,
		"active-blocked",
		time.Now(),
	))
	for _, sandboxID := range blockedIDs {
		assertManagerRetentionCount(t, pool, `
			SELECT COUNT(*)
			FROM manager.sandboxes
			WHERE sandbox_id = $1
		`, 0, sandboxID)
	}
	assertManagerRetentionCount(t, pool, `
		SELECT COUNT(*)
		FROM manager.sandboxes
		WHERE sandbox_id IN ('cleanup-pending', 'recent-trigger')
	`, 2)
}

func TestSandboxCleanupFencePreservesRuntimeIdentityUntilCompletion(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxRetentionIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	installSandboxRetentionQuotaStub(t, pool)
	record := sandboxRetentionRecord("sandbox-fence", "team-fence")
	record.CurrentPodNamespace = "sandbox"
	record.CurrentPodName = "sandbox-fence-pod"
	require.NoError(t, store.UpsertSandbox(ctx, record))

	deletedAt := time.Now().UTC()
	require.NoError(t, store.MarkSandboxDeleted(ctx, record.ID, deletedAt))
	deleted, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	assert.Equal(t, record.CurrentPodNamespace, deleted.CurrentPodNamespace)
	assert.Equal(t, record.CurrentPodName, deleted.CurrentPodName)
	assert.True(t, deleted.CleanupCompletedAt.IsZero())

	pending, err := store.ListPendingDeletedSandboxes(
		ctx,
		naming.DefaultClusterID,
		"",
		10,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, record.ID, pending[0].ID)
	pendingAfter, err := store.ListPendingDeletedSandboxes(
		ctx,
		naming.DefaultClusterID,
		record.ID,
		10,
	)
	require.NoError(t, err)
	assert.Empty(t, pendingAfter)
	otherCluster, err := store.ListPendingDeletedSandboxes(
		ctx,
		"other-cluster",
		"",
		10,
	)
	require.NoError(t, err)
	assert.Empty(t, otherCluster)

	require.NoError(t, store.MarkSandboxCleanupCompleted(ctx, record.ID, deletedAt))
	completed, err := store.GetSandbox(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Empty(t, completed.CurrentPodNamespace)
	assert.Empty(t, completed.CurrentPodName)
	assert.False(t, completed.CleanupCompletedAt.IsZero())
	pending, err = store.ListPendingDeletedSandboxes(
		ctx,
		naming.DefaultClusterID,
		"",
		10,
	)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func newSandboxRetentionIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	require.NoError(t, err)

	databaseName := "manager_retention_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedName := `"` + strings.ReplaceAll(databaseName, `"`, `""`) + `"`
	_, err = admin.Exec(ctx, "CREATE DATABASE "+quotedName)
	require.NoError(t, err)
	testConfig := adminConfig.Copy()
	testConfig.ConnConfig.Database = databaseName
	testConfig.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, testConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Close()
		_, dropErr := admin.Exec(context.Background(), "DROP DATABASE "+quotedName)
		assert.NoError(t, dropErr)
		admin.Close()
	})
	require.NoError(t, RunSandboxStoreMigrations(
		ctx,
		pool,
		noopSandboxStoreMigrateLogger{},
	))
	return pool
}

func installSandboxRetentionQuotaStub(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		CREATE SCHEMA quota;
		CREATE TABLE quota.allocations (
			allocation_id TEXT PRIMARY KEY,
			team_id TEXT NOT NULL,
			owner_kind TEXT NOT NULL,
			owner_id TEXT NOT NULL
		);
		CREATE INDEX idx_retention_quota_owner
			ON quota.allocations(team_id, owner_kind, owner_id)
	`)
	require.NoError(t, err)
}

func sandboxRetentionRecord(sandboxID, teamID string) *SandboxRecord {
	return &SandboxRecord{
		ID:                sandboxID,
		TeamID:            teamID,
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "default",
		Status:            SandboxStatusRunning,
		CreatedAt:         time.Now().UTC(),
	}
}

func managerRetentionCount(
	t *testing.T,
	pool *pgxpool.Pool,
	query string,
	args ...any,
) int64 {
	t.Helper()
	var count int64
	require.NoError(t, pool.QueryRow(context.Background(), query, args...).Scan(&count))
	return count
}

func assertManagerRetentionCount(
	t *testing.T,
	pool *pgxpool.Pool,
	query string,
	want int64,
	args ...any,
) {
	t.Helper()
	assert.Equal(t, want, managerRetentionCount(t, pool, query, args...))
}
