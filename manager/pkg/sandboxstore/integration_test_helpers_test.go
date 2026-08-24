package sandboxstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/stretchr/testify/require"
)

func newSandboxStoreIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := newSandboxStoreIntegrationDatabase(t)
	prepareSandboxStoreCredentialSchema(t, pool)
	require.NoError(t, RunSandboxStoreMigrations(context.Background(), pool, noopSandboxStoreMigrateLogger{}))
	return pool
}

func prepareSandboxStoreCredentialSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`)
	require.NoError(t, err)
	require.NoError(t, egressauthstore.RunMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
}

func newSandboxStoreIntegrationDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL, Schema: "scheduler", MaxConns: 10,
	})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS manager CASCADE")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS manager CASCADE") })
	return pool
}

func rootFSTestSandboxRecord(sandboxID, teamID string) *SandboxRecord {
	return &SandboxRecord{
		ID: sandboxID, TeamID: teamID, UserID: "user-1",
		TemplateID: "template-1", TemplateName: "template-1",
		TemplateNamespace: "template-default", DesiredState: SandboxDesiredStateActive,
		ResourceMillicpu: 1000, ResourceMemoryMiB: 1024,
		CreatedAt: time.Now().UTC(),
	}
}

func rootFSTestCountRows(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	if table != "rootfs_object_deletions" {
		t.Fatalf("unexpected table %q", table)
	}
	var count int64
	require.NoError(t, pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM manager.rootfs_object_deletions").Scan(&count))
	return count
}

type noopSandboxStoreMigrateLogger struct{}

func (noopSandboxStoreMigrateLogger) Printf(string, ...any) {}
func (noopSandboxStoreMigrateLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
