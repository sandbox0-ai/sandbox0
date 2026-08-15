package quota

import (
	"context"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	quotamigrations "github.com/sandbox0-ai/sandbox0/pkg/quota/migrations"
)

type migrationTestLogger struct{}

func (migrationTestLogger) Printf(string, ...any) {}
func (migrationTestLogger) Fatalf(string, ...any) {}

func TestVolumeQuotaRetirementMigrationIntegration(t *testing.T) {
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(pool.Close)
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+SchemaName+" CASCADE")
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+SchemaName+" CASCADE")
	})

	opts := []migrate.Option{
		migrate.WithLogger(migrationTestLogger{}),
		migrate.WithSchema(SchemaName),
	}
	if err := migrate.Up(ctx, pool, ".",
		append(opts, migrate.WithBaseFS(quotaMigrationsThrough(t, "00003")))...,
	); err != nil {
		t.Fatalf("run quota migrations before volume retirement: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO quota.team_quota_limits (
    team_id, dimension, limit_value, interval_ms, burst_value
) VALUES ('team-test', 'volume_storage_gb', 10, 0, 0);
INSERT INTO quota.region_quota_limits (
    dimension, limit_value, interval_ms, burst_value, managed_by
) VALUES ('snapshot_storage_gb', 10, 0, 0, 'test');
INSERT INTO quota.region_quota_bootstrap (dimension)
VALUES ('volume_storage_gb');
`); err != nil {
		t.Fatalf("seed legacy volume quotas: %v", err)
	}

	if err := RunMigrations(ctx, pool, migrationTestLogger{}); err != nil {
		t.Fatalf("run volume quota retirement migration: %v", err)
	}
	assertVolumeQuotasRetired(t, ctx, pool)

	if err := migrate.Down(ctx, pool, ".",
		append(opts, migrate.WithBaseFS(quotamigrations.FS))...,
	); err != nil {
		t.Fatalf("roll back terminal quota migration: %v", err)
	}
	assertVolumeQuotasRetired(t, ctx, pool)
}

func assertVolumeQuotasRetired(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var rows int
	if err := pool.QueryRow(ctx, `
SELECT
    (SELECT COUNT(*) FROM quota.team_quota_limits
     WHERE dimension IN ('volume_storage_gb', 'snapshot_storage_gb'))
  + (SELECT COUNT(*) FROM quota.region_quota_limits
     WHERE dimension IN ('volume_storage_gb', 'snapshot_storage_gb'))
  + (SELECT COUNT(*) FROM quota.region_quota_bootstrap
     WHERE dimension IN ('volume_storage_gb', 'snapshot_storage_gb'))
`).Scan(&rows); err != nil {
		t.Fatalf("count retired volume quotas: %v", err)
	}
	if rows != 0 {
		t.Fatalf("retired volume quota rows = %d, want 0", rows)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO quota.team_quota_limits (
    team_id, dimension, limit_value, interval_ms, burst_value
) VALUES ('team-rejected', 'volume_storage_gb', 1, 0, 0)
`); err == nil {
		t.Fatal("retired volume quota dimension was accepted")
	}
}

func quotaMigrationsThrough(t *testing.T, maximumPrefix string) fs.FS {
	t.Helper()
	files, err := fs.Glob(quotamigrations.FS, "*.sql")
	if err != nil {
		t.Fatalf("list quota migrations: %v", err)
	}
	selected := fstest.MapFS{}
	for _, name := range files {
		if len(name) < len(maximumPrefix) || name[:len(maximumPrefix)] > maximumPrefix {
			continue
		}
		data, err := fs.ReadFile(quotamigrations.FS, name)
		if err != nil {
			t.Fatalf("read quota migration %s: %v", name, err)
		}
		selected[name] = &fstest.MapFile{Data: data, Mode: 0o444}
	}
	return selected
}
