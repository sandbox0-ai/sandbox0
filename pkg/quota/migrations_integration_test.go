package quota

import (
	"context"
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

func TestNomadQuotaFreshSchemaIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newQuotaMigrationIntegrationPool(t, ctx)
	if err := RunMigrations(ctx, pool, migrationTestLogger{}); err != nil {
		t.Fatalf("run fresh quota migrations: %v", err)
	}
	assertRetiredQuotaDimensionsRejected(t, ctx, pool)
}

func TestNomadQuotaTerminalCutoverIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newQuotaMigrationIntegrationPool(t, ctx)
	baseline, err := quotamigrations.FS.ReadFile("00001_nomad_quota_baseline.sql")
	if err != nil {
		t.Fatalf("read quota baseline: %v", err)
	}
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(fstest.MapFS{
			"00001_nomad_quota_baseline.sql": {Data: baseline, Mode: 0o444},
		}),
		migrate.WithLogger(migrationTestLogger{}),
		migrate.WithSchema(SchemaName),
	); err != nil {
		t.Fatalf("run quota baseline: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		ALTER TABLE quota.team_quota_limits
			DROP CONSTRAINT team_quota_limits_dimension_supported,
			DROP CONSTRAINT team_quota_limits_policy_shape;
		ALTER TABLE quota.region_quota_limits
			DROP CONSTRAINT region_quota_limits_dimension_supported,
			DROP CONSTRAINT region_quota_limits_policy_shape;
		ALTER TABLE quota.region_quota_bootstrap
			DROP CONSTRAINT region_quota_bootstrap_dimension_supported;
		INSERT INTO quota.team_quota_limits (
			team_id, dimension, limit_value, interval_ms, burst_value
		) VALUES ('team-test', 'volume_storage_gb', 10, 0, 0);
		INSERT INTO quota.region_quota_limits (
			dimension, limit_value, interval_ms, burst_value, managed_by
		) VALUES ('snapshot_storage_gb', 10, 0, 0, 'test');
		INSERT INTO quota.region_quota_bootstrap (dimension)
		VALUES ('cpu_millicpu');
	`); err != nil {
		t.Fatalf("seed retired quota dimensions: %v", err)
	}

	if err := RunMigrations(ctx, pool, migrationTestLogger{}); err != nil {
		t.Fatalf("run terminal quota migration: %v", err)
	}
	assertRetiredQuotaDimensionsRejected(t, ctx, pool)
}

func newQuotaMigrationIntegrationPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(pool.Close)
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+SchemaName+" CASCADE")
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+SchemaName+" CASCADE")
	})
	return pool
}

func assertRetiredQuotaDimensionsRejected(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var unsupportedRows int
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM quota.team_quota_limits
			 WHERE dimension NOT IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes'))
		  + (SELECT COUNT(*) FROM quota.region_quota_limits
			 WHERE dimension NOT IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes'))
		  + (SELECT COUNT(*) FROM quota.region_quota_bootstrap
			 WHERE dimension NOT IN ('active_sandboxes', 'sandbox_claims', 'api_requests', 'network_egress_bytes', 'network_ingress_bytes'))
	`).Scan(&unsupportedRows); err != nil {
		t.Fatalf("count unsupported quota dimensions: %v", err)
	}
	if unsupportedRows != 0 {
		t.Fatalf("unsupported quota rows = %d, want 0", unsupportedRows)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO quota.team_quota_limits (
			team_id, dimension, limit_value, interval_ms, burst_value
		) VALUES ('team-rejected', 'volume_storage_gb', 1, 0, 0)
	`); err == nil {
		t.Fatal("retired quota dimension was accepted")
	}
}
