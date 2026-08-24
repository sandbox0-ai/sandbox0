package pg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	templatemigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
	"github.com/stretchr/testify/require"
)

func TestNomadBlockCOWTemplateFreshSchemaIntegration(t *testing.T) {
	ctx := context.Background()
	pool, schema := newTemplateMigrationIntegrationPool(t)
	require.NoError(t, migrateTemplateSchema(ctx, pool, schema))
	assertFinalTemplateSchema(t, ctx, pool)
}

func TestNomadBlockCOWTemplateTerminalCutoverIntegration(t *testing.T) {
	ctx := context.Background()
	pool, schema := newTemplateMigrationIntegrationPool(t)
	applyTemplateBaselineOnly(t, ctx, pool, schema)
	prepareLegacyTemplateSchema(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO scheduler_templates (
			template_id, scope, team_id, spec, creation_stage, creation_output_image
		) VALUES ('completed-template', 'public', '',
			'{"mainContainer":{"image":"registry.invalid/retained@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}',
			'reconciling',
			'registry.invalid/retired@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
	`)
	require.NoError(t, err)
	require.NoError(t, migrateTemplateSchema(ctx, pool, schema))
	assertFinalTemplateSchema(t, ctx, pool)

	var stage string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT creation_stage FROM scheduler_templates
		WHERE scope = 'public' AND team_id = '' AND template_id = 'completed-template'
	`).Scan(&stage))
	require.Equal(t, "publishing", stage)
}

func TestNomadBlockCOWTemplateTerminalCutoverRejectsMutableTemplateImageIntegration(t *testing.T) {
	ctx := context.Background()
	pool, schema := newTemplateMigrationIntegrationPool(t)
	applyTemplateBaselineOnly(t, ctx, pool, schema)
	prepareLegacyTemplateSchema(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO scheduler_templates (template_id, scope, team_id, spec)
		VALUES ('mutable-template', 'public', '',
			'{"mainContainer":{"image":"registry.invalid/runtime:latest"}}')
	`)
	require.NoError(t, err)
	err = migrateTemplateSchema(ctx, pool, schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "digest-pinned SHA-256 OCI input")
}

func TestNomadBlockCOWTemplateTerminalCutoverRejectsLegacyPublicationIntegration(t *testing.T) {
	ctx := context.Background()
	pool, schema := newTemplateMigrationIntegrationPool(t)
	applyTemplateBaselineOnly(t, ctx, pool, schema)
	prepareLegacyTemplateSchema(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO scheduler_template_builds (
			build_id, template_id, scope, team_id, source_sandbox_id,
			target_cluster_id, request_hash, stage, capture_metadata, output_image
		) VALUES ($1, 'unsafe-template', 'team', 'team-1', 'sandbox-1',
			'cluster-1', $2, 'reconciling', '{"version":1}', 'registry.invalid/unsafe')
	`, uuid.New(), strings.Repeat("a", 64))
	require.NoError(t, err)

	err = migrateTemplateSchema(ctx, pool, schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "durable version-2 capture state")
}

func newTemplateMigrationIntegrationPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	schema := "template_migration_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: databaseURL, Schema: schema, DefaultMaxConns: 4,
	})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	})
	return pool, schema
}

func migrateTemplateSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	return migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(templatemigrations.FS),
		migrate.WithSchema(schema),
		migrate.WithTableName("goose_template_migration_test"),
	)
}

func applyTemplateBaselineOnly(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) {
	t.Helper()
	data, err := templatemigrations.FS.ReadFile("00001_nomad_block_cow_baseline.sql")
	require.NoError(t, err)
	require.NoError(t, migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(fstest.MapFS{
			"00001_nomad_block_cow_baseline.sql": {Data: data, Mode: 0o444},
		}),
		migrate.WithSchema(schema),
		migrate.WithTableName("goose_template_migration_test"),
	))
}

func prepareLegacyTemplateSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		ALTER TABLE scheduler_templates
			DROP CONSTRAINT scheduler_templates_image_digest_check,
			DROP CONSTRAINT scheduler_templates_creation_stage_check,
			ADD CONSTRAINT scheduler_templates_creation_stage_check
				CHECK (creation_stage IS NULL OR creation_stage IN ('capturing', 'publishing', 'reconciling')),
			ADD COLUMN creation_output_image TEXT;

		DROP INDEX scheduler_template_builds_takeover_claim;
		ALTER TABLE scheduler_template_builds
			DROP CONSTRAINT scheduler_template_builds_stage_check,
			DROP CONSTRAINT scheduler_template_builds_capture_version_check,
			ADD CONSTRAINT scheduler_template_builds_stage_check
				CHECK (stage IN ('capturing', 'publishing', 'reconciling')),
			ADD COLUMN output_image TEXT;
		CREATE INDEX scheduler_template_builds_takeover_claim
			ON scheduler_template_builds(next_attempt_at, created_at)
			WHERE status IN ('queued', 'running')
			  AND stage IN ('publishing', 'reconciling')
			  AND cancel_requested_at IS NULL;

		CREATE TABLE scheduler_template_allocations (
			template_id TEXT NOT NULL,
			cluster_id TEXT NOT NULL
		);
	`)
	require.NoError(t, err)
}

func assertFinalTemplateSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, identity := range []string{
		"scheduler_templates.creation_output_image",
		"scheduler_template_builds.output_image",
	} {
		parts := strings.Split(identity, ".")
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema()
				  AND table_name = $1 AND column_name = $2
			)
		`, parts[0], parts[1]).Scan(&exists))
		require.False(t, exists, "legacy column %s still exists", identity)
	}

	var allocationsExist bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT to_regclass(current_schema() || '.scheduler_template_allocations') IS NOT NULL
	`).Scan(&allocationsExist))
	require.False(t, allocationsExist)

	_, err := pool.Exec(ctx, `
		INSERT INTO scheduler_templates (template_id, scope, team_id, spec)
		VALUES ('mutable-template', 'public', '',
			'{"mainContainer":{"image":"registry.invalid/runtime:latest"}}')
	`)
	require.Error(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO scheduler_template_builds (
			build_id, template_id, scope, team_id, source_sandbox_id,
			target_cluster_id, request_hash, stage
		) VALUES ($1, 'rejected-template', 'team', 'team-1', 'sandbox-1',
			'cluster-1', $2, 'reconciling')
	`, uuid.New(), strings.Repeat("b", 64))
	require.Error(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO scheduler_template_builds (
			build_id, template_id, scope, team_id, source_sandbox_id,
			target_cluster_id, request_hash, stage, capture_metadata
		) VALUES ($1, 'rejected-metadata', 'team', 'team-1', 'sandbox-1',
			'cluster-1', $2, 'publishing', '{"version":1}')
	`, uuid.New(), strings.Repeat("c", 64))
	require.Error(t, err)
}
