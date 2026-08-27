package sandboxstore

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/stretchr/testify/require"
)

func TestNomadBlockCOWFreshSchemaIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)

	assertFinalNomadBlockCOWSchema(t, ctx, pool)
}

func TestNomadBlockCOWTerminalCutoverIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationDatabase(t)
	prepareSandboxStoreCredentialSchema(t, pool)
	applySandboxStoreBaselineOnly(t, ctx, pool)
	prepareMixedRuntimeSchemaForCutover(t, ctx, pool)

	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	assertFinalNomadBlockCOWSchema(t, ctx, pool)
}

func TestNomadBlockCOWTerminalCutoverRejectsLiveKubernetesSandboxIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationDatabase(t)
	prepareSandboxStoreCredentialSchema(t, pool)
	applySandboxStoreBaselineOnly(t, ctx, pool)
	prepareMixedRuntimeSchemaForCutover(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandboxes (
			sandbox_id, team_id, template_id, template_name, template_namespace,
			runtime_backend, desired_state, resource_millicpu, resource_memory_mib
		) VALUES ('unsafe-kubernetes-sandbox', 'team-1', 'template-1',
			'template-1', 'default', 'kubernetes', 'active', 1000, 1024)
	`)
	require.NoError(t, err)

	err = RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "every live sandbox runtime_backend to be nomad")
}

func TestNomadBlockCOWTerminalCutoverRejectsMissingResourceLeaseTruthIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationDatabase(t)
	prepareSandboxStoreCredentialSchema(t, pool)
	applySandboxStoreBaselineOnly(t, ctx, pool)
	prepareMixedRuntimeSchemaForCutover(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandboxes (
			sandbox_id, team_id, template_id, template_name, template_namespace,
			runtime_backend, desired_state
		) VALUES ('unsafe-metering-sandbox', 'team-1', 'template-1',
			'template-1', 'default', 'nomad', 'paused')
	`)
	require.NoError(t, err)

	err = RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "positive resource lease metering truth")
}

func TestNomadBlockCOWTerminalCutoverRejectsIncompleteBaseArtifactTruthIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationDatabase(t)
	prepareSandboxStoreCredentialSchema(t, pool)
	applySandboxStoreBaselineOnly(t, ctx, pool)
	prepareMixedRuntimeSchemaForCutover(t, ctx, pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.rootfs_base_artifacts (
			artifact_digest, source_oci_ref, source_oci_digest, base_block_root,
			format_generation, state, descriptor
		) VALUES (
			'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			'registry.invalid/runtime@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
			'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
			'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
			1, 'ready', '\x01'
		)
	`)
	require.NoError(t, err)

	err = RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "exact platform, procd, and logical-size truth")
}

func applySandboxStoreBaselineOnly(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	data, err := storemigrations.FS.ReadFile("00001_nomad_block_cow_baseline.sql")
	require.NoError(t, err)
	require.NoError(t, migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(fstest.MapFS{
			"00001_nomad_block_cow_baseline.sql": {Data: data, Mode: 0o444},
		}),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}),
		migrate.WithSchema(sandboxStoreSchemaName),
	))
}

func prepareMixedRuntimeSchemaForCutover(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		DROP INDEX manager.idx_sandbox_lifecycle_txns_recovery_due;
		ALTER TABLE manager.sandbox_lifecycle_txns
			DROP CONSTRAINT sandbox_lifecycle_txns_recovery_last_error_check,
			DROP CONSTRAINT sandbox_lifecycle_txns_recovery_claim_check,
			DROP CONSTRAINT sandbox_lifecycle_txns_recovery_attempts_check,
			DROP COLUMN recovery_last_error,
			DROP COLUMN recovery_claimed_until,
			DROP COLUMN recovery_claim_token,
			DROP COLUMN recovery_claimed_by,
			DROP COLUMN recovery_next_attempt_at,
			DROP COLUMN recovery_attempts;

		ALTER TABLE manager.runtime_slots
			DROP CONSTRAINT runtime_slots_resource_lease_claim,
			DROP COLUMN resource_lease_id;
		DROP TABLE manager.runtime_resource_leases;
		DROP TABLE manager.runtime_node_capacities;

		DROP TABLE manager.rootfs_base_artifact_objects;
		DROP TABLE manager.rootfs_import_operation_objects;
		DROP TABLE manager.rootfs_import_operations;
		ALTER TABLE manager.rootfs_base_artifacts
			DROP COLUMN attestation,
			DROP COLUMN manifest_digest,
			DROP COLUMN config_digest,
			DROP COLUMN procd_protocol,
			DROP COLUMN procd_digest,
			DROP COLUMN logical_size_bytes,
			DROP COLUMN descriptor_digest;

		DROP TRIGGER enqueue_nomad_sandbox_metering_from_sandbox ON manager.sandboxes;
		ALTER TABLE manager.sandbox_lifecycle_txns
			DROP CONSTRAINT sandbox_lifecycle_txns_rebase_identity_check;

		ALTER TABLE manager.sandboxes
			RENAME COLUMN runtime_namespace TO current_pod_namespace;
		ALTER TABLE manager.sandboxes
			RENAME COLUMN runtime_id TO current_pod_name;
		ALTER TABLE manager.sandboxes
			DROP CONSTRAINT sandboxes_resource_millicpu_check,
			DROP CONSTRAINT sandboxes_resource_memory_mib_check,
			ALTER COLUMN resource_millicpu SET DEFAULT 0,
			ALTER COLUMN resource_memory_mib SET DEFAULT 0,
			ADD CONSTRAINT sandboxes_resource_millicpu_check CHECK (resource_millicpu >= 0),
			ADD CONSTRAINT sandboxes_resource_memory_mib_check CHECK (resource_memory_mib >= 0);
		ALTER TABLE manager.sandboxes
			ADD COLUMN runtime_backend TEXT NOT NULL DEFAULT 'nomad'
				CHECK (runtime_backend IN ('kubernetes', 'nomad'));

		ALTER TABLE manager.sandbox_lifecycle_txns
			RENAME COLUMN from_runtime_namespace TO from_pod_namespace;
		ALTER TABLE manager.sandbox_lifecycle_txns
			RENAME COLUMN from_runtime_id TO from_pod_name;
		ALTER TABLE manager.sandbox_lifecycle_txns
			RENAME COLUMN to_runtime_namespace TO to_pod_namespace;
		ALTER TABLE manager.sandbox_lifecycle_txns
			RENAME COLUMN to_runtime_id TO to_pod_name;
		ALTER TABLE manager.sandbox_lifecycle_txns
			RENAME COLUMN expected_generation_id TO expected_head_layer_id;
		ALTER TABLE manager.sandbox_lifecycle_txns
			RENAME COLUMN prepared_generation_id TO prepared_head_layer_id;

		ALTER TABLE manager.rootfs_writer_grants
			RENAME COLUMN runtime_namespace TO runtime_pod_namespace;
		ALTER TABLE manager.rootfs_writer_grants
			RENAME COLUMN runtime_id TO runtime_pod_name;
		ALTER TABLE manager.rootfs_writer_grants
			RENAME COLUMN runtime_incarnation_id TO runtime_pod_uid;
		ALTER TABLE manager.rootfs_writer_grants
			RENAME COLUMN consumer_agent_uid TO consumer_ctld_pod_uid;
		ALTER TABLE manager.rootfs_writer_grants
			ADD COLUMN initial_head_layer_id TEXT NOT NULL DEFAULT '';

		ALTER TABLE manager.rootfs_filesystems
			DROP CONSTRAINT rootfs_filesystems_format_generation_check,
			ADD COLUMN head_layer_id TEXT,
			ADD COLUMN base_image_ref TEXT NOT NULL DEFAULT '',
			ADD COLUMN base_image_digest TEXT NOT NULL DEFAULT '',
			ADD COLUMN storage_format TEXT NOT NULL DEFAULT 'block-cow-v1';
		ALTER TABLE manager.rootfs_snapshots ADD COLUMN head_layer_id TEXT;

		ALTER TABLE manager.rootfs_base_artifacts
			ALTER COLUMN oci_os DROP NOT NULL,
			ALTER COLUMN oci_architecture DROP NOT NULL,
			ALTER COLUMN oci_variant DROP NOT NULL;

		CREATE TABLE manager.sandbox_rootfs_states (sandbox_id TEXT PRIMARY KEY);
		CREATE TABLE manager.sandbox_rootfs_heads (sandbox_id TEXT PRIMARY KEY);
		CREATE TABLE manager.rootfs_objects (object_key TEXT PRIMARY KEY);
		CREATE TABLE manager.rootfs_layers (layer_id TEXT PRIMARY KEY);
	`)
	require.NoError(t, err)
}

func assertFinalNomadBlockCOWSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	legacyTables := []string{
		"sandbox_rootfs_states", "sandbox_rootfs_heads", "rootfs_objects", "rootfs_layers",
	}
	for _, table := range legacyTables {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT to_regclass('manager.' || $1) IS NOT NULL
		`, table).Scan(&exists))
		require.False(t, exists, "legacy table manager.%s still exists", table)
	}

	legacyColumns := map[string][]string{
		"sandboxes":              {"current_pod_namespace", "current_pod_name", "runtime_backend"},
		"sandbox_lifecycle_txns": {"from_pod_namespace", "from_pod_name", "to_pod_namespace", "to_pod_name", "expected_head_layer_id", "prepared_head_layer_id"},
		"rootfs_filesystems":     {"head_layer_id", "base_image_ref", "base_image_digest", "storage_format"},
		"rootfs_snapshots":       {"head_layer_id"},
		"rootfs_writer_grants":   {"initial_head_layer_id", "runtime_pod_namespace", "runtime_pod_name", "runtime_pod_uid", "consumer_ctld_pod_uid"},
	}
	for table, columns := range legacyColumns {
		for _, column := range columns {
			var exists bool
			require.NoError(t, pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM information_schema.columns
					WHERE table_schema = 'manager' AND table_name = $1 AND column_name = $2
				)
			`, table, column).Scan(&exists))
			require.False(t, exists, "legacy column manager.%s.%s still exists", table, column)
		}
	}

	for _, identity := range []string{
		"sandboxes.runtime_namespace", "sandboxes.runtime_id",
		"sandbox_lifecycle_txns.expected_generation_id",
		"sandbox_lifecycle_txns.prepared_generation_id",
		"rootfs_writer_grants.initial_generation_id",
		"rootfs_materialization_objects.missing_at",
		"rootfs_materialization_objects.last_error",
		"rootfs_materialization_objects.last_audited_at",
		"rootfs_base_artifacts.procd_protocol",
		"rootfs_base_artifacts.procd_digest",
		"rootfs_base_artifacts.logical_size_bytes",
		"runtime_slots.resource_lease_id",
		"sandbox_lifecycle_txns.recovery_attempts",
		"sandbox_lifecycle_txns.recovery_next_attempt_at",
		"sandbox_lifecycle_txns.recovery_claimed_by",
		"sandbox_lifecycle_txns.recovery_claim_token",
		"sandbox_lifecycle_txns.recovery_claimed_until",
		"sandbox_lifecycle_txns.recovery_last_error",
	} {
		parts := strings.Split(identity, ".")
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'manager' AND table_name = $1 AND column_name = $2
			)
		`, parts[0], parts[1]).Scan(&exists))
		require.True(t, exists, "required column manager.%s is missing", identity)
	}

	for _, table := range []string{
		"rootfs_import_operations", "rootfs_import_operation_objects",
		"rootfs_base_artifact_objects", "runtime_node_capacities",
		"runtime_resource_leases",
	} {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT to_regclass('manager.' || $1) IS NOT NULL
		`, table).Scan(&exists))
		require.True(t, exists, "required table manager.%s is missing", table)
	}

	var artifactIndex string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE schemaname = 'manager'
			AND indexname = 'idx_rootfs_base_artifacts_source_platform_ready'
	`).Scan(&artifactIndex))
	require.Contains(t, artifactIndex, "logical_size_bytes")
	require.Contains(t, artifactIndex, "procd_protocol")
	require.Contains(t, artifactIndex, "procd_digest")

	var recoveryIndex string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE schemaname = 'manager'
			AND indexname = 'idx_sandbox_lifecycle_txns_recovery_due'
	`).Scan(&recoveryIndex))
	require.Contains(t, recoveryIndex, "recovery_next_attempt_at")
	require.Contains(t, recoveryIndex, "recovery_claimed_until")

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandboxes (
			sandbox_id, team_id, template_id, template_name, template_namespace,
			desired_state, resource_millicpu, resource_memory_mib
		) VALUES ('zero-resource-truth', 'team-1', 'template-1', 'template-1',
			'default', 'paused', 0, 0)
	`)
	require.Error(t, err)
}
