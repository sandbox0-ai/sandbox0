package legacyackmigration

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
)

func TestCaptureStoreRetiresExactLegacySchemaAndSurvivesCurrentMigrations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, catalog := newLegacyRetirementIntegrationPool(t, ctx)
	store, err := NewCaptureStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := store.CaptureCatalog(ctx, "retirement-session", "ali-ue1-nomad", catalog)
	if err != nil {
		t.Fatal(err)
	}
	retired, err := store.RetireLegacyManagerSchema(ctx, "retirement-session")
	if err != nil {
		t.Fatal(err)
	}
	if retired.RetiredAt.IsZero() || retired.SourceCatalogDigest != captured.SourceCatalogDigest {
		t.Fatalf("retired capture = %#v", retired)
	}
	var managerExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regnamespace('manager') IS NOT NULL`).Scan(&managerExists); err != nil {
		t.Fatal(err)
	}
	if managerExists {
		t.Fatal("legacy manager schema still exists after retirement")
	}
	retry, err := store.RetireLegacyManagerSchema(ctx, "retirement-session")
	if err != nil {
		t.Fatalf("exact retirement retry: %v", err)
	}
	if !retry.RetiredAt.Equal(retired.RetiredAt) {
		t.Fatalf("retirement retry changed marker: %s != %s", retry.RetiredAt, retired.RetiredAt)
	}

	prepareTargetStoreCredentialSchema(t, ctx, pool)
	logger := targetStoreIntegrationLogger{}
	if err := egressauthstore.RunMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	if err := sandboxstore.RunSandboxStoreMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadCapturedCatalog(ctx, "retirement-session")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SourceCatalogDigest != captured.SourceCatalogDigest || !loaded.RetiredAt.Equal(retired.RetiredAt) {
		t.Fatalf("capture after current migrations = %#v", loaded)
	}
	target, err := NewTargetStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := target.EnsureSession(ctx, loaded.SessionID, loaded.SourceCatalogDigest, loaded.TargetClusterID); err != nil {
		t.Fatalf("current target session after exact retirement: %v", err)
	}
}

func TestCaptureStoreRetirementRejectsSourceDriftAndPendingQueues(t *testing.T) {
	t.Run("source drift", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		pool, catalog := newLegacyRetirementIntegrationPool(t, ctx)
		store, err := NewCaptureStore(pool)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.CaptureCatalog(ctx, "drift-session", "ali-ue1-nomad", catalog); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE manager.sandboxes SET updated_at = updated_at + INTERVAL '1 second'
			WHERE sandbox_id = 'sandbox-1'
		`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RetireLegacyManagerSchema(ctx, "drift-session"); !errors.Is(err, ErrTargetMigrationConflict) {
			t.Fatalf("retirement after source drift error = %v", err)
		}
		assertLegacyManagerAndUnretiredCapture(t, ctx, pool, store, "drift-session")
	})

	t.Run("pending queues", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		pool, catalog := newLegacyRetirementIntegrationPool(t, ctx)
		store, err := NewCaptureStore(pool)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.CaptureCatalog(ctx, "queue-session", "ali-ue1-nomad", catalog); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO manager.rootfs_object_deletions DEFAULT VALUES`); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RetireLegacyManagerSchema(ctx, "queue-session"); err == nil ||
			!strings.Contains(err.Error(), "pending RootFS deletions") {
			t.Fatalf("retirement with pending queue error = %v", err)
		}
		assertLegacyManagerAndUnretiredCapture(t, ctx, pool, store, "queue-session")
	})
}

func assertLegacyManagerAndUnretiredCapture(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *CaptureStore,
	sessionID string,
) {
	t.Helper()
	var managerExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regnamespace('manager') IS NOT NULL`).Scan(&managerExists); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadCapturedCatalog(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !managerExists || !loaded.RetiredAt.IsZero() {
		t.Fatalf("manager exists = %t, retired at = %s", managerExists, loaded.RetiredAt)
	}
}

func newLegacyRetirementIntegrationPool(
	t *testing.T,
	ctx context.Context,
) (*pgxpool.Pool, *Catalog) {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: databaseURL, Schema: "scheduler", MaxConns: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, schema := range []string{"legacy_ack_migration", "manager"} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS legacy_ack_migration CASCADE")
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS manager CASCADE")
	})
	if _, err := pool.Exec(ctx, legacyRetirementSchemaSQL); err != nil {
		t.Fatal(err)
	}
	catalog := validCatalog(t)
	insertLegacyRetirementCatalog(t, ctx, pool, &catalog)
	read, err := ReadCatalog(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read.Normalize(testNormalizeOptions()); err != nil {
		t.Fatalf("database fixture is not a valid frozen catalog: %v", err)
	}
	return pool, read
}

func insertLegacyRetirementCatalog(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	catalog *Catalog,
) {
	t.Helper()
	for _, sandbox := range catalog.Sandboxes {
		_, err := pool.Exec(ctx, `
			INSERT INTO manager.sandboxes (
				sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
				cluster_id, desired_state, config, template_spec, runtime_generation,
				lifecycle_epoch, owner_kind, hot_claim_completed_at, claimed_at, expires_at,
				hard_expires_at, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
				NULLIF($14::timestamptz, '-infinity'), NULLIF($15::timestamptz, '-infinity'),
				NULLIF($16::timestamptz, '-infinity'), NULLIF($17::timestamptz, '-infinity'),$18,$19)
		`, sandbox.ID, sandbox.TeamID, sandbox.UserID, sandbox.TemplateID, sandbox.TemplateName,
			sandbox.TemplateNamespace, sandbox.ClusterID, sandbox.DesiredState, sandbox.Config,
			sandbox.TemplateSpec, sandbox.RuntimeGeneration, sandbox.LifecycleEpoch, sandbox.OwnerKind,
			integrationTimeValue(sandbox.HotClaimCompletedAt), integrationTimeValue(sandbox.ClaimedAt),
			integrationTimeValue(sandbox.ExpiresAt), integrationTimeValue(sandbox.HardExpiresAt),
			sandbox.CreatedAt, sandbox.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, layer := range catalog.Layers {
		_, err := pool.Exec(ctx, `
			INSERT INTO manager.rootfs_layers (
				layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
				runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
				snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
				diff_size, diff_object_key, platform_os, platform_architecture, platform_variant, created_at
			) VALUES ($1,NULLIF($2,''),$3,$4,$5,$6,$7,$8,$9,$10,$11,COALESCE($12::jsonb, 'null'::jsonb),$13,$14,$15,$16,$17,$18,$19,$20,$21)
		`, layer.ID, layer.ParentID, layer.SourceSandboxID, layer.TeamID, layer.RuntimeGeneration,
			layer.Runtime, layer.RuntimeHandler, layer.BaseImageRef, layer.BaseImageDigest,
			layer.Snapshotter, layer.SnapshotParent, layer.SnapshotParentChain, layer.DiffDigest,
			layer.DiffID, layer.DiffMediaType, layer.DiffSize, layer.DiffObjectKey,
			layer.PlatformOS, layer.PlatformArchitecture, layer.PlatformVariant, layer.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, filesystem := range catalog.Filesystems {
		_, err := pool.Exec(ctx, `
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, source_filesystem_id, head_layer_id,
				base_image_ref, base_image_digest, created_at, updated_at
			) VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),$5,$6,$7,$8)
		`, filesystem.ID, filesystem.TeamID, filesystem.SourceFilesystemID, filesystem.HeadLayerID,
			filesystem.BaseImageRef, filesystem.BaseImageDigest, filesystem.CreatedAt, filesystem.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, binding := range catalog.Bindings {
		if _, err := pool.Exec(ctx, `
			INSERT INTO manager.sandbox_rootfs_bindings
				(sandbox_id, filesystem_id, team_id, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5)
		`, binding.SandboxID, binding.FilesystemID, binding.TeamID, binding.CreatedAt, binding.UpdatedAt); err != nil {
			t.Fatal(err)
		}
	}
	for _, snapshot := range catalog.Snapshots {
		if _, err := pool.Exec(ctx, `
			INSERT INTO manager.rootfs_snapshots (
				snapshot_id, team_id, source_sandbox_id, head_layer_id, filesystem_id,
				name, description, created_at, expires_at
			) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,NULLIF($9::timestamptz, '-infinity'))
		`, snapshot.ID, snapshot.TeamID, snapshot.SourceSandboxID, snapshot.HeadLayerID,
			snapshot.FilesystemID, snapshot.Name, snapshot.Description, snapshot.CreatedAt,
			integrationTimeValue(snapshot.ExpiresAt)); err != nil {
			t.Fatal(err)
		}
	}
}

func integrationTimeValue(value time.Time) any {
	if value.IsZero() {
		return "-infinity"
	}
	return value
}

func prepareTargetStoreCredentialSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		t.Fatal(err)
	}
}

const legacyRetirementSchemaSQL = `
	CREATE SCHEMA manager;
	CREATE TABLE manager.goose_db_version (
		id BIGSERIAL PRIMARY KEY, version_id BIGINT NOT NULL, is_applied BOOLEAN NOT NULL, tstamp TIMESTAMPTZ DEFAULT NOW()
	);
	INSERT INTO manager.goose_db_version (version_id, is_applied) VALUES (19, TRUE);
	CREATE TABLE manager.sandboxes (
		sandbox_id TEXT PRIMARY KEY, team_id TEXT NOT NULL, user_id TEXT NOT NULL DEFAULT '',
		template_id TEXT NOT NULL, template_name TEXT NOT NULL, template_namespace TEXT NOT NULL,
		cluster_id TEXT NOT NULL DEFAULT '', desired_state TEXT NOT NULL, config JSONB NOT NULL DEFAULT '{}',
		template_spec JSONB NOT NULL DEFAULT '{}', runtime_generation BIGINT NOT NULL DEFAULT 0,
		lifecycle_epoch BIGINT NOT NULL DEFAULT 0, owner_kind TEXT NOT NULL DEFAULT '',
		hot_claim_completed_at TIMESTAMPTZ, claimed_at TIMESTAMPTZ, expires_at TIMESTAMPTZ,
		hard_expires_at TIMESTAMPTZ, deleted_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	);
	CREATE TABLE manager.sandbox_lifecycle_txns (phase TEXT NOT NULL);
	CREATE TABLE manager.rootfs_layers (
		layer_id TEXT PRIMARY KEY, parent_layer_id TEXT, source_sandbox_id TEXT NOT NULL,
		team_id TEXT NOT NULL, runtime_generation BIGINT NOT NULL, runtime TEXT NOT NULL,
		runtime_handler TEXT NOT NULL, base_image_ref TEXT NOT NULL, base_image_digest TEXT NOT NULL,
		snapshotter TEXT NOT NULL, snapshot_parent TEXT NOT NULL, snapshot_parent_chain JSONB NOT NULL,
		diff_digest TEXT NOT NULL, diff_id TEXT NOT NULL, diff_media_type TEXT NOT NULL,
		diff_size BIGINT NOT NULL, diff_object_key TEXT NOT NULL, platform_os TEXT NOT NULL,
		platform_architecture TEXT NOT NULL, platform_variant TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL
	);
	CREATE TABLE manager.rootfs_filesystems (
		filesystem_id TEXT PRIMARY KEY, team_id TEXT NOT NULL, source_filesystem_id TEXT,
		head_layer_id TEXT, base_image_ref TEXT NOT NULL, base_image_digest TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
	);
	CREATE TABLE manager.sandbox_rootfs_bindings (
		sandbox_id TEXT PRIMARY KEY, filesystem_id TEXT NOT NULL, team_id TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
	);
	CREATE TABLE manager.rootfs_snapshots (
		snapshot_id TEXT PRIMARY KEY, team_id TEXT NOT NULL, source_sandbox_id TEXT NOT NULL,
		head_layer_id TEXT NOT NULL, filesystem_id TEXT, name TEXT NOT NULL, description TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ
	);
	CREATE TABLE manager.sandbox_rootfs_states (id BIGINT);
	CREATE TABLE manager.sandbox_rootfs_heads (id BIGINT);
	CREATE TABLE manager.rootfs_objects (id BIGINT);
	CREATE TABLE manager.rootfs_object_deletions (id BIGSERIAL PRIMARY KEY);
	CREATE TABLE manager.sandbox_deletion_webhook_outbox (
		id BIGSERIAL PRIMARY KEY, delivered_at TIMESTAMPTZ, terminal_at TIMESTAMPTZ
	);
`
