package legacyackmigration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrCapturedCatalogNotFound = errors.New("legacy ACK captured catalog not found")
	ErrLegacyCatalogNotRetired = errors.New("legacy ACK manager schema not retired")
)

// CapturedCatalog is the immutable source-of-truth fence retained across the
// destructive manager schema transition from the ACK runtime to Nomad.
type CapturedCatalog struct {
	SessionID           string
	SourceCatalogDigest string
	TargetClusterID     string
	Catalog             Catalog
	CapturedAt          time.Time
	RetiredAt           time.Time
}

// CaptureStore persists the complete frozen ACK catalog outside the manager
// schema. It deliberately has no foreign keys into manager tables, allowing
// current manager migrations to replace the legacy schema after capture.
type CaptureStore struct {
	pool *pgxpool.Pool
}

func NewCaptureStore(pool *pgxpool.Pool) (*CaptureStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("capture PostgreSQL pool is required")
	}
	return &CaptureStore{pool: pool}, nil
}

// EnsureSchema creates only the immutable capture boundary and is safe to run
// while the database still contains the legacy manager schema.
func (s *CaptureStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("legacy ACK capture store is not configured")
	}
	_, err := s.pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS legacy_ack_migration;
		CREATE TABLE IF NOT EXISTS legacy_ack_migration.source_catalogs (
			session_id TEXT PRIMARY KEY CHECK (octet_length(session_id) BETWEEN 1 AND 128),
			source_catalog_digest TEXT NOT NULL CHECK (source_catalog_digest ~ '^sha256:[0-9a-f]{64}$'),
			target_cluster_id TEXT NOT NULL CHECK (octet_length(target_cluster_id) BETWEEN 1 AND 512),
			catalog JSONB NOT NULL CHECK (jsonb_typeof(catalog) = 'object'),
			captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			retired_at TIMESTAMPTZ
		);
		ALTER TABLE legacy_ack_migration.source_catalogs
			ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;
	`)
	if err != nil {
		return fmt.Errorf("create legacy ACK capture schema: %w", err)
	}
	return nil
}

// CaptureCatalog installs one immutable source catalog. An exact retry returns
// the original row; no caller can replace a session with divergent input.
func (s *CaptureStore) CaptureCatalog(
	ctx context.Context,
	sessionID, targetClusterID string,
	catalog *Catalog,
) (*CapturedCatalog, error) {
	if s == nil || s.pool == nil || catalog == nil {
		return nil, fmt.Errorf("legacy ACK capture store and catalog are required")
	}
	sessionID, targetClusterID = strings.TrimSpace(sessionID), strings.TrimSpace(targetClusterID)
	if sessionID == "" || len(sessionID) > 128 || targetClusterID == "" || len(targetClusterID) > 512 {
		return nil, fmt.Errorf("legacy ACK capture session and target cluster identities are invalid")
	}
	payload, err := canonicalCatalogPayload(*catalog)
	if err != nil {
		return nil, err
	}
	digest, err := catalog.Digest()
	if err != nil {
		return nil, err
	}
	if err := s.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO legacy_ack_migration.source_catalogs (
			session_id, source_catalog_digest, target_cluster_id, catalog
		) VALUES ($1, $2, $3, $4::jsonb)
		ON CONFLICT (session_id) DO NOTHING
	`, sessionID, digest, targetClusterID, payload)
	if err != nil {
		return nil, fmt.Errorf("capture frozen legacy ACK catalog: %w", err)
	}
	captured, err := s.LoadCapturedCatalog(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if captured.SourceCatalogDigest != digest || captured.TargetClusterID != targetClusterID {
		return nil, fmt.Errorf("%w: capture session %s has different immutable inputs", ErrTargetMigrationConflict, sessionID)
	}
	return captured, nil
}

// LoadCapturedCatalog verifies the stored JSON against its immutable digest
// before returning it. This detects direct database edits or damaged capture
// state before any target objects become reachable.
func (s *CaptureStore) LoadCapturedCatalog(ctx context.Context, sessionID string) (*CapturedCatalog, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("legacy ACK capture store is not configured")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || len(sessionID) > 128 {
		return nil, fmt.Errorf("legacy ACK capture session identity is invalid")
	}
	var captured CapturedCatalog
	var payload []byte
	err := scanCapturedCatalog(s.pool.QueryRow(ctx, `
		SELECT session_id, source_catalog_digest, target_cluster_id, catalog, captured_at, retired_at
		FROM legacy_ack_migration.source_catalogs
		WHERE session_id = $1
	`, sessionID), &captured, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrCapturedCatalogNotFound, sessionID)
	}
	if err != nil {
		return nil, fmt.Errorf("read captured legacy ACK catalog: %w", err)
	}
	if err := decodeAndVerifyCapturedCatalog(&captured, payload); err != nil {
		return nil, err
	}
	return &captured, nil
}

// RetireLegacyManagerSchema is the one destructive transition in the import.
// It takes exclusive NOWAIT locks, re-reads the frozen source under the same
// transaction, proves that it is the captured catalog, rejects pending queues,
// and only then removes the version-19 manager schema. The independent capture
// row and retirement marker commit atomically with that removal.
func (s *CaptureStore) RetireLegacyManagerSchema(
	ctx context.Context,
	sessionID string,
) (*CapturedCatalog, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("legacy ACK capture store is not configured")
	}
	if err := s.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || len(sessionID) > 128 {
		return nil, fmt.Errorf("legacy ACK capture session identity is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin legacy ACK schema retirement: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var captured CapturedCatalog
	var payload []byte
	if err := scanCapturedCatalog(tx.QueryRow(ctx, `
		SELECT session_id, source_catalog_digest, target_cluster_id, catalog, captured_at, retired_at
		FROM legacy_ack_migration.source_catalogs
		WHERE session_id = $1
		FOR UPDATE
	`, sessionID), &captured, &payload); errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrCapturedCatalogNotFound, sessionID)
	} else if err != nil {
		return nil, fmt.Errorf("lock captured legacy ACK catalog: %w", err)
	}
	if err := decodeAndVerifyCapturedCatalog(&captured, payload); err != nil {
		return nil, err
	}
	if !captured.RetiredAt.IsZero() {
		return &captured, nil
	}
	if _, err := tx.Exec(ctx, `
		LOCK TABLE
			manager.goose_db_version,
			manager.sandboxes,
			manager.sandbox_lifecycle_txns,
			manager.rootfs_layers,
			manager.sandbox_rootfs_states,
			manager.sandbox_rootfs_heads,
			manager.rootfs_filesystems,
			manager.sandbox_rootfs_bindings,
			manager.rootfs_snapshots,
			manager.rootfs_objects,
			manager.rootfs_object_deletions,
			manager.sandbox_deletion_webhook_outbox
		IN ACCESS EXCLUSIVE MODE NOWAIT
	`); err != nil {
		return nil, fmt.Errorf("lock frozen legacy ACK manager schema: %w", err)
	}
	current, err := readCatalogSnapshot(ctx, tx)
	if err != nil {
		return nil, err
	}
	currentDigest, err := current.Digest()
	if err != nil {
		return nil, err
	}
	if currentDigest != captured.SourceCatalogDigest {
		return nil, fmt.Errorf("%w: frozen legacy manager changed after capture", ErrTargetMigrationConflict)
	}
	var pendingObjectDeletions, pendingDeletionWebhooks int64
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_object_deletions`).Scan(&pendingObjectDeletions); err != nil {
		return nil, fmt.Errorf("count legacy RootFS object deletions: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.sandbox_deletion_webhook_outbox
		WHERE delivered_at IS NULL AND terminal_at IS NULL
	`).Scan(&pendingDeletionWebhooks); err != nil {
		return nil, fmt.Errorf("count legacy deletion webhooks: %w", err)
	}
	if pendingObjectDeletions != 0 || pendingDeletionWebhooks != 0 {
		return nil, fmt.Errorf("legacy manager cannot be retired with %d pending RootFS deletions and %d pending deletion webhooks",
			pendingObjectDeletions, pendingDeletionWebhooks)
	}
	if _, err := tx.Exec(ctx, `DROP SCHEMA manager CASCADE`); err != nil {
		return nil, fmt.Errorf("retire captured legacy manager schema: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		UPDATE legacy_ack_migration.source_catalogs
		SET retired_at = clock_timestamp()
		WHERE session_id = $1 AND retired_at IS NULL
		RETURNING retired_at
	`, sessionID).Scan(&captured.RetiredAt); err != nil {
		return nil, fmt.Errorf("mark legacy ACK manager schema retired: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit legacy ACK manager schema retirement: %w", err)
	}
	return &captured, nil
}

type capturedCatalogScanner interface {
	Scan(...any) error
}

func scanCapturedCatalog(row capturedCatalogScanner, captured *CapturedCatalog, payload *[]byte) error {
	var retired sql.NullTime
	if err := row.Scan(
		&captured.SessionID, &captured.SourceCatalogDigest, &captured.TargetClusterID,
		payload, &captured.CapturedAt, &retired,
	); err != nil {
		return err
	}
	if retired.Valid {
		captured.RetiredAt = retired.Time
	}
	return nil
}

func decodeAndVerifyCapturedCatalog(captured *CapturedCatalog, payload []byte) error {
	if captured == nil {
		return fmt.Errorf("%w: captured catalog is missing", ErrTargetMigrationConflict)
	}
	if err := validateCanonicalDigest(captured.SourceCatalogDigest); err != nil {
		return fmt.Errorf("%w: capture session %s has an invalid source digest", ErrTargetMigrationConflict, captured.SessionID)
	}
	if err := json.Unmarshal(payload, &captured.Catalog); err != nil {
		return fmt.Errorf("%w: decode capture session %s: %v", ErrTargetMigrationConflict, captured.SessionID, err)
	}
	actualDigest, err := captured.Catalog.Digest()
	if err != nil {
		return fmt.Errorf("%w: digest capture session %s: %v", ErrTargetMigrationConflict, captured.SessionID, err)
	}
	if actualDigest != captured.SourceCatalogDigest {
		return fmt.Errorf("%w: capture session %s catalog digest does not match", ErrTargetMigrationConflict, captured.SessionID)
	}
	return nil
}
