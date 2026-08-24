package legacyackmigration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrCapturedCatalogNotFound = errors.New("legacy ACK captured catalog not found")

// CapturedCatalog is the immutable source-of-truth fence retained across the
// destructive manager schema transition from the ACK runtime to Nomad.
type CapturedCatalog struct {
	SessionID           string
	SourceCatalogDigest string
	TargetClusterID     string
	Catalog             Catalog
	CapturedAt          time.Time
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
			captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
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
	err := s.pool.QueryRow(ctx, `
		SELECT session_id, source_catalog_digest, target_cluster_id, catalog, captured_at
		FROM legacy_ack_migration.source_catalogs
		WHERE session_id = $1
	`, sessionID).Scan(
		&captured.SessionID, &captured.SourceCatalogDigest, &captured.TargetClusterID,
		&payload, &captured.CapturedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrCapturedCatalogNotFound, sessionID)
	}
	if err != nil {
		return nil, fmt.Errorf("read captured legacy ACK catalog: %w", err)
	}
	if err := validateCanonicalDigest(captured.SourceCatalogDigest); err != nil {
		return nil, fmt.Errorf("%w: capture session %s has an invalid source digest", ErrTargetMigrationConflict, sessionID)
	}
	if err := json.Unmarshal(payload, &captured.Catalog); err != nil {
		return nil, fmt.Errorf("%w: decode capture session %s: %v", ErrTargetMigrationConflict, sessionID, err)
	}
	actualDigest, err := captured.Catalog.Digest()
	if err != nil {
		return nil, fmt.Errorf("%w: digest capture session %s: %v", ErrTargetMigrationConflict, sessionID, err)
	}
	if actualDigest != captured.SourceCatalogDigest {
		return nil, fmt.Errorf("%w: capture session %s catalog digest does not match", ErrTargetMigrationConflict, sessionID)
	}
	return &captured, nil
}
