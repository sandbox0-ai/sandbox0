package egressauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BindingStore is the shared manager/broker contract for effective sandbox bindings
// and credential source metadata.
type BindingStore interface {
	GetBindings(ctx context.Context, clusterID, sandboxID string) (*BindingRecord, error)
	UpsertBindings(ctx context.Context, record *BindingRecord) error
	DeleteBindings(ctx context.Context, clusterID, sandboxID string) error
	GetSourceByRef(ctx context.Context, teamID, ref string) (*CredentialSource, error)
	GetSourceVersion(ctx context.Context, sourceID, version int64) (*CredentialSourceVersion, error)
}

// SourceStore owns control-plane CRUD for credential sources.
type SourceStore interface {
	ListSourceMetadata(ctx context.Context, teamID string) ([]CredentialSourceMetadata, error)
	GetSourceMetadata(ctx context.Context, teamID, name string) (*CredentialSourceMetadata, error)
	PutSource(ctx context.Context, teamID string, record *CredentialSourceWriteRequest) (*CredentialSourceMetadata, error)
	DeleteSource(ctx context.Context, teamID, name string) error
}

// Repository persists effective credential bindings in PostgreSQL.
type Repository struct {
	db   DB
	pool *pgxpool.Pool
}

var ErrCredentialSourceInUse = errors.New("credential source is in use")

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		db:   pool,
		pool: pool,
	}
}

func (r *Repository) Pool() *pgxpool.Pool {
	if r == nil {
		return nil
	}
	return r.pool
}

func (r *Repository) GetBindings(ctx context.Context, clusterID, sandboxID string) (*BindingRecord, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if clusterID == "" {
		return nil, fmt.Errorf("cluster_id is required")
	}
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}

	var (
		record       BindingRecord
		bindingsJSON []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT
			cluster_id,
			sandbox_id,
			MIN(team_id) AS team_id,
			COALESCE(
				jsonb_agg(
					jsonb_build_object(
						'ref', ref,
						'sourceRef', source_ref,
						'sourceId', source_id,
						'sourceVersion', source_version,
						'projection', projection,
						'cachePolicy', cache_policy
					) ORDER BY ref
				),
				'[]'::jsonb
			) AS bindings,
			MAX(updated_at) AS updated_at
		FROM sandbox_egress_credential_bindings
		WHERE cluster_id = $1 AND sandbox_id = $2
		GROUP BY cluster_id, sandbox_id
	`, clusterID, sandboxID).Scan(
		&record.ClusterID,
		&record.SandboxID,
		&record.TeamID,
		&bindingsJSON,
		&record.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get bindings: %w", err)
	}
	if len(bindingsJSON) > 0 {
		if err := json.Unmarshal(bindingsJSON, &record.Bindings); err != nil {
			return nil, fmt.Errorf("unmarshal bindings: %w", err)
		}
	}
	return &record, nil
}

func (r *Repository) UpsertBindings(ctx context.Context, record *BindingRecord) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if record == nil {
		return fmt.Errorf("binding record is nil")
	}
	if record.ClusterID == "" {
		return fmt.Errorf("cluster_id is required")
	}
	if record.SandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}

	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now().UTC()
	}

	if r.pool == nil {
		return fmt.Errorf("binding repository pool is not configured")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin binding upsert transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE cluster_id = $1 AND sandbox_id = $2
	`, record.ClusterID, record.SandboxID); err != nil {
		return fmt.Errorf("delete existing bindings: %w", err)
	}

	for _, binding := range record.Bindings {
		projectionJSON, err := json.Marshal(binding.Projection)
		if err != nil {
			return fmt.Errorf("marshal projection for %q: %w", binding.Ref, err)
		}
		cachePolicyJSON, err := json.Marshal(binding.CachePolicy)
		if err != nil {
			return fmt.Errorf("marshal cache policy for %q: %w", binding.Ref, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO sandbox_egress_credential_bindings (
				cluster_id, sandbox_id, team_id, ref, source_ref, source_id, source_version, projection, cache_policy, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`, record.ClusterID, record.SandboxID, record.TeamID, binding.Ref, binding.SourceRef, binding.SourceID, binding.SourceVersion, projectionJSON, cachePolicyJSON, record.UpdatedAt); err != nil {
			return fmt.Errorf("insert binding %q: %w", binding.Ref, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit binding upsert: %w", err)
	}
	return nil
}

func (r *Repository) DeleteBindings(ctx context.Context, clusterID, sandboxID string) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if clusterID == "" {
		return fmt.Errorf("cluster_id is required")
	}
	if sandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}

	if _, err := r.db.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE cluster_id = $1 AND sandbox_id = $2
	`, clusterID, sandboxID); err != nil {
		return fmt.Errorf("delete bindings: %w", err)
	}
	return nil
}

func (r *Repository) GetSourceByRef(ctx context.Context, teamID, ref string) (*CredentialSource, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if ref == "" {
		return nil, fmt.Errorf("source ref is required")
	}

	var source CredentialSource
	err := r.db.QueryRow(ctx, `
		SELECT id, team_id, name, resolver_kind, current_version, status, created_at, updated_at
		FROM credential_sources
		WHERE team_id = $1 AND name = $2
	`, teamID, ref).Scan(
		&source.ID,
		&source.TeamID,
		&source.Name,
		&source.ResolverKind,
		&source.CurrentVersion,
		&source.Status,
		&source.CreatedAt,
		&source.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get source by ref: %w", err)
	}
	return &source, nil
}

func (r *Repository) GetSourceVersion(ctx context.Context, sourceID, version int64) (*CredentialSourceVersion, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if sourceID <= 0 {
		return nil, fmt.Errorf("source_id is required")
	}
	if version <= 0 {
		return nil, fmt.Errorf("version is required")
	}

	var (
		source   CredentialSourceVersion
		specJSON []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT v.source_id, v.version, s.resolver_kind, v.spec_json, v.created_at
		FROM credential_source_versions v
		JOIN credential_sources s ON s.id = v.source_id
		WHERE v.source_id = $1 AND v.version = $2
	`, sourceID, version).Scan(
		&source.SourceID,
		&source.Version,
		&source.ResolverKind,
		&specJSON,
		&source.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get source version: %w", err)
	}
	if len(specJSON) > 0 {
		if err := json.Unmarshal(specJSON, &source.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal source spec: %w", err)
		}
	}
	return &source, nil
}

func (r *Repository) ListSourceMetadata(ctx context.Context, teamID string) ([]CredentialSourceMetadata, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}

	rows, err := r.db.Query(ctx, `
		SELECT s.name, s.resolver_kind, s.current_version, s.status, v.spec_json, s.created_at, s.updated_at
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1
		ORDER BY s.name
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("list source records: %w", err)
	}
	defer rows.Close()

	var out []CredentialSourceMetadata
	for rows.Next() {
		record, err := r.scanSourceMetadata(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source records: %w", err)
	}
	return out, nil
}

func (r *Repository) GetSourceMetadata(ctx context.Context, teamID, name string) (*CredentialSourceMetadata, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if name == "" {
		return nil, fmt.Errorf("source name is required")
	}

	var (
		record   CredentialSourceMetadata
		specJSON []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT s.name, s.resolver_kind, s.current_version, s.status, v.spec_json, s.created_at, s.updated_at
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1 AND s.name = $2
	`, teamID, name).Scan(
		&record.Name,
		&record.ResolverKind,
		&record.CurrentVersion,
		&record.Status,
		&specJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get source record: %w", err)
	}
	return &record, nil
}

func (r *Repository) PutSource(ctx context.Context, teamID string, record *CredentialSourceWriteRequest) (*CredentialSourceMetadata, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("binding repository pool is not configured")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if record == nil {
		return nil, fmt.Errorf("source record is required")
	}
	if record.Name == "" {
		return nil, fmt.Errorf("source name is required")
	}
	if record.ResolverKind == "" {
		return nil, fmt.Errorf("resolver_kind is required")
	}

	specJSON, err := json.Marshal(record.Spec)
	if err != nil {
		return nil, fmt.Errorf("marshal source record spec: %w", err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin source upsert transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		sourceID       int64
		currentVersion int64
	)
	err = tx.QueryRow(ctx, `
		SELECT id, current_version
		FROM credential_sources
		WHERE team_id = $1 AND name = $2
		FOR UPDATE
	`, teamID, record.Name).Scan(&sourceID, &currentVersion)
	switch err {
	case nil:
		currentVersion++
		if _, err := tx.Exec(ctx, `
			UPDATE credential_sources
			SET resolver_kind = $3,
			    current_version = $4,
			    status = $5
			WHERE team_id = $1 AND name = $2
			`, teamID, record.Name, record.ResolverKind, currentVersion, normalizeSourceStatus("")); err != nil {
			return nil, fmt.Errorf("update source record: %w", err)
		}
	case pgx.ErrNoRows:
		currentVersion = 1
		err = tx.QueryRow(ctx, `
			INSERT INTO credential_sources (team_id, name, resolver_kind, current_version, status)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id
		`, teamID, record.Name, record.ResolverKind, currentVersion, normalizeSourceStatus("")).Scan(&sourceID)
		if err != nil {
			return nil, fmt.Errorf("insert source record: %w", err)
		}
	default:
		return nil, fmt.Errorf("load source record for update: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO credential_source_versions (source_id, version, spec_json)
		VALUES ($1, $2, $3)
	`, sourceID, currentVersion, specJSON); err != nil {
		return nil, fmt.Errorf("insert source version: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit source upsert: %w", err)
	}
	return r.GetSourceMetadata(ctx, teamID, record.Name)
}

func (r *Repository) DeleteSource(ctx context.Context, teamID, name string) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if name == "" {
		return fmt.Errorf("source name is required")
	}

	if _, err := r.db.Exec(ctx, `
		DELETE FROM credential_sources
		WHERE team_id = $1 AND name = $2
	`, teamID, name); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrCredentialSourceInUse
		}
		return fmt.Errorf("delete source record: %w", err)
	}
	return nil
}

func (r *Repository) scanSourceMetadata(rows pgx.Rows) (*CredentialSourceMetadata, error) {
	var (
		record   CredentialSourceMetadata
		specJSON []byte
	)
	if err := rows.Scan(
		&record.Name,
		&record.ResolverKind,
		&record.CurrentVersion,
		&record.Status,
		&specJSON,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scan source record: %w", err)
	}
	return &record, nil
}

func normalizeSourceStatus(status string) string {
	if status == "" {
		return "active"
	}
	return status
}
