package egressauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BindingStore is the shared manager/broker contract for effective sandbox bindings
// and credential source metadata.
type BindingStore interface {
	GetBindings(ctx context.Context, teamID, sandboxID string) (*BindingRecord, error)
	UpsertBindings(ctx context.Context, record *BindingRecord) error
	DeleteBindings(ctx context.Context, teamID, sandboxID string) error
	GetSourcesByRef(ctx context.Context, teamID string, refs []string) (map[string]*CredentialSource, error)
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
	db                 DB
	pool               *pgxpool.Pool
	defaultStorageKind string
	secretCodec        SecretCodec
	vaultResolver      *VaultResolver
}

var ErrCredentialSourceInUse = errors.New("credential source is in use")
var ErrCredentialSourceResolverKindImmutable = errors.New("credential source resolver_kind is immutable")

type RepositoryOption func(*Repository)

func WithDefaultStorageKind(kind string) RepositoryOption {
	return func(r *Repository) {
		r.defaultStorageKind = kind
	}
}

func WithSecretCodec(codec SecretCodec) RepositoryOption {
	return func(r *Repository) {
		r.secretCodec = codec
	}
}

func WithVaultResolver(resolver *VaultResolver) RepositoryOption {
	return func(r *Repository) {
		r.vaultResolver = resolver
	}
}

func NewRepository(pool *pgxpool.Pool, opts ...RepositoryOption) *Repository {
	repo := &Repository{
		db:                 pool,
		pool:               pool,
		defaultStorageKind: CredentialSourceStorageKindEncryptedPG,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(repo)
		}
	}
	if repo.defaultStorageKind == "" {
		repo.defaultStorageKind = CredentialSourceStorageKindEncryptedPG
	}
	return repo
}

func (r *Repository) Pool() *pgxpool.Pool {
	if r == nil {
		return nil
	}
	return r.pool
}

func (r *Repository) GetBindings(ctx context.Context, teamID, sandboxID string) (*BindingRecord, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
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
			sandbox_id,
			team_id,
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
		WHERE team_id = $1 AND sandbox_id = $2
		GROUP BY team_id, sandbox_id
	`, teamID, sandboxID).Scan(
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
	if record.TeamID == "" {
		return fmt.Errorf("team_id is required")
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
		WHERE team_id = $1 AND sandbox_id = $2
	`, record.TeamID, record.SandboxID); err != nil {
		return fmt.Errorf("delete existing bindings: %w", err)
	}

	if len(record.Bindings) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit binding upsert: %w", err)
		}
		return nil
	}

	bindings := make([]credentialBindingRow, 0, len(record.Bindings))
	for _, binding := range record.Bindings {
		projectionJSON, err := json.Marshal(binding.Projection)
		if err != nil {
			return fmt.Errorf("marshal projection for %q: %w", binding.Ref, err)
		}
		cachePolicyJSON, err := json.Marshal(binding.CachePolicy)
		if err != nil {
			return fmt.Errorf("marshal cache policy for %q: %w", binding.Ref, err)
		}
		bindings = append(bindings, credentialBindingRow{
			Ref:           binding.Ref,
			SourceRef:     binding.SourceRef,
			SourceID:      binding.SourceID,
			SourceVersion: binding.SourceVersion,
			Projection:    projectionJSON,
			CachePolicy:   cachePolicyJSON,
		})
	}

	payload, err := json.Marshal(bindings)
	if err != nil {
		return fmt.Errorf("marshal credential binding batch: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		WITH input AS (
			SELECT *
			FROM jsonb_to_recordset($3::jsonb) AS binding(
				ref text,
				source_ref text,
				source_id bigint,
				source_version bigint,
				projection jsonb,
				cache_policy jsonb
			)
		)
		INSERT INTO sandbox_egress_credential_bindings (
			team_id, sandbox_id, ref, source_ref, source_id, source_version, projection, cache_policy, updated_at
		)
		SELECT $1, $2, ref, source_ref, source_id, source_version, projection, cache_policy, $4
		FROM input
	`, record.TeamID, record.SandboxID, payload, record.UpdatedAt); err != nil {
		return fmt.Errorf("insert bindings: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit binding upsert: %w", err)
	}
	return nil
}

func (r *Repository) DeleteBindings(ctx context.Context, teamID, sandboxID string) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if sandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}

	if _, err := r.db.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE team_id = $1 AND sandbox_id = $2
	`, teamID, sandboxID); err != nil {
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

func (r *Repository) GetSourcesByRef(ctx context.Context, teamID string, refs []string) (map[string]*CredentialSource, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if len(refs) == 0 {
		return map[string]*CredentialSource{}, nil
	}
	uniqueRefs := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return nil, fmt.Errorf("source ref is required")
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		uniqueRefs = append(uniqueRefs, ref)
	}
	payload, err := json.Marshal(uniqueRefs)
	if err != nil {
		return nil, fmt.Errorf("marshal source refs: %w", err)
	}
	rows, err := r.db.Query(ctx, `
		WITH refs AS (
			SELECT value AS name
			FROM jsonb_array_elements_text($2::jsonb)
		)
		SELECT s.id, s.team_id, s.name, s.resolver_kind, s.current_version, s.status, s.created_at, s.updated_at
		FROM credential_sources s
		JOIN refs ON refs.name = s.name
		WHERE s.team_id = $1
		ORDER BY s.name
	`, teamID, payload)
	if err != nil {
		return nil, fmt.Errorf("get sources by ref: %w", err)
	}
	defer rows.Close()

	sources := make(map[string]*CredentialSource, len(uniqueRefs))
	for rows.Next() {
		source := &CredentialSource{}
		if err := rows.Scan(
			&source.ID,
			&source.TeamID,
			&source.Name,
			&source.ResolverKind,
			&source.CurrentVersion,
			&source.Status,
			&source.CreatedAt,
			&source.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan source by ref: %w", err)
		}
		sources[source.Name] = source
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sources by ref: %w", err)
	}
	return sources, nil
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
		source         CredentialSourceVersion
		specJSON       []byte
		storagePayload []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT v.source_id, s.team_id, v.version, v.resolver_kind, v.storage_kind, v.spec_json, v.storage_payload, v.created_at
		FROM credential_source_versions v
		JOIN credential_sources s ON s.id = v.source_id
		WHERE v.source_id = $1 AND v.version = $2
	`, sourceID, version).Scan(
		&source.SourceID,
		&source.TeamID,
		&source.Version,
		&source.ResolverKind,
		&source.StorageKind,
		&specJSON,
		&storagePayload,
		&source.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get source version: %w", err)
	}
	spec, externalRef, err := r.resolveSourceVersionSpec(ctx, &source, specJSON, storagePayload)
	if err != nil {
		return nil, err
	}
	source.Spec = spec
	source.ExternalRef = externalRef
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
		SELECT s.name, s.resolver_kind, v.storage_kind, s.current_version, s.status, v.spec_json, s.created_at, s.updated_at
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

	out := make([]CredentialSourceMetadata, 0)
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
		SELECT s.name, s.resolver_kind, v.storage_kind, s.current_version, s.status, v.spec_json, s.created_at, s.updated_at
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1 AND s.name = $2
	`, teamID, name).Scan(
		&record.Name,
		&record.ResolverKind,
		&record.StorageKind,
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

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin source upsert transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		sourceID             int64
		currentVersion       int64
		existingResolverKind string
		existingSource       bool
	)
	err = tx.QueryRow(ctx, `
		SELECT id, current_version, resolver_kind
		FROM credential_sources
		WHERE team_id = $1 AND name = $2
		FOR UPDATE
	`, teamID, record.Name).Scan(&sourceID, &currentVersion, &existingResolverKind)
	switch err {
	case nil:
		existingSource = true
		if existingResolverKind != record.ResolverKind {
			return nil, fmt.Errorf("%w: existing %q, requested %q", ErrCredentialSourceResolverKindImmutable, existingResolverKind, record.ResolverKind)
		}
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

	storageKind := normalizeStorageKind(record.StorageKind, r.defaultStorageKind)
	specJSON, storagePayload, err := r.prepareSourceVersionPayload(ctx, teamID, sourceID, currentVersion, record.ResolverKind, storageKind, record)
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO credential_source_versions (source_id, version, resolver_kind, spec_json, storage_kind, storage_payload)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, sourceID, currentVersion, record.ResolverKind, specJSON, storageKind, storagePayload); err != nil {
		return nil, fmt.Errorf("insert source version: %w", err)
	}
	if existingSource {
		if _, err := tx.Exec(ctx, `
			UPDATE sandbox_egress_credential_bindings
			SET source_version = $2,
			    updated_at = NOW()
			WHERE source_id = $1
		`, sourceID, currentVersion); err != nil {
			return nil, fmt.Errorf("advance source bindings: %w", err)
		}
		event := pubsub.CredentialSourceRotatedEvent{
			EventBase:     pubsub.NewEventBase(nil),
			TeamID:        teamID,
			SourceID:      sourceID,
			SourceRef:     record.Name,
			SourceVersion: currentVersion,
		}
		if err := notifyCredentialSourceRotated(ctx, tx, event); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit source upsert: %w", err)
	}
	return r.GetSourceMetadata(ctx, teamID, record.Name)
}

func notifyCredentialSourceRotated(ctx context.Context, tx pgx.Tx, event pubsub.CredentialSourceRotatedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal credential source rotation event: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", pubsub.CredentialSourceRotationChannel, string(payload)); err != nil {
		return fmt.Errorf("notify credential source rotation: %w", err)
	}
	return nil
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

type credentialBindingRow struct {
	Ref           string          `json:"ref"`
	SourceRef     string          `json:"source_ref"`
	SourceID      int64           `json:"source_id"`
	SourceVersion int64           `json:"source_version"`
	Projection    json.RawMessage `json:"projection"`
	CachePolicy   json.RawMessage `json:"cache_policy"`
}

func (r *Repository) scanSourceMetadata(rows pgx.Rows) (*CredentialSourceMetadata, error) {
	var (
		record   CredentialSourceMetadata
		specJSON []byte
	)
	if err := rows.Scan(
		&record.Name,
		&record.ResolverKind,
		&record.StorageKind,
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

func (r *Repository) resolveSourceVersionSpec(ctx context.Context, source *CredentialSourceVersion, specJSON, storagePayload []byte) (CredentialSourceSecretSpec, *CredentialSourceExternalRefSpec, error) {
	if source == nil {
		return CredentialSourceSecretSpec{}, nil, fmt.Errorf("credential source version is required")
	}
	switch normalizeStorageKind(source.StorageKind, "") {
	case CredentialSourceStorageKindPlaintextPG, "":
		var spec CredentialSourceSecretSpec
		if len(specJSON) > 0 {
			if err := json.Unmarshal(specJSON, &spec); err != nil {
				return CredentialSourceSecretSpec{}, nil, fmt.Errorf("unmarshal plaintext source spec: %w", err)
			}
		}
		return spec, nil, nil
	case CredentialSourceStorageKindEncryptedPG:
		if r.secretCodec == nil {
			return CredentialSourceSecretSpec{}, nil, fmt.Errorf("encrypted_pg credential source storage is not configured")
		}
		spec, err := r.secretCodec.Decrypt(ctx, credentialSourceAAD(source.TeamID, source.SourceID, source.Version, source.ResolverKind), storagePayload)
		if err != nil {
			return CredentialSourceSecretSpec{}, nil, err
		}
		return spec, nil, nil
	case CredentialSourceStorageKindHashiCorpVault:
		if r.vaultResolver == nil {
			return CredentialSourceSecretSpec{}, nil, fmt.Errorf("hashicorp_vault credential source storage is not configured")
		}
		ref, err := decodeExternalRef(storagePayload)
		if err != nil {
			return CredentialSourceSecretSpec{}, nil, err
		}
		spec, err := r.vaultResolver.Resolve(ctx, source.TeamID, source.ResolverKind, ref)
		if err != nil {
			return CredentialSourceSecretSpec{}, nil, err
		}
		return spec, ref, nil
	case CredentialSourceStorageKindExternalRef:
		if r.vaultResolver == nil {
			return CredentialSourceSecretSpec{}, nil, fmt.Errorf("external_ref credential source storage is not configured")
		}
		ref, err := decodeExternalRef(storagePayload)
		if err != nil {
			return CredentialSourceSecretSpec{}, nil, err
		}
		switch ref.Provider {
		case CredentialSourceExternalProviderHashiCorpVault:
			spec, err := r.vaultResolver.Resolve(ctx, source.TeamID, source.ResolverKind, ref)
			if err != nil {
				return CredentialSourceSecretSpec{}, nil, err
			}
			return spec, ref, nil
		default:
			return CredentialSourceSecretSpec{}, nil, fmt.Errorf("external credential source provider %q is not supported", ref.Provider)
		}
	default:
		return CredentialSourceSecretSpec{}, nil, fmt.Errorf("credential source storage kind %q is not supported", source.StorageKind)
	}
}

func (r *Repository) prepareSourceVersionPayload(ctx context.Context, teamID string, sourceID, version int64, resolverKind, storageKind string, record *CredentialSourceWriteRequest) ([]byte, []byte, error) {
	switch storageKind {
	case CredentialSourceStorageKindEncryptedPG:
		if r.secretCodec == nil {
			return nil, nil, fmt.Errorf("encrypted_pg credential source storage is not configured")
		}
		payload, err := r.secretCodec.Encrypt(ctx, credentialSourceAAD(teamID, sourceID, version, resolverKind), record.Spec)
		if err != nil {
			return nil, nil, err
		}
		return []byte(`{}`), payload, nil
	case CredentialSourceStorageKindHashiCorpVault:
		if r.vaultResolver == nil {
			return nil, nil, fmt.Errorf("hashicorp_vault credential source storage is not configured")
		}
		ref := record.ExternalRef
		if ref == nil {
			if !credentialSourceSpecPresent(record.Spec) {
				return nil, nil, fmt.Errorf("hashicorp_vault credential source requires spec or externalRef")
			}
			ref = defaultVaultSourceRef(teamID, record.Name, version)
		}
		ref.Provider = CredentialSourceExternalProviderHashiCorpVault
		if ref.Connection == "" {
			ref.Connection = "default"
		}
		if ref.Mount == "" {
			ref.Mount = "secret"
		}
		if credentialSourceSpecPresent(record.Spec) {
			if err := r.vaultResolver.Put(ctx, teamID, resolverKind, ref, record.Spec); err != nil {
				return nil, nil, err
			}
		}
		payload, err := json.Marshal(ref)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal vault source reference: %w", err)
		}
		return []byte(`{}`), payload, nil
	case CredentialSourceStorageKindExternalRef:
		if record.ExternalRef == nil {
			return nil, nil, fmt.Errorf("external_ref credential source requires externalRef")
		}
		payload, err := json.Marshal(record.ExternalRef)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal external source reference: %w", err)
		}
		return []byte(`{}`), payload, nil
	case CredentialSourceStorageKindPlaintextPG:
		specJSON, err := json.Marshal(record.Spec)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal source record spec: %w", err)
		}
		return specJSON, []byte(`{}`), nil
	default:
		return nil, nil, fmt.Errorf("credential source storage kind %q is not supported", storageKind)
	}
}

func defaultVaultSourceRef(teamID, name string, version int64) *CredentialSourceExternalRefSpec {
	return &CredentialSourceExternalRefSpec{
		Provider:   CredentialSourceExternalProviderHashiCorpVault,
		Connection: "default",
		Mount:      "secret",
		Path:       fmt.Sprintf("sandbox0/credential-sources/%s/%s/%d", cleanVaultPath(teamID), cleanVaultPath(name), version),
	}
}

func decodeExternalRef(payload []byte) (*CredentialSourceExternalRefSpec, error) {
	var ref CredentialSourceExternalRefSpec
	if err := json.Unmarshal(payload, &ref); err != nil {
		return nil, fmt.Errorf("unmarshal external source reference: %w", err)
	}
	if ref.Provider == "" {
		ref.Provider = CredentialSourceExternalProviderHashiCorpVault
	}
	return &ref, nil
}

func normalizeStorageKind(kind, defaultKind string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = strings.TrimSpace(defaultKind)
	}
	if kind == "" {
		return CredentialSourceStorageKindEncryptedPG
	}
	return kind
}

func credentialSourceSpecPresent(spec CredentialSourceSecretSpec) bool {
	return spec.StaticHeaders != nil ||
		spec.StaticTLSClientCertificate != nil ||
		spec.StaticUsernamePassword != nil ||
		spec.StaticSSHPrivateKey != nil
}

func normalizeSourceStatus(status string) string {
	if status == "" {
		return "active"
	}
	return status
}
