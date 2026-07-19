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
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
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
	db                 DB
	pool               *pgxpool.Pool
	defaultStorageKind string
	secretCodec        SecretCodec
	vaultResolver      *VaultResolver
	teamQuotaStore     teamquota.CapacityTxStore
}

var ErrCredentialSourceInUse = errors.New("credential source is in use")
var ErrCredentialSourceResolverKindImmutable = errors.New("credential source resolver_kind is immutable")
var ErrCredentialSourceStorageKindImmutable = errors.New("credential source storage_kind is immutable")
var ErrCredentialSourceManagementModeImmutable = errors.New("credential source managed/external mode is immutable")
var ErrCredentialSourceExternalRefReadOnly = errors.New("external credential source references are read-only")
var ErrCredentialSourceLifecycleConflict = errors.New("credential source lifecycle state conflicts with the requested operation")
var ErrCredentialSourceUnavailable = errors.New("credential source is not active")

const (
	credentialSourceControlPlaneObjectTarget int64 = 2
	credentialSourceFinalizeTimeout                = 10 * time.Second

	credentialSourceStatusActive       = "active"
	credentialSourceStatusProvisioning = "provisioning"
	credentialSourceStatusDeleting     = "deleting"
)

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

// WithTeamQuotaStore overrides the team quota store.
func WithTeamQuotaStore(store teamquota.CapacityTxStore) RepositoryOption {
	return func(r *Repository) {
		r.teamQuotaStore = store
	}
}

func NewRepository(pool *pgxpool.Pool, opts ...RepositoryOption) *Repository {
	repo := &Repository{
		db:                 pool,
		pool:               pool,
		defaultStorageKind: CredentialSourceStorageKindEncryptedPG,
		teamQuotaStore:     teamquota.NewRepository(pool),
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
	if err := ValidateBindingRecordSize(record); err != nil {
		return err
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
	quotaRef, err := teamquota.ReserveControlPlaneObjectTargetTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(
			record.TeamID,
			teamquota.ControlPlaneOwnerKindSandboxEgressBindings,
			record.SandboxID,
		),
		"replace_sandbox_egress_bindings",
		int64(len(record.Bindings)),
	)
	if err != nil {
		return err
	}

	for _, binding := range record.Bindings {
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM credential_sources s
				JOIN credential_source_versions v
				  ON v.source_id = s.id AND v.version = s.current_version
				WHERE s.team_id = $1
				  AND s.name = $2
				  AND s.id = $3
				  AND s.current_version = $4
				  AND s.status = $5
				FOR KEY SHARE OF s
			)
		`, record.TeamID, binding.SourceRef, binding.SourceID, binding.SourceVersion, credentialSourceStatusActive).Scan(&active); err != nil {
			return fmt.Errorf("validate binding source %q: %w", binding.SourceRef, err)
		}
		if !active {
			return fmt.Errorf("%w: %q", ErrCredentialSourceUnavailable, binding.SourceRef)
		}
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE team_id = $1 AND sandbox_id = $2
	`, record.TeamID, record.SandboxID); err != nil {
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
				team_id, sandbox_id, ref, source_ref, source_id, source_version, projection, cache_policy, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, record.TeamID, record.SandboxID, binding.Ref, binding.SourceRef, binding.SourceID, binding.SourceVersion, projectionJSON, cachePolicyJSON, record.UpdatedAt); err != nil {
			return fmt.Errorf("insert binding %q: %w", binding.Ref, err)
		}
	}

	if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return err
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

	if r.pool == nil {
		return fmt.Errorf("binding repository pool is not configured")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin binding delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	quotaRef, err := teamquota.BeginControlPlaneObjectReleaseTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(
			teamID,
			teamquota.ControlPlaneOwnerKindSandboxEgressBindings,
			sandboxID,
		),
		"delete_sandbox_egress_bindings",
		0,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM sandbox_egress_credential_bindings
		WHERE team_id = $1 AND sandbox_id = $2
	`, teamID, sandboxID); err != nil {
		return fmt.Errorf("delete bindings: %w", err)
	}
	if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit binding deletion: %w", err)
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
		WHERE team_id = $1 AND name = $2 AND status = $3
	`, teamID, ref, credentialSourceStatusActive).Scan(
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
		source         CredentialSourceVersion
		specJSON       []byte
		storagePayload []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT v.source_id, s.team_id, v.version, v.resolver_kind, v.storage_kind, v.spec_json, v.storage_payload, v.created_at
		FROM credential_source_versions v
		JOIN credential_sources s ON s.id = v.source_id
		WHERE v.source_id = $1 AND v.version = $2 AND s.status = $3
	`, sourceID, version, credentialSourceStatusActive).Scan(
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
	if err := ValidateCredentialSourceWriteSize(record); err != nil {
		return nil, err
	}

	storageKind := normalizeStorageKind(record.StorageKind, r.defaultStorageKind)
	if err := validateCredentialSourceStorageRequest(storageKind, record); err != nil {
		return nil, err
	}
	if storageKind == CredentialSourceStorageKindHashiCorpVault && record.ExternalRef == nil {
		return r.putManagedVaultSource(ctx, teamID, storageKind, record)
	}
	return r.putSourceWithoutManagedExternalWrite(ctx, teamID, storageKind, record)
}

func (r *Repository) putSourceWithoutManagedExternalWrite(
	ctx context.Context,
	teamID, storageKind string,
	record *CredentialSourceWriteRequest,
) (*CredentialSourceMetadata, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin source upsert transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	quotaRef, err := teamquota.ReserveControlPlaneObjectTargetTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(
			teamID,
			teamquota.ControlPlaneOwnerKindCredentialSource,
			record.Name,
		),
		"put_credential_source",
		credentialSourceControlPlaneObjectTarget,
	)
	if err != nil {
		return nil, err
	}
	var (
		sourceID             int64
		currentVersion       int64
		existingResolverKind string
		existingStatus       string
		existingStorageKind  string
		existingPayload      []byte
		existingSource       bool
	)
	err = tx.QueryRow(ctx, `
		SELECT s.id, s.current_version, s.resolver_kind, s.status, v.storage_kind, v.storage_payload
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1 AND s.name = $2
		FOR UPDATE OF s, v
	`, teamID, record.Name).Scan(
		&sourceID,
		&currentVersion,
		&existingResolverKind,
		&existingStatus,
		&existingStorageKind,
		&existingPayload,
	)
	switch err {
	case nil:
		existingSource = true
		if err := validateExistingCredentialSourceMutation(
			existingResolverKind,
			existingStatus,
			existingStorageKind,
			existingPayload,
			record,
			storageKind,
		); err != nil {
			return nil, err
		}
		currentVersion++
	case pgx.ErrNoRows:
		currentVersion = 1
	default:
		return nil, fmt.Errorf("load source record for update: %w", err)
	}

	if !existingSource {
		if err := tx.QueryRow(ctx, `
			INSERT INTO credential_sources (team_id, name, resolver_kind, current_version, status)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id
		`, teamID, record.Name, record.ResolverKind, currentVersion, credentialSourceStatusActive).Scan(&sourceID); err != nil {
			return nil, fmt.Errorf("insert source record: %w", err)
		}
	}

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
			UPDATE credential_sources
			SET current_version = $3,
			    status = $4
			WHERE team_id = $1 AND name = $2
		`, teamID, record.Name, currentVersion, credentialSourceStatusActive); err != nil {
			return nil, fmt.Errorf("update source record: %w", err)
		}
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
	// Bindings advance before old versions are removed, so the source and its
	// current version are the only retained PostgreSQL objects.
	if _, err := tx.Exec(ctx, `
		DELETE FROM credential_source_versions
		WHERE source_id = $1 AND version <> $2
	`, sourceID, currentVersion); err != nil {
		return nil, fmt.Errorf("compact credential source versions: %w", err)
	}

	if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit source upsert: %w", err)
	}
	return r.GetSourceMetadata(ctx, teamID, record.Name)
}

type managedVaultPutStage struct {
	sourceID int64
	version  int64
	ref      *CredentialSourceExternalRefSpec
	rotated  bool
}

func (r *Repository) putManagedVaultSource(
	ctx context.Context,
	teamID, storageKind string,
	record *CredentialSourceWriteRequest,
) (*CredentialSourceMetadata, error) {
	if r.vaultResolver == nil {
		return nil, fmt.Errorf("hashicorp_vault credential source storage is not configured")
	}
	stage, err := r.stageManagedVaultSource(ctx, teamID, storageKind, record)
	if err != nil {
		return nil, err
	}
	if err := r.vaultResolver.Put(ctx, teamID, record.ResolverKind, stage.ref, record.Spec, true); err != nil {
		return nil, fmt.Errorf("write managed vault source: %w", err)
	}

	finalizeCtx, cancel := detachedCredentialSourceContext(ctx)
	defer cancel()
	metadata, err := r.finalizeManagedVaultSource(finalizeCtx, teamID, record.Name, record.ResolverKind, storageKind, stage)
	if err != nil {
		return nil, fmt.Errorf("finalize managed vault source: %w", err)
	}
	return metadata, nil
}

func (r *Repository) stageManagedVaultSource(
	ctx context.Context,
	teamID, storageKind string,
	record *CredentialSourceWriteRequest,
) (*managedVaultPutStage, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin managed vault source staging transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	quotaRef, err := teamquota.ReserveControlPlaneObjectTargetTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(
			teamID,
			teamquota.ControlPlaneOwnerKindCredentialSource,
			record.Name,
		),
		"stage_managed_vault_credential_source",
		credentialSourceControlPlaneObjectTarget,
	)
	if err != nil {
		return nil, err
	}

	stage := &managedVaultPutStage{}
	var (
		existingResolverKind string
		existingStatus       string
		existingStorageKind  string
		existingPayload      []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT s.id, s.current_version, s.resolver_kind, s.status, v.storage_kind, v.storage_payload
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1 AND s.name = $2
		FOR UPDATE OF s, v
	`, teamID, record.Name).Scan(
		&stage.sourceID,
		&stage.version,
		&existingResolverKind,
		&existingStatus,
		&existingStorageKind,
		&existingPayload,
	)
	switch err {
	case nil:
		if err := validateExistingCredentialSourceMutation(
			existingResolverKind,
			existingStatus,
			existingStorageKind,
			existingPayload,
			record,
			storageKind,
		); err != nil && !errors.Is(err, ErrCredentialSourceLifecycleConflict) {
			return nil, err
		}
		switch existingStatus {
		case credentialSourceStatusActive:
			stage.version++
			ref, managed, err := decodeExternalRefStorage(existingPayload)
			if err != nil {
				return nil, err
			}
			if !managed {
				return nil, fmt.Errorf("%w: existing source is external", ErrCredentialSourceManagementModeImmutable)
			}
			stage.ref = ref
			payload, err := encodeExternalRefStorage(stage.ref, true)
			if err != nil {
				return nil, fmt.Errorf("marshal managed vault source reference: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO credential_source_versions (
					source_id, version, resolver_kind, spec_json, storage_kind, storage_payload
				) VALUES ($1, $2, $3, '{}'::jsonb, $4, $5)
			`, stage.sourceID, stage.version, record.ResolverKind, storageKind, payload); err != nil {
				return nil, fmt.Errorf("insert staged managed vault source version: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE credential_sources
				SET current_version = $3,
				    status = $4
				WHERE team_id = $1 AND name = $2
			`, teamID, record.Name, stage.version, credentialSourceStatusProvisioning); err != nil {
				return nil, fmt.Errorf("stage managed vault source rotation: %w", err)
			}
			stage.rotated = true
		case credentialSourceStatusProvisioning:
			ref, managed, err := decodeExternalRefStorage(existingPayload)
			if err != nil {
				return nil, err
			}
			if !managed {
				return nil, fmt.Errorf("%w: existing source is external", ErrCredentialSourceManagementModeImmutable)
			}
			stage.ref = ref
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM credential_source_versions
					WHERE source_id = $1 AND version <> $2
				)
			`, stage.sourceID, stage.version).Scan(&stage.rotated); err != nil {
				return nil, fmt.Errorf("inspect staged managed vault source rotation: %w", err)
			}
		default:
			return nil, fmt.Errorf("%w: source status is %q", ErrCredentialSourceLifecycleConflict, existingStatus)
		}
	case pgx.ErrNoRows:
		stage.version = 1
		stage.ref = defaultVaultSourceRef(teamID, record.Name)
		if err := tx.QueryRow(ctx, `
			INSERT INTO credential_sources (team_id, name, resolver_kind, current_version, status)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id
		`, teamID, record.Name, record.ResolverKind, stage.version, credentialSourceStatusProvisioning).Scan(&stage.sourceID); err != nil {
			return nil, fmt.Errorf("insert staged managed vault source: %w", err)
		}
		payload, err := encodeExternalRefStorage(stage.ref, true)
		if err != nil {
			return nil, fmt.Errorf("marshal managed vault source reference: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO credential_source_versions (
				source_id, version, resolver_kind, spec_json, storage_kind, storage_payload
			) VALUES ($1, $2, $3, '{}'::jsonb, $4, $5)
		`, stage.sourceID, stage.version, record.ResolverKind, storageKind, payload); err != nil {
			return nil, fmt.Errorf("insert staged managed vault source version: %w", err)
		}
	default:
		return nil, fmt.Errorf("load managed vault source for staging: %w", err)
	}

	if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit managed vault source staging: %w", err)
	}
	return stage, nil
}

func (r *Repository) finalizeManagedVaultSource(
	ctx context.Context,
	teamID, name, resolverKind, storageKind string,
	stage *managedVaultPutStage,
) (*CredentialSourceMetadata, error) {
	if stage == nil {
		return nil, fmt.Errorf("managed vault source stage is required")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin managed vault source finalize transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		metadata       CredentialSourceMetadata
		sourceID       int64
		storagePayload []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT
			s.id,
			s.name,
			s.resolver_kind,
			v.storage_kind,
			s.current_version,
			s.status,
			v.storage_payload,
			s.created_at,
			s.updated_at
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1 AND s.name = $2
		FOR UPDATE OF s, v
	`, teamID, name).Scan(
		&sourceID,
		&metadata.Name,
		&metadata.ResolverKind,
		&metadata.StorageKind,
		&metadata.CurrentVersion,
		&metadata.Status,
		&storagePayload,
		&metadata.CreatedAt,
		&metadata.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("lock managed vault source for finalize: %w", err)
	}
	if sourceID != stage.sourceID || metadata.CurrentVersion != stage.version {
		return nil, fmt.Errorf(
			"%w: staged source/version %d/%d, current %d/%d",
			ErrCredentialSourceLifecycleConflict,
			stage.sourceID,
			stage.version,
			sourceID,
			metadata.CurrentVersion,
		)
	}
	if metadata.ResolverKind != resolverKind {
		return nil, fmt.Errorf("%w: existing %q, requested %q", ErrCredentialSourceResolverKindImmutable, metadata.ResolverKind, resolverKind)
	}
	if normalizeStorageKind(metadata.StorageKind, "") != storageKind {
		return nil, fmt.Errorf("%w: existing %q, requested %q", ErrCredentialSourceStorageKindImmutable, metadata.StorageKind, storageKind)
	}
	_, managed, err := decodeExternalRefStorage(storagePayload)
	if err != nil {
		return nil, err
	}
	if !managed {
		return nil, fmt.Errorf("%w: existing source is external", ErrCredentialSourceManagementModeImmutable)
	}
	if metadata.Status == credentialSourceStatusActive {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit idempotent managed vault source finalize: %w", err)
		}
		return &metadata, nil
	}
	if metadata.Status != credentialSourceStatusProvisioning {
		return nil, fmt.Errorf("%w: source status is %q", ErrCredentialSourceLifecycleConflict, metadata.Status)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sandbox_egress_credential_bindings
		SET source_version = $2,
		    updated_at = NOW()
		WHERE source_id = $1
	`, stage.sourceID, stage.version); err != nil {
		return nil, fmt.Errorf("advance managed vault source bindings: %w", err)
	}
	if stage.rotated {
		event := pubsub.CredentialSourceRotatedEvent{
			EventBase:     pubsub.NewEventBase(nil),
			TeamID:        teamID,
			SourceID:      stage.sourceID,
			SourceRef:     name,
			SourceVersion: stage.version,
		}
		if err := notifyCredentialSourceRotated(ctx, tx, event); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM credential_source_versions
		WHERE source_id = $1 AND version <> $2
	`, stage.sourceID, stage.version); err != nil {
		return nil, fmt.Errorf("compact managed vault source versions: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		UPDATE credential_sources
		SET status = $3
		WHERE team_id = $1 AND name = $2
		RETURNING updated_at
	`, teamID, name, credentialSourceStatusActive).Scan(&metadata.UpdatedAt); err != nil {
		return nil, fmt.Errorf("activate managed vault source: %w", err)
	}
	metadata.Status = credentialSourceStatusActive
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit managed vault source finalize: %w", err)
	}
	return &metadata, nil
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
	if r == nil || r.pool == nil {
		return fmt.Errorf("binding repository is not configured")
	}
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if name == "" {
		return fmt.Errorf("source name is required")
	}

	stage, err := r.stageCredentialSourceDeletion(ctx, teamID, name)
	if err != nil || stage == nil {
		return err
	}
	if stage.managedVault {
		if r.vaultResolver == nil {
			return fmt.Errorf("hashicorp_vault credential source storage is not configured")
		}
		if err := r.vaultResolver.Delete(ctx, teamID, stage.ref); err != nil {
			return fmt.Errorf("delete managed vault source: %w", err)
		}
	}

	finalizeCtx, cancel := detachedCredentialSourceContext(ctx)
	defer cancel()
	if err := r.finalizeCredentialSourceDeletion(finalizeCtx, teamID, name, stage.sourceID); err != nil {
		return fmt.Errorf("finalize credential source deletion: %w", err)
	}
	return nil
}

type credentialSourceDeleteStage struct {
	sourceID     int64
	managedVault bool
	ref          *CredentialSourceExternalRefSpec
}

func (r *Repository) stageCredentialSourceDeletion(
	ctx context.Context,
	teamID, name string,
) (*credentialSourceDeleteStage, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin source delete staging transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		status         string
		sourceID       int64
		storageKind    string
		storagePayload []byte
	)
	if err := tx.QueryRow(ctx, `
		SELECT s.id, s.status, v.storage_kind, v.storage_payload
		FROM credential_sources s
		JOIN credential_source_versions v
		  ON v.source_id = s.id AND v.version = s.current_version
		WHERE s.team_id = $1 AND s.name = $2
		FOR UPDATE OF s, v
	`, teamID, name).Scan(&sourceID, &status, &storageKind, &storagePayload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lock source record for deletion: %w", err)
	}

	var bound bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM sandbox_egress_credential_bindings
			WHERE source_id = $1
		)
	`, sourceID).Scan(&bound); err != nil {
		return nil, fmt.Errorf("check credential source bindings: %w", err)
	}
	if bound {
		return nil, ErrCredentialSourceInUse
	}
	switch status {
	case credentialSourceStatusActive, credentialSourceStatusProvisioning, credentialSourceStatusDeleting:
	default:
		return nil, fmt.Errorf("%w: source status is %q", ErrCredentialSourceLifecycleConflict, status)
	}

	stage := &credentialSourceDeleteStage{sourceID: sourceID}
	if normalizeStorageKind(storageKind, "") == CredentialSourceStorageKindHashiCorpVault {
		ref, managed, err := decodeExternalRefStorage(storagePayload)
		if err != nil {
			return nil, err
		}
		stage.managedVault = managed
		stage.ref = ref
	}

	if _, err := tx.Exec(ctx, `
		UPDATE credential_sources
		SET status = $3
		WHERE team_id = $1 AND name = $2
	`, teamID, name, credentialSourceStatusDeleting); err != nil {
		return nil, fmt.Errorf("stage credential source deletion: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit credential source deletion staging: %w", err)
	}
	return stage, nil
}

func (r *Repository) finalizeCredentialSourceDeletion(
	ctx context.Context,
	teamID, name string,
	sourceID int64,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin source delete finalize transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	quotaRef, err := teamquota.BeginControlPlaneObjectReleaseTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindCredentialSource, name),
		"finalize_delete_credential_source",
		0,
	)
	if err != nil {
		return err
	}

	var (
		currentSourceID int64
		status          string
	)
	err = tx.QueryRow(ctx, `
		SELECT id, status
		FROM credential_sources
		WHERE team_id = $1 AND name = $2
		FOR UPDATE
	`, teamID, name).Scan(&currentSourceID, &status)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock credential source for delete finalize: %w", err)
	}
	if err == nil {
		if currentSourceID != sourceID {
			return fmt.Errorf(
				"%w: staged source %d, current source %d",
				ErrCredentialSourceLifecycleConflict,
				sourceID,
				currentSourceID,
			)
		}
		if status != credentialSourceStatusDeleting {
			return fmt.Errorf("%w: source status is %q", ErrCredentialSourceLifecycleConflict, status)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM credential_sources
			WHERE team_id = $1 AND name = $2
		`, teamID, name); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" {
				return ErrCredentialSourceInUse
			}
			return fmt.Errorf("delete source record: %w", err)
		}
	}
	if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source deletion finalize: %w", err)
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
		ref, _, err := decodeExternalRefStorage(storagePayload)
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
		ref, _, err := decodeExternalRefStorage(storagePayload)
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
		ref := cloneCredentialSourceExternalRef(record.ExternalRef)
		managed := ref == nil
		if ref == nil {
			if !credentialSourceSpecPresent(record.Spec) {
				return nil, nil, fmt.Errorf("hashicorp_vault credential source requires spec or externalRef")
			}
			ref = defaultVaultSourceRef(teamID, record.Name)
		}
		ref.Provider = CredentialSourceExternalProviderHashiCorpVault
		if ref.Connection == "" {
			ref.Connection = "default"
		}
		if ref.Mount == "" {
			ref.Mount = "secret"
		}
		payload, err := encodeExternalRefStorage(ref, managed)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal vault source reference: %w", err)
		}
		return []byte(`{}`), payload, nil
	case CredentialSourceStorageKindExternalRef:
		if record.ExternalRef == nil {
			return nil, nil, fmt.Errorf("external_ref credential source requires externalRef")
		}
		payload, err := encodeExternalRefStorage(record.ExternalRef, false)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal external source reference: %w", err)
		}
		return []byte(`{}`), payload, nil
	case CredentialSourceStorageKindPlaintextPG:
		specJSON, err := CanonicalCredentialSourceSpec(record.Spec)
		if err != nil {
			return nil, nil, err
		}
		return specJSON, []byte(`{}`), nil
	default:
		return nil, nil, fmt.Errorf("credential source storage kind %q is not supported", storageKind)
	}
}

func defaultVaultSourceRef(teamID, name string) *CredentialSourceExternalRefSpec {
	return &CredentialSourceExternalRefSpec{
		Provider:   CredentialSourceExternalProviderHashiCorpVault,
		Connection: "default",
		Mount:      "secret",
		Path:       fmt.Sprintf("sandbox0/credential-sources/%s/%s", cleanVaultPath(teamID), cleanVaultPath(name)),
	}
}

func cloneCredentialSourceExternalRef(ref *CredentialSourceExternalRefSpec) *CredentialSourceExternalRefSpec {
	if ref == nil {
		return nil
	}
	clone := *ref
	if ref.Fields != nil {
		clone.Fields = make(map[string]string, len(ref.Fields))
		for key, value := range ref.Fields {
			clone.Fields[key] = value
		}
	}
	return &clone
}

type credentialSourceExternalRefStorage struct {
	Ref               *CredentialSourceExternalRefSpec `json:"ref"`
	ManagedBySandbox0 bool                             `json:"managedBySandbox0,omitempty"`
}

func encodeExternalRefStorage(ref *CredentialSourceExternalRefSpec, managed bool) ([]byte, error) {
	if err := ValidateCredentialSourceExternalRefSize(ref); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(credentialSourceExternalRefStorage{
		Ref:               ref,
		ManagedBySandbox0: managed,
	})
	if err != nil {
		return nil, err
	}
	if err := ValidateCredentialEnvelope(payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func decodeExternalRefStorage(payload []byte) (*CredentialSourceExternalRefSpec, bool, error) {
	if err := ValidateCredentialEnvelope(payload); err != nil {
		return nil, false, err
	}
	var storage credentialSourceExternalRefStorage
	if err := json.Unmarshal(payload, &storage); err != nil {
		return nil, false, fmt.Errorf("unmarshal external source reference: %w", err)
	}
	if storage.Ref == nil {
		return nil, false, fmt.Errorf("external source reference is missing")
	}
	ref := storage.Ref
	if ref.Provider == "" {
		ref.Provider = CredentialSourceExternalProviderHashiCorpVault
	}
	if err := ValidateCredentialSourceExternalRefSize(ref); err != nil {
		return nil, false, err
	}
	return ref, storage.ManagedBySandbox0, nil
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

func validateCredentialSourceStorageRequest(storageKind string, record *CredentialSourceWriteRequest) error {
	if record == nil {
		return fmt.Errorf("credential source record is required")
	}
	hasSpec := credentialSourceSpecPresent(record.Spec)
	if record.ExternalRef != nil && hasSpec {
		return fmt.Errorf("%w: externalRef cannot be combined with secret spec", ErrCredentialSourceExternalRefReadOnly)
	}
	switch storageKind {
	case CredentialSourceStorageKindEncryptedPG, CredentialSourceStorageKindPlaintextPG:
		if record.ExternalRef != nil {
			return fmt.Errorf("credential source storage kind %q does not accept externalRef", storageKind)
		}
	case CredentialSourceStorageKindHashiCorpVault:
		if record.ExternalRef == nil && !hasSpec {
			return fmt.Errorf("hashicorp_vault credential source requires spec or externalRef")
		}
	case CredentialSourceStorageKindExternalRef:
		if record.ExternalRef == nil {
			return fmt.Errorf("external_ref credential source requires externalRef")
		}
	default:
		return fmt.Errorf("credential source storage kind %q is not supported", storageKind)
	}
	return nil
}

type credentialSourceManagementMode string

const (
	credentialSourceManagementModeInternal credentialSourceManagementMode = "internal"
	credentialSourceManagementModeManaged  credentialSourceManagementMode = "managed"
	credentialSourceManagementModeExternal credentialSourceManagementMode = "external"
)

func requestedCredentialSourceManagementMode(
	storageKind string,
	record *CredentialSourceWriteRequest,
) credentialSourceManagementMode {
	switch storageKind {
	case CredentialSourceStorageKindHashiCorpVault:
		if record != nil && record.ExternalRef == nil {
			return credentialSourceManagementModeManaged
		}
		return credentialSourceManagementModeExternal
	case CredentialSourceStorageKindExternalRef:
		return credentialSourceManagementModeExternal
	default:
		return credentialSourceManagementModeInternal
	}
}

func storedCredentialSourceManagementMode(
	storageKind string,
	storagePayload []byte,
) (credentialSourceManagementMode, error) {
	switch normalizeStorageKind(storageKind, "") {
	case CredentialSourceStorageKindHashiCorpVault:
		_, managed, err := decodeExternalRefStorage(storagePayload)
		if err != nil {
			return "", err
		}
		if managed {
			return credentialSourceManagementModeManaged, nil
		}
		return credentialSourceManagementModeExternal, nil
	case CredentialSourceStorageKindExternalRef:
		if _, _, err := decodeExternalRefStorage(storagePayload); err != nil {
			return "", err
		}
		return credentialSourceManagementModeExternal, nil
	default:
		return credentialSourceManagementModeInternal, nil
	}
}

func validateExistingCredentialSourceMutation(
	existingResolverKind, existingStatus, existingStorageKind string,
	existingStoragePayload []byte,
	record *CredentialSourceWriteRequest,
	requestedStorageKind string,
) error {
	if existingResolverKind != record.ResolverKind {
		return fmt.Errorf(
			"%w: existing %q, requested %q",
			ErrCredentialSourceResolverKindImmutable,
			existingResolverKind,
			record.ResolverKind,
		)
	}
	normalizedExistingStorageKind := normalizeStorageKind(existingStorageKind, "")
	if normalizedExistingStorageKind != requestedStorageKind {
		return fmt.Errorf(
			"%w: existing %q, requested %q",
			ErrCredentialSourceStorageKindImmutable,
			normalizedExistingStorageKind,
			requestedStorageKind,
		)
	}
	existingMode, err := storedCredentialSourceManagementMode(normalizedExistingStorageKind, existingStoragePayload)
	if err != nil {
		return err
	}
	requestedMode := requestedCredentialSourceManagementMode(requestedStorageKind, record)
	if existingMode != requestedMode {
		return fmt.Errorf(
			"%w: existing %q, requested %q",
			ErrCredentialSourceManagementModeImmutable,
			existingMode,
			requestedMode,
		)
	}
	if existingStatus != credentialSourceStatusActive {
		return fmt.Errorf("%w: source status is %q", ErrCredentialSourceLifecycleConflict, existingStatus)
	}
	return nil
}

func detachedCredentialSourceContext(parent context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if parent != nil {
		base = context.WithoutCancel(parent)
	}
	return context.WithTimeout(base, credentialSourceFinalizeTimeout)
}
