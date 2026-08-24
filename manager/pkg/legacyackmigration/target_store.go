package legacyackmigration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

const (
	targetBuildStatePending  = "pending"
	targetBuildStateBuilding = "building"
	targetBuildStateReady    = "ready"

	MinTargetBuildLeaseTTL = 5 * time.Second
	MaxTargetBuildLeaseTTL = 15 * time.Minute
)

var (
	ErrTargetMigrationConflict = errors.New("legacy ACK target migration conflict")
	ErrTargetBuildLeaseLost    = errors.New("legacy ACK target build lease lost")
)

// TargetContract is the exact executable and block format accepted by the
// target region.
type TargetContract struct {
	FormatGeneration int
	ProcdProtocol    string
	ProcdDigest      string
	BlockOptions     rootfsblock.BuildOptions
}

type TargetBuildLease struct {
	BuildID   string
	WorkerID  string
	Token     string
	ExpiresAt time.Time
}

type TargetBuildOperation struct {
	SessionID          string
	Build              MaterializedBuild
	Contract           TargetContract
	InputDigest        string
	State              string
	LeaseOwner         string
	LeaseToken         string
	LeaseExpiresAt     time.Time
	AttemptCount       int
	BaseArtifactDigest string
	Result             *rootfsimporter.MaterializedGenerationBuildResult
	CreatedAt          time.Time
	UpdatedAt          time.Time
	ReadyAt            time.Time
}

func (o *TargetBuildOperation) Lease() (TargetBuildLease, error) {
	if o == nil || o.State != targetBuildStateBuilding || o.LeaseOwner == "" ||
		o.LeaseToken == "" || o.LeaseExpiresAt.IsZero() {
		return TargetBuildLease{}, fmt.Errorf("target build operation is not leased")
	}
	return TargetBuildLease{
		BuildID: o.Build.ID, WorkerID: o.LeaseOwner,
		Token: o.LeaseToken, ExpiresAt: o.LeaseExpiresAt,
	}, nil
}

// TargetStore owns restart-safe object publication and the ready CAS in a
// temporary migration schema. Product RootFS reachability is installed later
// in one separate catalog commit.
type TargetStore struct {
	pool *pgxpool.Pool
}

func NewTargetStore(pool *pgxpool.Pool) (*TargetStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("target PostgreSQL pool is required")
	}
	return &TargetStore{pool: pool}, nil
}

// EnsureSchema creates only temporary operation state. It is intentionally
// outside the manager schema and is dropped after verified cutover.
func (s *TargetStore) EnsureSchema(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("target migration store is not configured")
	}
	_, err := s.pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS legacy_ack_migration;
		CREATE TABLE IF NOT EXISTS legacy_ack_migration.sessions (
			session_id TEXT PRIMARY KEY CHECK (octet_length(session_id) BETWEEN 1 AND 128),
			source_catalog_digest TEXT NOT NULL CHECK (source_catalog_digest ~ '^sha256:[0-9a-f]{64}$'),
			target_cluster_id TEXT NOT NULL CHECK (octet_length(target_cluster_id) BETWEEN 1 AND 512),
			state TEXT NOT NULL CHECK (state IN ('prepared', 'importing', 'committed')),
			commit_digest TEXT CHECK (commit_digest IS NULL OR commit_digest ~ '^sha256:[0-9a-f]{64}$'),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			committed_at TIMESTAMPTZ,
			CHECK (
				(state IN ('prepared', 'importing') AND commit_digest IS NULL AND committed_at IS NULL)
				OR (state = 'committed' AND commit_digest IS NOT NULL AND committed_at IS NOT NULL)
			)
		);
		CREATE TABLE IF NOT EXISTS legacy_ack_migration.builds (
			build_id TEXT PRIMARY KEY CHECK (octet_length(build_id) BETWEEN 1 AND 128),
			session_id TEXT NOT NULL REFERENCES legacy_ack_migration.sessions(session_id) ON DELETE RESTRICT,
			team_id TEXT NOT NULL CHECK (octet_length(team_id) BETWEEN 1 AND 512),
			head_layer_id TEXT NOT NULL CHECK (octet_length(head_layer_id) BETWEEN 1 AND 512),
			pinned_oci_ref TEXT NOT NULL CHECK (octet_length(pinned_oci_ref) BETWEEN 1 AND 2048),
			source_oci_digest TEXT NOT NULL CHECK (source_oci_digest ~ '^sha256:[0-9a-f]{64}$'),
			oci_os TEXT NOT NULL CHECK (oci_os = 'linux'),
			oci_architecture TEXT NOT NULL CHECK (octet_length(oci_architecture) BETWEEN 1 AND 64),
			oci_variant TEXT NOT NULL DEFAULT '' CHECK (octet_length(oci_variant) <= 64),
			logical_size_bytes BIGINT NOT NULL CHECK (logical_size_bytes BETWEEN 314572800 AND 1099511627776 AND logical_size_bytes % 4096 = 0),
			mutation_digest TEXT NOT NULL CHECK (mutation_digest ~ '^sha256:[0-9a-f]{64}$'),
			object_prefix TEXT NOT NULL CHECK (octet_length(object_prefix) BETWEEN 1 AND 512),
			format_generation INTEGER NOT NULL CHECK (format_generation > 0),
			procd_protocol TEXT NOT NULL CHECK (octet_length(procd_protocol) BETWEEN 1 AND 128),
			procd_digest TEXT NOT NULL CHECK (procd_digest ~ '^sha256:[0-9a-f]{64}$'),
			block_data_range_bytes INTEGER NOT NULL CHECK (block_data_range_bytes > 0),
			block_pack_bytes INTEGER NOT NULL CHECK (block_pack_bytes > 0),
			block_page_entries INTEGER NOT NULL CHECK (block_page_entries > 0),
			input_digest TEXT NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
			state TEXT NOT NULL CHECK (state IN ('pending', 'building', 'ready')),
			lease_owner TEXT,
			lease_token TEXT,
			lease_expires_at TIMESTAMPTZ,
			attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 1000000),
			base_artifact_digest TEXT REFERENCES manager.rootfs_base_artifacts(artifact_digest) ON DELETE RESTRICT,
			descriptor_digest TEXT,
			current_block_head TEXT,
			descriptor BYTEA,
			result_objects INTEGER,
			result_bytes BIGINT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			ready_at TIMESTAMPTZ,
			CHECK (
				(state = 'pending' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL
					AND base_artifact_digest IS NULL AND descriptor_digest IS NULL
					AND current_block_head IS NULL AND descriptor IS NULL
					AND result_objects IS NULL AND result_bytes IS NULL AND ready_at IS NULL)
				OR (state = 'building' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL
					AND lease_expires_at IS NOT NULL AND base_artifact_digest IS NULL
					AND descriptor_digest IS NULL AND current_block_head IS NULL AND descriptor IS NULL
					AND result_objects IS NULL AND result_bytes IS NULL AND ready_at IS NULL)
				OR (state = 'ready' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL
					AND base_artifact_digest IS NOT NULL AND descriptor_digest ~ '^sha256:[0-9a-f]{64}$'
					AND current_block_head ~ '^sha256:[0-9a-f]{64}$' AND octet_length(descriptor) BETWEEN 1 AND 65536
					AND result_objects > 0 AND result_bytes > 0 AND ready_at IS NOT NULL)
			)
		);
		CREATE INDEX IF NOT EXISTS legacy_ack_migration_build_work
			ON legacy_ack_migration.builds(state, lease_expires_at, created_at, build_id);
		CREATE TABLE IF NOT EXISTS legacy_ack_migration.build_objects (
			build_id TEXT NOT NULL REFERENCES legacy_ack_migration.builds(build_id) ON DELETE CASCADE,
			object_key TEXT NOT NULL REFERENCES manager.rootfs_materialization_objects(object_key) ON DELETE RESTRICT,
			upload_state TEXT NOT NULL CHECK (upload_state IN ('prepared', 'published')),
			result_object BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (build_id, object_key)
		);
	`)
	if err != nil {
		return fmt.Errorf("create legacy ACK target migration schema: %w", err)
	}
	return nil
}

func (s *TargetStore) EnsureSession(
	ctx context.Context,
	sessionID, sourceCatalogDigest, targetClusterID string,
) error {
	sessionID, targetClusterID = strings.TrimSpace(sessionID), strings.TrimSpace(targetClusterID)
	if sessionID == "" || len(sessionID) > 128 || targetClusterID == "" || len(targetClusterID) > 512 {
		return fmt.Errorf("target migration session and cluster identities are invalid")
	}
	if err := validateCanonicalDigest(sourceCatalogDigest); err != nil {
		return fmt.Errorf("source catalog digest: %w", err)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO legacy_ack_migration.sessions (
			session_id, source_catalog_digest, target_cluster_id, state
		) VALUES ($1, $2, $3, 'prepared')
		ON CONFLICT (session_id) DO NOTHING
	`, sessionID, sourceCatalogDigest, targetClusterID)
	if err != nil {
		return fmt.Errorf("ensure target migration session: %w", err)
	}
	var storedDigest, storedCluster string
	if err := s.pool.QueryRow(ctx, `
		SELECT source_catalog_digest, target_cluster_id
		FROM legacy_ack_migration.sessions WHERE session_id = $1
	`, sessionID).Scan(&storedDigest, &storedCluster); err != nil {
		return fmt.Errorf("read target migration session: %w", err)
	}
	if storedDigest != sourceCatalogDigest || storedCluster != targetClusterID {
		return fmt.Errorf("%w: session %s has different immutable inputs", ErrTargetMigrationConflict, sessionID)
	}
	return nil
}

func (s *TargetStore) BeginBuild(
	ctx context.Context,
	sessionID string,
	build MaterializedBuild,
	contract TargetContract,
) (*TargetBuildOperation, error) {
	if err := validateMaterializedBuildIdentity(build); err != nil {
		return nil, err
	}
	normalizedBuild, normalizedContract, inputDigest, err := normalizeTargetBuildInput(build, contract)
	if err != nil {
		return nil, err
	}
	options := normalizedContract.BlockOptions
	_, err = s.pool.Exec(ctx, `
		INSERT INTO legacy_ack_migration.builds (
			build_id, session_id, team_id, head_layer_id, pinned_oci_ref,
			source_oci_digest, oci_os, oci_architecture, oci_variant,
			logical_size_bytes, mutation_digest, object_prefix,
			format_generation, procd_protocol, procd_digest,
			block_data_range_bytes, block_pack_bytes, block_page_entries,
			input_digest, state
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18, $19, 'pending'
		)
		ON CONFLICT (build_id) DO NOTHING
	`, normalizedBuild.ID, strings.TrimSpace(sessionID), normalizedBuild.TeamID,
		normalizedBuild.HeadLayerID, normalizedBuild.PinnedOCIRef, normalizedBuild.SourceOCIDigest,
		normalizedBuild.Platform.OS, normalizedBuild.Platform.Architecture, normalizedBuild.Platform.Variant,
		normalizedBuild.LogicalSizeBytes, normalizedBuild.MutationDigest, normalizedBuild.ObjectPrefix,
		normalizedContract.FormatGeneration, normalizedContract.ProcdProtocol, normalizedContract.ProcdDigest,
		options.DataRangeBytes, options.PackBytes, options.PageEntries, inputDigest)
	if err != nil {
		return nil, fmt.Errorf("begin target materialized build: %w", err)
	}
	operation, err := s.GetBuild(ctx, normalizedBuild.ID)
	if err != nil {
		return nil, err
	}
	if operation.SessionID != strings.TrimSpace(sessionID) || operation.InputDigest != inputDigest {
		return nil, fmt.Errorf("%w: build %s has different immutable inputs", ErrTargetMigrationConflict, normalizedBuild.ID)
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE legacy_ack_migration.sessions SET state = 'importing', updated_at = NOW()
		WHERE session_id = $1 AND state = 'prepared'
	`, strings.TrimSpace(sessionID))
	if err != nil {
		return nil, fmt.Errorf("mark target migration session importing: %w", err)
	}
	return operation, nil
}

func (s *TargetStore) GetBuild(ctx context.Context, buildID string) (*TargetBuildOperation, error) {
	operation, err := scanTargetBuild(s.pool.QueryRow(ctx, targetBuildSelectSQL()+" WHERE build_id = $1", strings.TrimSpace(buildID)))
	if err != nil {
		return nil, err
	}
	if err := validateStoredTargetBuildInput(operation); err != nil {
		return nil, err
	}
	return operation, nil
}

func (s *TargetStore) LeaseBuild(
	ctx context.Context,
	buildID, workerID string,
	ttl time.Duration,
) (*TargetBuildOperation, error) {
	buildID, workerID = strings.TrimSpace(buildID), strings.TrimSpace(workerID)
	if buildID == "" || workerID == "" || len(workerID) > 256 {
		return nil, fmt.Errorf("target build and worker identities are required")
	}
	if ttl < MinTargetBuildLeaseTTL || ttl > MaxTargetBuildLeaseTTL || ttl%time.Millisecond != 0 {
		return nil, fmt.Errorf("target build lease TTL is outside supported bounds")
	}
	token, err := newTargetBuildLeaseToken()
	if err != nil {
		return nil, err
	}
	operation, err := scanTargetBuild(s.pool.QueryRow(ctx, `
		UPDATE legacy_ack_migration.builds AS build
		SET state = 'building', lease_owner = $2, lease_token = $3,
			lease_expires_at = clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond'),
			attempt_count = attempt_count + 1, updated_at = NOW()
		WHERE build_id = $1 AND (
			state = 'pending' OR (state = 'building' AND lease_expires_at <= clock_timestamp())
		)
		RETURNING `+targetBuildReturningColumns("build"),
		buildID, workerID, token, ttl.Milliseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		operation, getErr := s.GetBuild(ctx, buildID)
		if getErr != nil {
			return nil, getErr
		}
		if operation.State == targetBuildStateReady {
			return operation, nil
		}
		return nil, fmt.Errorf("%w: build %s is leased by another worker", ErrTargetBuildLeaseLost, buildID)
	}
	if err != nil {
		return nil, fmt.Errorf("lease target materialized build: %w", err)
	}
	if err := validateStoredTargetBuildInput(operation); err != nil {
		return nil, err
	}
	return operation, nil
}

func (s *TargetStore) RenewBuildLease(
	ctx context.Context,
	lease TargetBuildLease,
	ttl time.Duration,
) (TargetBuildLease, error) {
	lease, err := normalizeTargetBuildLease(lease)
	if err != nil {
		return TargetBuildLease{}, err
	}
	if ttl < MinTargetBuildLeaseTTL || ttl > MaxTargetBuildLeaseTTL || ttl%time.Millisecond != 0 {
		return TargetBuildLease{}, fmt.Errorf("target build lease TTL is outside supported bounds")
	}
	var expiresAt time.Time
	err = s.pool.QueryRow(ctx, `
		UPDATE legacy_ack_migration.builds
		SET lease_expires_at = clock_timestamp() + ($4::bigint * INTERVAL '1 millisecond'), updated_at = NOW()
		WHERE build_id = $1 AND state = 'building' AND lease_owner = $2 AND lease_token = $3
			AND lease_expires_at > clock_timestamp()
		RETURNING lease_expires_at
	`, lease.BuildID, lease.WorkerID, lease.Token, ttl.Milliseconds()).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TargetBuildLease{}, fmt.Errorf("%w: %s", ErrTargetBuildLeaseLost, lease.BuildID)
	}
	if err != nil {
		return TargetBuildLease{}, fmt.Errorf("renew target build lease: %w", err)
	}
	lease.ExpiresAt = expiresAt
	return lease, nil
}

// ReleaseBuildLease makes a transiently failed build immediately retryable.
// Prepared or already published objects remain journaled for exact replay.
func (s *TargetStore) ReleaseBuildLease(ctx context.Context, lease TargetBuildLease) error {
	lease, err := normalizeTargetBuildLease(lease)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE legacy_ack_migration.builds
		SET state = 'pending', lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, updated_at = NOW()
		WHERE build_id = $1 AND state = 'building'
			AND lease_owner = $2 AND lease_token = $3
	`, lease.BuildID, lease.WorkerID, lease.Token)
	if err != nil {
		return fmt.Errorf("release target build lease: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	operation, getErr := s.GetBuild(ctx, lease.BuildID)
	if getErr == nil && operation.State == targetBuildStateReady {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrTargetBuildLeaseLost, lease.BuildID)
}

// Journal returns the pre-PUT boundary used by rootfsimporter.JournaledPublisher.
func (s *TargetStore) Journal(lease TargetBuildLease) rootfsimporter.ObjectPublicationJournal {
	return targetBuildJournal{store: s, lease: lease}
}

type targetBuildJournal struct {
	store *TargetStore
	lease TargetBuildLease
}

func (j targetBuildJournal) PrepareObject(
	ctx context.Context,
	operationID string,
	reference rootfsblock.ObjectReference,
) error {
	if j.store == nil || operationID != j.lease.BuildID {
		return fmt.Errorf("target build journal identity does not match its lease")
	}
	return j.store.PrepareBuildObject(ctx, j.lease, reference)
}

func (j targetBuildJournal) MarkObjectPublished(
	ctx context.Context,
	operationID string,
	reference rootfsblock.ObjectReference,
) error {
	if j.store == nil || operationID != j.lease.BuildID {
		return fmt.Errorf("target build journal identity does not match its lease")
	}
	return j.store.MarkBuildObjectPublished(ctx, j.lease, reference)
}

func (s *TargetStore) PrepareBuildObject(
	ctx context.Context,
	lease TargetBuildLease,
	reference rootfsblock.ObjectReference,
) error {
	lease, err := normalizeTargetBuildLease(lease)
	if err != nil {
		return err
	}
	if err := rootfsblock.ValidateObjectReference(reference); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin target build object preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockTargetBuildLease(ctx, tx, lease); err != nil {
		return err
	}
	var pendingDeletion bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key = $1
	)`, reference.Key).Scan(&pendingDeletion); err != nil {
		return fmt.Errorf("check target build object deletion fence: %w", err)
	}
	if pendingDeletion {
		return fmt.Errorf("%w: object %s is pending deletion", ErrTargetMigrationConflict, reference.Key)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_materialization_objects (
			object_key, object_kind, object_size, checksum
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (object_key) DO NOTHING
	`, reference.Key, reference.Kind, reference.Size, reference.Checksum); err != nil {
		return fmt.Errorf("register target build object: %w", err)
	}
	if err := verifyMaterializationObject(ctx, tx, reference, false); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO legacy_ack_migration.build_objects (build_id, object_key, upload_state)
		VALUES ($1, $2, 'prepared')
		ON CONFLICT (build_id, object_key) DO UPDATE SET updated_at = NOW()
	`, lease.BuildID, reference.Key); err != nil {
		return fmt.Errorf("link target build object: %w", err)
	}
	return commitTargetTx(ctx, tx, "object preparation")
}

func (s *TargetStore) MarkBuildObjectPublished(
	ctx context.Context,
	lease TargetBuildLease,
	reference rootfsblock.ObjectReference,
) error {
	lease, err := normalizeTargetBuildLease(lease)
	if err != nil {
		return err
	}
	if err := rootfsblock.ValidateObjectReference(reference); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin target build object publication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockTargetBuildLease(ctx, tx, lease); err != nil {
		return err
	}
	if err := verifyBuildObject(ctx, tx, lease.BuildID, reference, false); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE legacy_ack_migration.build_objects
		SET upload_state = 'published', updated_at = NOW()
		WHERE build_id = $1 AND object_key = $2
	`, lease.BuildID, reference.Key)
	if err != nil {
		return fmt.Errorf("mark target build object published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: object %s is not linked to build %s", ErrTargetMigrationConflict, reference.Key, lease.BuildID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_materialization_objects
		SET uploaded_at = COALESCE(uploaded_at, NOW()), updated_at = NOW()
		WHERE object_key = $1
	`, reference.Key); err != nil {
		return fmt.Errorf("mark target block object uploaded: %w", err)
	}
	return commitTargetTx(ctx, tx, "object publication")
}

// PublishReadyBuild is the only transition that exposes a complete migrated
// descriptor to the later catalog transaction. Exact retries resolve commit
// response loss without requiring a live lease after success.
func (s *TargetStore) PublishReadyBuild(
	ctx context.Context,
	lease TargetBuildLease,
	baseArtifactDigest string,
	result rootfsimporter.MaterializedGenerationBuildResult,
) (*TargetBuildOperation, error) {
	lease, err := normalizeTargetBuildLease(lease)
	if err != nil {
		return nil, err
	}
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("validate migrated generation build: %w", err)
	}
	if err := validateCanonicalDigest(baseArtifactDigest); err != nil {
		return nil, fmt.Errorf("base artifact digest: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin target build ready publication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := scanTargetBuild(tx.QueryRow(ctx, targetBuildSelectSQL()+" WHERE build_id = $1 FOR UPDATE", lease.BuildID))
	if err != nil {
		return nil, fmt.Errorf("lock target materialized build: %w", err)
	}
	if err := validateStoredTargetBuildInput(operation); err != nil {
		return nil, err
	}
	if err := validateTargetBuildResult(operation, result); err != nil {
		return nil, err
	}
	if operation.State == targetBuildStateReady {
		if err := verifyReadyBaseArtifact(ctx, tx, operation, baseArtifactDigest, result); err != nil {
			return nil, err
		}
		if err := verifyReadyTargetBuild(ctx, tx, operation, baseArtifactDigest, result); err != nil {
			return nil, err
		}
		if err := commitTargetTx(ctx, tx, "existing ready build"); err != nil {
			return nil, err
		}
		return operation, nil
	}
	if operation.State != targetBuildStateBuilding || operation.LeaseOwner != lease.WorkerID ||
		operation.LeaseToken != lease.Token {
		return nil, fmt.Errorf("%w: %s", ErrTargetBuildLeaseLost, lease.BuildID)
	}
	if err := verifyReadyBaseArtifact(ctx, tx, operation, baseArtifactDigest, result); err != nil {
		return nil, err
	}
	for _, reference := range result.References {
		if err := verifyBuildObject(ctx, tx, operation.Build.ID, reference, true); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE legacy_ack_migration.build_objects SET result_object = FALSE
		WHERE build_id = $1
	`, operation.Build.ID); err != nil {
		return nil, fmt.Errorf("clear target build result object selection: %w", err)
	}
	keys := make([]string, 0, len(result.References))
	for _, reference := range result.References {
		keys = append(keys, reference.Key)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE legacy_ack_migration.build_objects SET result_object = TRUE, updated_at = NOW()
		WHERE build_id = $1 AND object_key = ANY($2::text[]) AND upload_state = 'published'
	`, operation.Build.ID, keys); err != nil {
		return nil, fmt.Errorf("select target build result objects: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE legacy_ack_migration.builds
		SET state = 'ready', lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			base_artifact_digest = $4, descriptor_digest = $5, current_block_head = $6,
			descriptor = $7, result_objects = $8, result_bytes = $9,
			ready_at = NOW(), updated_at = NOW()
		WHERE build_id = $1 AND state = 'building' AND lease_owner = $2 AND lease_token = $3
			AND lease_expires_at > clock_timestamp()
	`, operation.Build.ID, lease.WorkerID, lease.Token, baseArtifactDigest,
		result.DescriptorDigest.String(), result.CurrentBlockHead.String(), result.DescriptorBytes,
		result.Objects, result.Bytes)
	if err != nil {
		return nil, fmt.Errorf("publish target materialized build ready: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: %s", ErrTargetBuildLeaseLost, operation.Build.ID)
	}
	ready, err := scanTargetBuild(tx.QueryRow(ctx, targetBuildSelectSQL()+" WHERE build_id = $1", operation.Build.ID))
	if err != nil {
		return nil, fmt.Errorf("read ready target materialized build: %w", err)
	}
	if err := validateStoredTargetBuildInput(ready); err != nil {
		return nil, err
	}
	if err := commitTargetTx(ctx, tx, "ready build"); err != nil {
		return nil, err
	}
	return ready, nil
}

func normalizeTargetBuildInput(
	build MaterializedBuild,
	contract TargetContract,
) (MaterializedBuild, TargetContract, string, error) {
	build.ID = strings.TrimSpace(build.ID)
	build.TeamID = strings.TrimSpace(build.TeamID)
	build.HeadLayerID = strings.TrimSpace(build.HeadLayerID)
	build.PinnedOCIRef = strings.TrimSpace(build.PinnedOCIRef)
	build.SourceOCIDigest = strings.TrimSpace(build.SourceOCIDigest)
	build.MutationDigest = strings.TrimSpace(build.MutationDigest)
	build.ObjectPrefix = strings.TrimSpace(build.ObjectPrefix)
	for name, value := range map[string]string{
		"build_id": build.ID, "team_id": build.TeamID, "head_layer_id": build.HeadLayerID,
		"pinned_oci_ref": build.PinnedOCIRef, "object_prefix": build.ObjectPrefix,
	} {
		if value == "" {
			return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("%s is required", name)
		}
	}
	if source, err := rootfsimporter.PinnedSourceDigest(build.PinnedOCIRef); err != nil || source.String() != build.SourceOCIDigest {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("materialized build pinned image does not match its source digest")
	}
	if err := validateCanonicalDigest(build.MutationDigest); err != nil {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("mutation digest: %w", err)
	}
	if build.Platform.OS != "linux" || build.Platform.Architecture == "" || build.LogicalSizeBytes <= 0 {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("materialized build platform or logical size is invalid")
	}
	if !strings.HasPrefix(build.ObjectPrefix, "rootfs/legacy-ack-v1/") {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("materialized build object prefix is outside the migration namespace")
	}
	if contract.FormatGeneration <= 0 || rootfsimporter.ValidateProcdProtocol(contract.ProcdProtocol) != nil {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("target RootFS format or procd protocol is invalid")
	}
	if err := validateCanonicalDigest(contract.ProcdDigest); err != nil {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("target procd digest: %w", err)
	}
	configuredPrefix := strings.TrimSpace(contract.BlockOptions.ObjectPrefix)
	if configuredPrefix == "" {
		contract.BlockOptions.ObjectPrefix = build.ObjectPrefix
	} else if configuredPrefix != build.ObjectPrefix {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("target block options use another object prefix")
	}
	options, err := rootfsblock.NormalizeBuildOptions(contract.BlockOptions)
	if err != nil {
		return MaterializedBuild{}, TargetContract{}, "", fmt.Errorf("target block options: %w", err)
	}
	contract.BlockOptions = options
	type digestInput struct {
		BuildID          string `json:"build_id"`
		TeamID           string `json:"team_id"`
		HeadLayerID      string `json:"head_layer_id"`
		PinnedOCIRef     string `json:"pinned_oci_ref"`
		SourceOCIDigest  string `json:"source_oci_digest"`
		OS               string `json:"os"`
		Architecture     string `json:"architecture"`
		Variant          string `json:"variant"`
		LogicalSizeBytes int64  `json:"logical_size_bytes"`
		MutationDigest   string `json:"mutation_digest"`
		ObjectPrefix     string `json:"object_prefix"`
		FormatGeneration int    `json:"format_generation"`
		ProcdProtocol    string `json:"procd_protocol"`
		ProcdDigest      string `json:"procd_digest"`
		DataRangeBytes   int    `json:"data_range_bytes"`
		PackBytes        int    `json:"pack_bytes"`
		PageEntries      int    `json:"page_entries"`
	}
	payload, err := json.Marshal(digestInput{
		BuildID: build.ID, TeamID: build.TeamID, HeadLayerID: build.HeadLayerID,
		PinnedOCIRef: build.PinnedOCIRef, SourceOCIDigest: build.SourceOCIDigest,
		OS: build.Platform.OS, Architecture: build.Platform.Architecture, Variant: build.Platform.Variant,
		LogicalSizeBytes: build.LogicalSizeBytes, MutationDigest: build.MutationDigest,
		ObjectPrefix: build.ObjectPrefix, FormatGeneration: contract.FormatGeneration,
		ProcdProtocol: contract.ProcdProtocol, ProcdDigest: contract.ProcdDigest,
		DataRangeBytes: options.DataRangeBytes, PackBytes: options.PackBytes, PageEntries: options.PageEntries,
	})
	if err != nil {
		return MaterializedBuild{}, TargetContract{}, "", err
	}
	return build, contract, digest.FromBytes(payload).String(), nil
}

func validateStoredTargetBuildInput(operation *TargetBuildOperation) error {
	if operation == nil {
		return fmt.Errorf("%w: target build operation is missing", ErrTargetMigrationConflict)
	}
	_, _, inputDigest, err := normalizeTargetBuildInput(operation.Build, operation.Contract)
	if err != nil {
		return fmt.Errorf("%w: durable build inputs are invalid: %v", ErrTargetMigrationConflict, err)
	}
	if inputDigest != operation.InputDigest {
		return fmt.Errorf("%w: durable build input digest does not match", ErrTargetMigrationConflict)
	}
	return nil
}

func validateTargetBuildResult(
	operation *TargetBuildOperation,
	result rootfsimporter.MaterializedGenerationBuildResult,
) error {
	if operation == nil || result.SourceOCIRef != operation.Build.PinnedOCIRef ||
		result.SourceOCIDigest.String() != operation.Build.SourceOCIDigest ||
		result.Platform.OS != operation.Build.Platform.OS ||
		result.Platform.Architecture != operation.Build.Platform.Architecture ||
		result.Platform.Variant != operation.Build.Platform.Variant ||
		result.ProcdDigest.String() != operation.Contract.ProcdDigest ||
		result.LogicalSizeBytes != operation.Build.LogicalSizeBytes ||
		result.MutationDigest.String() != operation.Build.MutationDigest {
		return fmt.Errorf("%w: generation result does not match durable build inputs", ErrTargetMigrationConflict)
	}
	prefix := operation.Build.ObjectPrefix + "/"
	for _, reference := range result.References {
		if !strings.HasPrefix(reference.Key, prefix) {
			return fmt.Errorf("%w: generation object escaped its build prefix", ErrTargetMigrationConflict)
		}
	}
	return nil
}

func verifyReadyBaseArtifact(
	ctx context.Context,
	tx pgx.Tx,
	operation *TargetBuildOperation,
	artifactDigest string,
	result rootfsimporter.MaterializedGenerationBuildResult,
) error {
	var sourceDigest, osName, architecture, variant, procdProtocol, procdDigest, baseRoot string
	var formatGeneration int
	var logicalSize int64
	var descriptor []byte
	err := tx.QueryRow(ctx, `
		SELECT source_oci_digest, oci_os, oci_architecture, oci_variant,
			format_generation, procd_protocol, procd_digest, logical_size_bytes,
			base_block_root, descriptor
		FROM manager.rootfs_base_artifacts
		WHERE artifact_digest = $1 AND state = 'ready'
		FOR UPDATE
	`, artifactDigest).Scan(&sourceDigest, &osName, &architecture, &variant,
		&formatGeneration, &procdProtocol, &procdDigest, &logicalSize, &baseRoot, &descriptor)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: ready Base artifact %s is missing", ErrTargetMigrationConflict, artifactDigest)
	}
	if err != nil {
		return fmt.Errorf("lock target Base artifact: %w", err)
	}
	if sourceDigest != operation.Build.SourceOCIDigest || osName != operation.Build.Platform.OS ||
		architecture != operation.Build.Platform.Architecture || variant != operation.Build.Platform.Variant ||
		formatGeneration != operation.Contract.FormatGeneration || procdProtocol != operation.Contract.ProcdProtocol ||
		procdDigest != operation.Contract.ProcdDigest || logicalSize != operation.Build.LogicalSizeBytes {
		return fmt.Errorf("%w: Base artifact is incompatible with migrated generation", ErrTargetMigrationConflict)
	}
	baseDescriptor, err := rootfsblock.DecodeDescriptor(descriptor)
	if err != nil || baseDescriptor.MappingRoot.RootDigest != baseRoot ||
		baseDescriptor.LogicalSizeBytes != result.Descriptor.LogicalSizeBytes ||
		baseDescriptor.BlockSizeBytes != result.Descriptor.BlockSizeBytes {
		return fmt.Errorf("%w: Base artifact geometry is incompatible with migrated generation", ErrTargetMigrationConflict)
	}
	return nil
}

func verifyReadyTargetBuild(
	ctx context.Context,
	tx pgx.Tx,
	operation *TargetBuildOperation,
	baseArtifactDigest string,
	result rootfsimporter.MaterializedGenerationBuildResult,
) error {
	if operation.BaseArtifactDigest != baseArtifactDigest || operation.Result == nil ||
		operation.Result.DescriptorDigest != result.DescriptorDigest ||
		operation.Result.CurrentBlockHead != result.CurrentBlockHead ||
		!bytes.Equal(operation.Result.DescriptorBytes, result.DescriptorBytes) ||
		operation.Result.Objects != result.Objects || operation.Result.Bytes != result.Bytes {
		return fmt.Errorf("%w: ready build result changed", ErrTargetMigrationConflict)
	}
	for _, reference := range result.References {
		if err := verifyBuildObject(ctx, tx, operation.Build.ID, reference, true); err != nil {
			return err
		}
	}
	rows, err := tx.Query(ctx, `
		SELECT object_key FROM legacy_ack_migration.build_objects
		WHERE build_id = $1 AND result_object ORDER BY object_key
	`, operation.Build.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var selected []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		selected = append(selected, key)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	expected := make([]string, 0, len(result.References))
	for _, reference := range result.References {
		expected = append(expected, reference.Key)
	}
	if !slices.Equal(selected, expected) {
		return fmt.Errorf("%w: ready build object selection changed", ErrTargetMigrationConflict)
	}
	return nil
}

func lockTargetBuildLease(ctx context.Context, tx pgx.Tx, lease TargetBuildLease) error {
	var state, owner, token string
	var current bool
	err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(lease_owner, ''), COALESCE(lease_token, ''),
			COALESCE(lease_expires_at > clock_timestamp(), FALSE)
		FROM legacy_ack_migration.builds WHERE build_id = $1 FOR UPDATE
	`, lease.BuildID).Scan(&state, &owner, &token, &current)
	if err != nil {
		return fmt.Errorf("lock target build lease: %w", err)
	}
	if state != targetBuildStateBuilding || owner != lease.WorkerID || token != lease.Token || !current {
		return fmt.Errorf("%w: %s", ErrTargetBuildLeaseLost, lease.BuildID)
	}
	return nil
}

func verifyBuildObject(
	ctx context.Context,
	tx pgx.Tx,
	buildID string,
	reference rootfsblock.ObjectReference,
	requirePublished bool,
) error {
	var kind, checksum, uploadState string
	var size int64
	var uploadedAt *time.Time
	err := tx.QueryRow(ctx, `
		SELECT object.object_kind, object.object_size, object.checksum,
			object.uploaded_at, link.upload_state
		FROM legacy_ack_migration.build_objects link
		JOIN manager.rootfs_materialization_objects object USING (object_key)
		WHERE link.build_id = $1 AND link.object_key = $2
	`, buildID, reference.Key).Scan(&kind, &size, &checksum, &uploadedAt, &uploadState)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: build object %s was not prepared", ErrTargetMigrationConflict, reference.Key)
	}
	if err != nil {
		return err
	}
	if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum ||
		requirePublished && (uploadState != "published" || uploadedAt == nil) {
		return fmt.Errorf("%w: build object %s metadata or publication state changed", ErrTargetMigrationConflict, reference.Key)
	}
	return nil
}

func verifyMaterializationObject(
	ctx context.Context,
	tx pgx.Tx,
	reference rootfsblock.ObjectReference,
	requirePublished bool,
) error {
	var kind, checksum string
	var size int64
	var uploadedAt *time.Time
	err := tx.QueryRow(ctx, `
		SELECT object_kind, object_size, checksum, uploaded_at
		FROM manager.rootfs_materialization_objects WHERE object_key = $1 FOR UPDATE
	`, reference.Key).Scan(&kind, &size, &checksum, &uploadedAt)
	if err != nil {
		return err
	}
	if kind != reference.Kind || size != reference.Size || checksum != reference.Checksum ||
		requirePublished && uploadedAt == nil {
		return fmt.Errorf("%w: object %s has different immutable metadata", ErrTargetMigrationConflict, reference.Key)
	}
	return nil
}

func normalizeTargetBuildLease(lease TargetBuildLease) (TargetBuildLease, error) {
	lease.BuildID, lease.WorkerID, lease.Token = strings.TrimSpace(lease.BuildID), strings.TrimSpace(lease.WorkerID), strings.TrimSpace(lease.Token)
	if lease.BuildID == "" || lease.WorkerID == "" || len(lease.WorkerID) > 256 ||
		len(lease.Token) != 64 {
		return TargetBuildLease{}, fmt.Errorf("target build lease identity is invalid")
	}
	if _, err := hex.DecodeString(lease.Token); err != nil {
		return TargetBuildLease{}, fmt.Errorf("target build lease token is invalid")
	}
	return lease, nil
}

func newTargetBuildLeaseToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate target build lease token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func validateCanonicalDigest(value string) error {
	parsed, err := digest.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value || len(parsed.Encoded()) != sha256.Size*2 {
		return fmt.Errorf("value must be a canonical SHA-256 digest")
	}
	return nil
}

func commitTargetTx(ctx context.Context, tx pgx.Tx, action string) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit target migration %s: %w", action, err)
	}
	return nil
}

func targetBuildSelectSQL() string {
	return `SELECT ` + targetBuildReturningColumns("build") + ` FROM legacy_ack_migration.builds build`
}

func targetBuildReturningColumns(alias string) string {
	return fmt.Sprintf(`
		%s.build_id, %s.session_id, %s.team_id, %s.head_layer_id,
		%s.pinned_oci_ref, %s.source_oci_digest, %s.oci_os,
		%s.oci_architecture, %s.oci_variant, %s.logical_size_bytes,
		%s.mutation_digest, %s.object_prefix, %s.format_generation,
		%s.procd_protocol, %s.procd_digest, %s.block_data_range_bytes,
		%s.block_pack_bytes, %s.block_page_entries, %s.input_digest,
		%s.state, %s.lease_owner, %s.lease_token, %s.lease_expires_at,
		%s.attempt_count, %s.base_artifact_digest, %s.descriptor_digest,
		%s.current_block_head, %s.descriptor, %s.result_objects,
		%s.result_bytes, %s.created_at, %s.updated_at, %s.ready_at`,
		alias, alias, alias, alias, alias, alias, alias, alias, alias, alias,
		alias, alias, alias, alias, alias, alias, alias, alias, alias, alias,
		alias, alias, alias, alias, alias, alias, alias, alias, alias, alias,
		alias, alias, alias,
	)
}

type targetBuildScanner interface {
	Scan(...any) error
}

func scanTargetBuild(row targetBuildScanner) (*TargetBuildOperation, error) {
	var operation TargetBuildOperation
	var leaseOwner, leaseToken, baseArtifact, descriptorDigest, currentHead *string
	var leaseExpires, readyAt *time.Time
	var descriptor []byte
	var resultObjects *int
	var resultBytes *int64
	err := row.Scan(
		&operation.Build.ID, &operation.SessionID, &operation.Build.TeamID,
		&operation.Build.HeadLayerID, &operation.Build.PinnedOCIRef,
		&operation.Build.SourceOCIDigest, &operation.Build.Platform.OS,
		&operation.Build.Platform.Architecture, &operation.Build.Platform.Variant,
		&operation.Build.LogicalSizeBytes, &operation.Build.MutationDigest,
		&operation.Build.ObjectPrefix, &operation.Contract.FormatGeneration,
		&operation.Contract.ProcdProtocol, &operation.Contract.ProcdDigest,
		&operation.Contract.BlockOptions.DataRangeBytes, &operation.Contract.BlockOptions.PackBytes,
		&operation.Contract.BlockOptions.PageEntries, &operation.InputDigest,
		&operation.State, &leaseOwner, &leaseToken, &leaseExpires,
		&operation.AttemptCount, &baseArtifact, &descriptorDigest, &currentHead,
		&descriptor, &resultObjects, &resultBytes,
		&operation.CreatedAt, &operation.UpdatedAt, &readyAt,
	)
	if err != nil {
		return nil, err
	}
	operation.Contract.BlockOptions.ObjectPrefix = operation.Build.ObjectPrefix
	if leaseOwner != nil {
		operation.LeaseOwner = *leaseOwner
	}
	if leaseToken != nil {
		operation.LeaseToken = *leaseToken
	}
	if leaseExpires != nil {
		operation.LeaseExpiresAt = *leaseExpires
	}
	if baseArtifact != nil {
		operation.BaseArtifactDigest = *baseArtifact
	}
	if readyAt != nil {
		operation.ReadyAt = *readyAt
	}
	if descriptor != nil && descriptorDigest != nil && currentHead != nil && resultObjects != nil && resultBytes != nil {
		descriptorValue, err := rootfsblock.DecodeDescriptor(descriptor)
		if err != nil {
			return nil, err
		}
		descriptorDigestValue, err := digest.Parse(*descriptorDigest)
		if err != nil {
			return nil, err
		}
		currentHeadValue, err := digest.Parse(*currentHead)
		if err != nil {
			return nil, err
		}
		mutationDigest, err := digest.Parse(operation.Build.MutationDigest)
		if err != nil {
			return nil, err
		}
		sourceDigest, err := digest.Parse(operation.Build.SourceOCIDigest)
		if err != nil {
			return nil, err
		}
		procdDigest, err := digest.Parse(operation.Contract.ProcdDigest)
		if err != nil {
			return nil, err
		}
		operation.Result = &rootfsimporter.MaterializedGenerationBuildResult{
			SourceOCIRef: operation.Build.PinnedOCIRef, SourceOCIDigest: sourceDigest,
			Platform: operation.Build.Platform, ProcdDigest: procdDigest,
			LogicalSizeBytes: operation.Build.LogicalSizeBytes, MutationDigest: mutationDigest,
			DescriptorDigest: descriptorDigestValue, CurrentBlockHead: currentHeadValue,
			Descriptor: descriptorValue, DescriptorBytes: append([]byte(nil), descriptor...),
			Objects: *resultObjects, Bytes: *resultBytes,
		}
	}
	return &operation, nil
}

var _ rootfsimporter.ObjectPublicationJournal = targetBuildJournal{}
