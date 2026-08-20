package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	digest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	RootFSStorageFormatLegacyLayer = "legacy-layer"
	RootFSStorageFormatBlockCOWV1  = "block-cow-v1"

	RootFSBaseArtifactStateReady = "ready"

	RootFSGenerationStateLocalSealed      = "local_sealed"
	RootFSGenerationStateCompositeDurable = "composite_durable"
	RootFSGenerationStateS3Materialized   = "s3_materialized"
	RootFSGenerationDescriptorMaxBytes    = 64 << 10
)

var rootFSPlatformPartPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

var (
	ErrRootFSBaseArtifactNotFound = errors.New("rootfs base artifact not found")
	ErrRootFSBaseArtifactConflict = errors.New("rootfs base artifact conflict")
	ErrRootFSGenerationConflict   = errors.New("rootfs generation conflict")
)

// PublishPausedRootFSRebaseRequest publishes the output of a privileged,
// file-aware rebase worker. The worker must have already persisted the target
// block objects and supplied a health-check digest over its immutable output.
type PublishPausedRootFSRebaseRequest struct {
	SandboxID                  string
	TeamID                     string
	OperationID                string
	ExpectedSourceGenerationID string
	ExpectedBaseArtifactDigest string
	Generation                 *RootFSGeneration
	HealthCheckDigest          []byte
	RollbackExpiresAt          time.Time
}

// RootFSBaseArtifact is an immutable, trusted OCI-to-block conversion shared
// by filesystems in one region. The descriptor is bounded control metadata;
// block data and mapping pages remain in the region object store.
type RootFSBaseArtifact struct {
	ArtifactDigest   string
	SourceOCIRef     string
	SourceOCIDigest  string
	BaseBlockRoot    string
	FormatGeneration int
	Platform         RootFSArtifactPlatform
	State            string
	Descriptor       []byte
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// RootFSArtifactPlatform is the OCI platform selected while converting an
// image into an immutable block artifact.
type RootFSArtifactPlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

// Validate rejects unattested or non-canonical Linux platform identities.
func (p RootFSArtifactPlatform) Validate() error {
	if p.OS != "linux" {
		return fmt.Errorf("rootfs artifact operating system must be linux")
	}
	if !rootFSPlatformPartPattern.MatchString(p.Architecture) {
		return fmt.Errorf("rootfs artifact architecture must be canonical")
	}
	if p.Variant != "" && !rootFSPlatformPartPattern.MatchString(p.Variant) {
		return fmt.Errorf("rootfs artifact architecture variant must be canonical")
	}
	return nil
}

// RootFSGeneration is one immutable durable block-map generation. It is not a
// legacy OCI diff layer and must never be inserted into rootfs_layers.
type RootFSGeneration struct {
	ID                 string
	FilesystemID       string
	ParentGenerationID string
	SourceOCIDigest    string
	BaseArtifactDigest string
	BaseBlockRoot      string
	CurrentBlockHead   string
	WriterEpoch        int64
	FormatGeneration   int
	DurabilityState    string
	LocatorVersion     int64
	Descriptor         []byte
	CreatedAt          time.Time
}

type PutReadyRootFSBaseArtifactRequest struct {
	ArtifactDigest   string
	SourceOCIRef     string
	SourceOCIDigest  string
	BaseBlockRoot    string
	FormatGeneration int
	Platform         RootFSArtifactPlatform
	Descriptor       []byte
}

type EnsureInitialRootFSGenerationRequest struct {
	SandboxID          string
	TeamID             string
	SourceOCIRef       string
	SourceOCIDigest    string
	BaseArtifactDigest string
}

// RootFSGenerationStore is kept separate from SandboxStore while the legacy
// diff-layer product remains available during the format migration.
type RootFSGenerationStore interface {
	PutReadyRootFSBaseArtifact(context.Context, *PutReadyRootFSBaseArtifactRequest) (*RootFSBaseArtifact, error)
	GetReadyRootFSBaseArtifact(context.Context, string, RootFSArtifactPlatform, int) (*RootFSBaseArtifact, error)
	GetReadyRootFSBaseArtifactByDigest(context.Context, string, RootFSArtifactPlatform) (*RootFSBaseArtifact, error)
	EnsureInitialRootFSGeneration(context.Context, *EnsureInitialRootFSGenerationRequest) (*RootFSFilesystem, *RootFSGeneration, error)
	GetRootFSGeneration(context.Context, string) (*RootFSGeneration, error)
	ForkRunningRootFSFilesystem(context.Context, *ForkRunningRootFSFilesystemRequest) (*RootFSFilesystem, error)
	RequestNomadPausedRebase(context.Context, *NomadPausedRebaseRequest) (*NomadPausedRebaseCandidate, error)
	PublishPausedRootFSRebase(context.Context, *PublishPausedRootFSRebaseRequest) (*RootFSFilesystem, error)
}

// PublishPausedRootFSRebase atomically installs a durable rebase generation
// only if the paused source head and Base artifact still match the worker's
// inputs. Physical migration is intentionally outside this method; this is the
// regional CAS, writer-fence, and rollback-retention boundary.
func (s *PGSandboxStore) PublishPausedRootFSRebase(
	ctx context.Context,
	req *PublishPausedRootFSRebaseRequest,
) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := validatePublishPausedRootFSRebaseRequest(req)
	if err != nil {
		return nil, err
	}
	var published *RootFSFilesystem
	err = s.WithSandboxLock(ctx, normalized.SandboxID, func(lockCtx context.Context, locked SandboxStoreTx, record *SandboxRecord) error {
		txStore, ok := locked.(sandboxStoreTx)
		if !ok {
			return fmt.Errorf("paused rootfs rebase requires a PostgreSQL transaction")
		}
		if record == nil || record.TeamID != normalized.TeamID || record.RuntimeBackend != SandboxRuntimeBackendNomad ||
			record.DesiredState != SandboxDesiredStatePaused || !record.DeletedAt.IsZero() ||
			record.CurrentPodNamespace != "" || record.CurrentPodName != "" {
			return fmt.Errorf("%w: sandbox %s is not a paused team-owned source", ErrRootFSGenerationConflict, normalized.SandboxID)
		}
		lifecycle, lifecycleErr := scanLifecycleTxn(txStore.tx.QueryRow(lockCtx, lifecycleTxnSelectSQL()+`
			WHERE txn_id = $1 AND sandbox_id = $2
			FOR UPDATE
		`, normalized.OperationID, normalized.SandboxID))
		if lifecycleErr != nil {
			return fmt.Errorf("lock paused rootfs rebase lifecycle: %w", lifecycleErr)
		}
		if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindRebase {
			return fmt.Errorf("%w: exact rebase pre-operation is missing", ErrRootFSGenerationConflict)
		}

		filesystem, source, loadErr := getRootFSFilesystemAndGenerationForUpdate(lockCtx, txStore.tx, normalized.SandboxID)
		if loadErr != nil {
			return loadErr
		}
		if lifecycle.Phase == SandboxLifecyclePhaseCommitted {
			if !committedNomadPausedRebaseLifecycleMatchesRequest(lifecycle, record, normalized) {
				return fmt.Errorf("%w: committed rebase lifecycle identity changed", ErrRootFSGenerationConflict)
			}
			if retry, retryErr := loadPublishedRootFSRebaseRetry(lockCtx, txStore.tx, filesystem, normalized); retryErr != nil {
				return retryErr
			} else if retry != nil {
				published = retry
				return nil
			}
			return fmt.Errorf("%w: committed rebase publication is missing", ErrRootFSGenerationConflict)
		}
		if filesystem.StorageFormat != RootFSStorageFormatBlockCOWV1 || filesystem.TeamID != normalized.TeamID ||
			filesystem.HeadGenerationID != normalized.ExpectedSourceGenerationID ||
			filesystem.BaseArtifactDigest != normalized.ExpectedBaseArtifactDigest ||
			source.ID != normalized.ExpectedSourceGenerationID ||
			(source.DurabilityState != RootFSGenerationStateCompositeDurable && source.DurabilityState != RootFSGenerationStateS3Materialized) {
			return fmt.Errorf("%w: paused source head or Base artifact changed", ErrRootFSGenerationConflict)
		}
		sourceArtifact, artifact, artifactErr := lockNomadPausedRebaseArtifacts(
			lockCtx, txStore.tx, source.BaseArtifactDigest, normalized.Generation.BaseArtifactDigest,
		)
		if artifactErr != nil {
			return artifactErr
		}
		if artifactErr := validateNomadPausedRebaseArtifacts(source, sourceArtifact, artifact); artifactErr != nil {
			return artifactErr
		}
		if !nomadPausedRebaseLifecycleMatches(
			lifecycle, record, source, sourceArtifact, artifact,
			normalized.Generation.ID, normalized.RollbackExpiresAt, false,
		) || lifecycle.ID != normalized.OperationID ||
			normalized.ExpectedSourceGenerationID != lifecycle.ExpectedHeadLayerID ||
			normalized.ExpectedBaseArtifactDigest != lifecycle.SourceBaseArtifactDigest {
			return fmt.Errorf("%w: active rebase lifecycle identity changed", ErrRootFSGenerationConflict)
		}
		if normalized.Generation.FilesystemID != filesystem.ID ||
			normalized.Generation.ParentGenerationID != source.ID ||
			normalized.Generation.WriterEpoch != filesystem.WriterEpoch+1 ||
			normalized.Generation.BaseArtifactDigest == source.BaseArtifactDigest {
			return fmt.Errorf("%w: target generation does not describe the next rebase branch", ErrRootFSGenerationConflict)
		}
		if artifact.SourceOCIDigest != normalized.Generation.SourceOCIDigest ||
			artifact.BaseBlockRoot != normalized.Generation.BaseBlockRoot ||
			artifact.FormatGeneration != normalized.Generation.FormatGeneration {
			return fmt.Errorf("%w: target generation does not match its Base artifact", ErrRootFSBaseArtifactConflict)
		}
		if outputErr := validateNomadPausedRebaseOutput(normalized.Generation, artifact); outputErr != nil {
			return outputErr
		}
		claim, claimErr := lockSandboxRuntimeClaim(lockCtx, txStore.tx, record.ID)
		if claimErr != nil {
			return claimErr
		}
		if claim.OperationID == "" || claim.Phase != SandboxRuntimeClaimPhaseReady ||
			!claim.LeaseExpiresAt.IsZero() || !claim.CleanupStartedAt.IsZero() || !claim.CleanedAt.IsZero() {
			return fmt.Errorf("%w: sandbox runtime claim changed", ErrRootFSGenerationConflict)
		}
		if terminalErr := ensureNomadPausedRebasePhysicalStateTerminal(
			lockCtx, txStore.tx, record.ID, filesystem.ID,
		); terminalErr != nil {
			return fmt.Errorf("%w: %v", ErrRootFSGenerationConflict, terminalErr)
		}
		var authorityNow time.Time
		if authorityErr := txStore.tx.QueryRow(lockCtx, `SELECT NOW()`).Scan(&authorityNow); authorityErr != nil {
			return fmt.Errorf("read paused rootfs rebase authority time: %w", authorityErr)
		}
		if !normalized.RollbackExpiresAt.After(authorityNow) ||
			(!record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow)) {
			return fmt.Errorf("%w: rebase rollback or sandbox hard deadline expired", ErrRootFSGenerationConflict)
		}
		if insertErr := insertPreparedRootFSGeneration(lockCtx, txStore.tx, normalized.Generation); insertErr != nil {
			return insertErr
		}
		tag, updateErr := txStore.tx.Exec(lockCtx, `
			UPDATE manager.rootfs_filesystems
			SET head_generation_id = $1,
				writer_epoch = $2,
				base_artifact_digest = $3,
				format_generation = $4,
				base_image_ref = $5,
				base_image_digest = $6,
				updated_at = NOW()
			WHERE filesystem_id = $7
				AND head_generation_id = $8
				AND base_artifact_digest = $9
				AND writer_epoch = $10
		`, normalized.Generation.ID, normalized.Generation.WriterEpoch,
			normalized.Generation.BaseArtifactDigest, normalized.Generation.FormatGeneration,
			artifact.SourceOCIRef, normalized.Generation.SourceOCIDigest, filesystem.ID,
			normalized.ExpectedSourceGenerationID, normalized.ExpectedBaseArtifactDigest,
			filesystem.WriterEpoch)
		if updateErr != nil {
			return fmt.Errorf("publish paused rootfs rebase: %w", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: paused source head changed during publish", ErrRootFSGenerationConflict)
		}
		if _, pinErr := txStore.tx.Exec(lockCtx, `
			INSERT INTO manager.rootfs_head_rollbacks (
				operation_id, filesystem_id, sandbox_id, team_id, operation_kind,
				old_generation_id, new_generation_id, health_check_digest,
				state, created_at, expires_at
			) VALUES ($1, $2, $3, $4, 'rebase', $5, $6, $7, 'available', NOW(), $8)
		`, normalized.OperationID, filesystem.ID, normalized.SandboxID, normalized.TeamID,
			source.ID, normalized.Generation.ID, normalized.HealthCheckDigest,
			nullableTime(normalized.RollbackExpiresAt)); pinErr != nil {
			return fmt.Errorf("retain paused rootfs rebase rollback: %w", pinErr)
		}
		if lifecycleErr := locked.CommitLifecycleTxn(
			lockCtx, lifecycle.ID, normalized.Generation.ID,
		); lifecycleErr != nil {
			return fmt.Errorf("commit paused rootfs rebase lifecycle: %w", lifecycleErr)
		}
		published, loadErr = scanRootFSFilesystem(txStore.tx.QueryRow(lockCtx, `
			SELECT filesystem_id, team_id, source_filesystem_id, head_layer_id,
				writer_epoch, base_image_ref, base_image_digest, storage_format,
				base_artifact_digest, format_generation, head_generation_id,
				created_at, updated_at
			FROM manager.rootfs_filesystems
			WHERE filesystem_id = $1
		`, filesystem.ID))
		return loadErr
	})
	if err != nil {
		return nil, err
	}
	return published, nil
}

func (s *PGSandboxStore) PutReadyRootFSBaseArtifact(
	ctx context.Context,
	req *PutReadyRootFSBaseArtifactRequest,
) (*RootFSBaseArtifact, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := validateReadyRootFSBaseArtifact(req)
	if err != nil {
		return nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.rootfs_base_artifacts (
			artifact_digest, source_oci_ref, source_oci_digest, base_block_root,
			format_generation, oci_os, oci_architecture, oci_variant,
			state, descriptor, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
		ON CONFLICT (artifact_digest) DO NOTHING
	`, normalized.ArtifactDigest, normalized.SourceOCIRef, normalized.SourceOCIDigest,
		normalized.BaseBlockRoot, normalized.FormatGeneration, normalized.Platform.OS,
		normalized.Platform.Architecture, normalized.Platform.Variant,
		RootFSBaseArtifactStateReady, normalized.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("put ready rootfs base artifact: %w", err)
	}
	artifact, err := scanRootFSBaseArtifact(s.pool.QueryRow(ctx, rootFSBaseArtifactSelectSQL()+`
		WHERE artifact_digest = $1
	`, normalized.ArtifactDigest))
	if err != nil {
		return nil, fmt.Errorf("read ready rootfs base artifact: %w", err)
	}
	if !rootFSBaseArtifactMatchesRequest(artifact, normalized) {
		return nil, fmt.Errorf("%w: artifact %s has different immutable fields",
			ErrRootFSBaseArtifactConflict, normalized.ArtifactDigest)
	}
	return artifact, nil
}

func (s *PGSandboxStore) GetReadyRootFSBaseArtifact(
	ctx context.Context,
	sourceOCIDigest string,
	platform RootFSArtifactPlatform,
	formatGeneration int,
) (*RootFSBaseArtifact, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	sourceOCIDigest = strings.TrimSpace(sourceOCIDigest)
	if _, err := digest.Parse(sourceOCIDigest); err != nil {
		return nil, fmt.Errorf("source_oci_digest: %w", err)
	}
	if err := platform.Validate(); err != nil {
		return nil, err
	}
	artifact, err := scanRootFSBaseArtifact(s.pool.QueryRow(ctx, rootFSBaseArtifactSelectSQL()+`
		WHERE source_oci_digest = $1
			AND state = $2
			AND oci_os = $3
			AND oci_architecture = $4
			AND oci_variant = $5
			AND ($6 = 0 OR format_generation = $6)
		ORDER BY format_generation DESC, created_at DESC
		LIMIT 1
	`, sourceOCIDigest, RootFSBaseArtifactStateReady, platform.OS,
		platform.Architecture, platform.Variant, formatGeneration))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: source %s", ErrRootFSBaseArtifactNotFound, sourceOCIDigest)
	}
	if err != nil {
		return nil, fmt.Errorf("get ready rootfs base artifact: %w", err)
	}
	return artifact, nil
}

func (s *PGSandboxStore) GetReadyRootFSBaseArtifactByDigest(
	ctx context.Context,
	artifactDigest string,
	platform RootFSArtifactPlatform,
) (*RootFSBaseArtifact, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	artifactDigest = strings.TrimSpace(artifactDigest)
	if _, err := digest.Parse(artifactDigest); err != nil {
		return nil, fmt.Errorf("artifact_digest: %w", err)
	}
	if err := platform.Validate(); err != nil {
		return nil, err
	}
	artifact, err := scanRootFSBaseArtifact(s.pool.QueryRow(ctx, rootFSBaseArtifactSelectSQL()+`
		WHERE artifact_digest = $1
			AND state = $2
			AND oci_os = $3
			AND oci_architecture = $4
			AND oci_variant = $5
	`, artifactDigest, RootFSBaseArtifactStateReady, platform.OS,
		platform.Architecture, platform.Variant))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: artifact %s for %s/%s/%s", ErrRootFSBaseArtifactNotFound,
			artifactDigest, platform.OS, platform.Architecture, platform.Variant)
	}
	if err != nil {
		return nil, fmt.Errorf("get ready rootfs base artifact by digest: %w", err)
	}
	return artifact, nil
}

func (s *PGSandboxStore) EnsureInitialRootFSGeneration(
	ctx context.Context,
	req *EnsureInitialRootFSGenerationRequest,
) (*RootFSFilesystem, *RootFSGeneration, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		filesystem, generation, err := s.ensureInitialRootFSGenerationOnce(ctx, req)
		if err == nil || !isSerializationFailure(err) {
			return filesystem, generation, err
		}
		lastErr = err
		delay := time.Duration(1<<attempt) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, nil, fmt.Errorf("initial rootfs generation serialization retries exhausted: %w", lastErr)
}

func (s *PGSandboxStore) ensureInitialRootFSGenerationOnce(
	ctx context.Context,
	req *EnsureInitialRootFSGenerationRequest,
) (*RootFSFilesystem, *RootFSGeneration, error) {
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := validateEnsureInitialRootFSGeneration(req)
	if err != nil {
		return nil, nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, nil, fmt.Errorf("begin initial rootfs generation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var sandboxTeamID string
	if err := tx.QueryRow(ctx, `
		SELECT team_id
		FROM manager.sandboxes
		WHERE sandbox_id = $1 AND deleted_at IS NULL
		FOR UPDATE
	`, normalized.SandboxID).Scan(&sandboxTeamID); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, normalized.SandboxID)
	} else if err != nil {
		return nil, nil, fmt.Errorf("lock sandbox for initial rootfs generation: %w", err)
	}
	if sandboxTeamID != normalized.TeamID {
		return nil, nil, fmt.Errorf("%w: sandbox team %s does not match %s",
			ErrRootFSGenerationConflict, sandboxTeamID, normalized.TeamID)
	}

	artifact, err := scanRootFSBaseArtifact(tx.QueryRow(ctx, rootFSBaseArtifactSelectSQL()+`
		WHERE artifact_digest = $1 AND state = $2
		FOR UPDATE
	`, normalized.BaseArtifactDigest, RootFSBaseArtifactStateReady))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, fmt.Errorf("%w: %s", ErrRootFSBaseArtifactNotFound, normalized.BaseArtifactDigest)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("lock rootfs base artifact: %w", err)
	}
	if artifact.SourceOCIDigest != normalized.SourceOCIDigest || artifact.SourceOCIRef != normalized.SourceOCIRef {
		return nil, nil, fmt.Errorf("%w: artifact %s does not match source image",
			ErrRootFSBaseArtifactConflict, artifact.ArtifactDigest)
	}

	filesystem, generation, err := getInitialRootFSGenerationForSandbox(ctx, tx, normalized.SandboxID)
	if err == nil {
		if !initialRootFSGenerationMatches(filesystem, generation, artifact, normalized) {
			return nil, nil, fmt.Errorf("%w: sandbox %s is already bound to another initial generation",
				ErrRootFSGenerationConflict, normalized.SandboxID)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, fmt.Errorf("commit existing initial rootfs generation: %w", err)
		}
		return filesystem, generation, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, err
	}

	filesystemID := normalized.SandboxID
	generationID := initialRootFSGenerationID(filesystemID, artifact.ArtifactDigest, artifact.FormatGeneration)
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (
			filesystem_id, team_id, storage_format, base_artifact_digest,
			format_generation, base_image_ref, base_image_digest, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
	`, filesystemID, normalized.TeamID, RootFSStorageFormatBlockCOWV1,
		artifact.ArtifactDigest, artifact.FormatGeneration, normalized.SourceOCIRef,
		normalized.SourceOCIDigest); err != nil {
		return nil, nil, fmt.Errorf("create block-cow rootfs filesystem: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_bindings (
			sandbox_id, filesystem_id, team_id, created_at, updated_at
		) VALUES ($1, $2, $3, NOW(), NOW())
	`, normalized.SandboxID, filesystemID, normalized.TeamID); err != nil {
		return nil, nil, fmt.Errorf("bind initial rootfs filesystem: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_generations (
			generation_id, filesystem_id, source_oci_digest, base_artifact_digest,
			base_block_root, current_block_head, writer_epoch, format_generation,
			durability_state, locator_version, descriptor, created_at
		) VALUES ($1, $2, $3, $4, $5, $5, 0, $6, $7, 1, $8, NOW())
	`, generationID, filesystemID, normalized.SourceOCIDigest, artifact.ArtifactDigest,
		artifact.BaseBlockRoot, artifact.FormatGeneration, RootFSGenerationStateS3Materialized,
		artifact.Descriptor); err != nil {
		return nil, nil, fmt.Errorf("create initial rootfs generation: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_generation_id = $2, updated_at = NOW()
		WHERE filesystem_id = $1 AND head_generation_id IS NULL
	`, filesystemID, generationID); err != nil {
		return nil, nil, fmt.Errorf("publish initial rootfs generation: %w", err)
	}

	filesystem, generation, err = getInitialRootFSGenerationForSandbox(ctx, tx, normalized.SandboxID)
	if err != nil {
		return nil, nil, fmt.Errorf("read created initial rootfs generation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit initial rootfs generation: %w", err)
	}
	return filesystem, generation, nil
}

func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func (s *PGSandboxStore) GetRootFSGeneration(ctx context.Context, generationID string) (*RootFSGeneration, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	generation, err := scanRootFSGeneration(s.pool.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1
	`, strings.TrimSpace(generationID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSFilesystemNotFound, generationID)
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs generation: %w", err)
	}
	return generation, nil
}

func getInitialRootFSGenerationForSandbox(
	ctx context.Context,
	query interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	sandboxID string,
) (*RootFSFilesystem, *RootFSGeneration, error) {
	row := query.QueryRow(ctx, `
		SELECT f.filesystem_id, f.team_id, f.source_filesystem_id, f.head_layer_id,
			f.writer_epoch, f.base_image_ref, f.base_image_digest, f.storage_format,
			f.base_artifact_digest, f.format_generation, f.head_generation_id,
			f.created_at, f.updated_at,
			g.generation_id, g.filesystem_id, g.parent_generation_id,
			g.source_oci_digest, g.base_artifact_digest, g.base_block_root,
			g.current_block_head, g.writer_epoch, g.format_generation,
			g.durability_state, g.locator_version, g.descriptor, g.created_at
		FROM manager.sandbox_rootfs_bindings b
		JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
		JOIN manager.rootfs_generations g ON g.generation_id = f.head_generation_id
		WHERE b.sandbox_id = $1
		FOR UPDATE OF f
	`, sandboxID)
	filesystem, generation, err := scanRootFSFilesystemAndGeneration(row)
	return filesystem, generation, err
}

func validateReadyRootFSBaseArtifact(req *PutReadyRootFSBaseArtifactRequest) (*PutReadyRootFSBaseArtifactRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("rootfs base artifact request is required")
	}
	normalized := *req
	normalized.ArtifactDigest = strings.TrimSpace(req.ArtifactDigest)
	normalized.SourceOCIRef = strings.TrimSpace(req.SourceOCIRef)
	normalized.SourceOCIDigest = strings.TrimSpace(req.SourceOCIDigest)
	normalized.BaseBlockRoot = strings.TrimSpace(req.BaseBlockRoot)
	normalized.Descriptor = append([]byte(nil), req.Descriptor...)
	for field, value := range map[string]string{
		"source_oci_ref": normalized.SourceOCIRef, "base_block_root": normalized.BaseBlockRoot,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", field)
		}
	}
	for field, value := range map[string]string{
		"artifact_digest": normalized.ArtifactDigest, "source_oci_digest": normalized.SourceOCIDigest,
	} {
		if _, err := digest.Parse(value); err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
	}
	if normalized.FormatGeneration <= 0 {
		return nil, fmt.Errorf("format_generation must be positive")
	}
	if err := normalized.Platform.Validate(); err != nil {
		return nil, err
	}
	if len(normalized.Descriptor) == 0 || len(normalized.Descriptor) > RootFSGenerationDescriptorMaxBytes {
		return nil, fmt.Errorf("descriptor must contain 1..%d bytes", RootFSGenerationDescriptorMaxBytes)
	}
	descriptor, err := rootfsblock.DecodeDescriptor(normalized.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("descriptor: %w", err)
	}
	if descriptor.MappingRoot.RootDigest != normalized.BaseBlockRoot || descriptor.CompositeTail != nil {
		return nil, fmt.Errorf("base artifact descriptor must point at the exact S3-materialized base block root")
	}
	return &normalized, nil
}

func validateEnsureInitialRootFSGeneration(req *EnsureInitialRootFSGenerationRequest) (*EnsureInitialRootFSGenerationRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("initial rootfs generation request is required")
	}
	normalized := *req
	normalized.SandboxID = strings.TrimSpace(req.SandboxID)
	normalized.TeamID = strings.TrimSpace(req.TeamID)
	normalized.SourceOCIRef = strings.TrimSpace(req.SourceOCIRef)
	normalized.SourceOCIDigest = strings.TrimSpace(req.SourceOCIDigest)
	normalized.BaseArtifactDigest = strings.TrimSpace(req.BaseArtifactDigest)
	for field, value := range map[string]string{
		"sandbox_id": normalized.SandboxID, "team_id": normalized.TeamID,
		"source_oci_ref": normalized.SourceOCIRef,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", field)
		}
	}
	for field, value := range map[string]string{
		"source_oci_digest": normalized.SourceOCIDigest, "base_artifact_digest": normalized.BaseArtifactDigest,
	} {
		if _, err := digest.Parse(value); err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
	}
	return &normalized, nil
}

func validatePublishPausedRootFSRebaseRequest(req *PublishPausedRootFSRebaseRequest) (*PublishPausedRootFSRebaseRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("publish paused rootfs rebase request is required")
	}
	normalized := *req
	normalized.SandboxID = strings.TrimSpace(req.SandboxID)
	normalized.TeamID = strings.TrimSpace(req.TeamID)
	normalized.OperationID = strings.TrimSpace(req.OperationID)
	normalized.ExpectedSourceGenerationID = strings.TrimSpace(req.ExpectedSourceGenerationID)
	normalized.ExpectedBaseArtifactDigest = strings.TrimSpace(req.ExpectedBaseArtifactDigest)
	normalized.HealthCheckDigest = append([]byte(nil), req.HealthCheckDigest...)
	if req.RollbackExpiresAt.IsZero() {
		return nil, fmt.Errorf("rollback_expires_at is required")
	}
	normalized.RollbackExpiresAt = req.RollbackExpiresAt.UTC().Truncate(time.Microsecond)
	if normalized.SandboxID == "" || normalized.TeamID == "" || normalized.OperationID == "" ||
		normalized.ExpectedSourceGenerationID == "" || normalized.ExpectedBaseArtifactDigest == "" {
		return nil, fmt.Errorf("sandbox_id, team_id, operation_id, expected source generation, and expected Base artifact are required")
	}
	if parsed, err := digest.Parse(normalized.ExpectedBaseArtifactDigest); err != nil ||
		parsed.Algorithm() != digest.SHA256 || parsed.String() != normalized.ExpectedBaseArtifactDigest {
		return nil, fmt.Errorf("expected_base_artifact_digest must be a canonical sha256 digest")
	}
	if len(normalized.HealthCheckDigest) != sha256.Size {
		return nil, fmt.Errorf("health_check_digest must be a 32-byte SHA-256 digest")
	}
	generation, err := normalizeDurableRootFSGeneration(req.Generation, normalized.ExpectedSourceGenerationID)
	if err != nil {
		return nil, err
	}
	normalized.Generation = generation
	return &normalized, nil
}

func normalizeDurableRootFSGeneration(input *RootFSGeneration, expectedParent string) (*RootFSGeneration, error) {
	if input == nil {
		return nil, fmt.Errorf("generation is required")
	}
	generation := *input
	generation.ID = strings.TrimSpace(generation.ID)
	generation.FilesystemID = strings.TrimSpace(generation.FilesystemID)
	generation.ParentGenerationID = strings.TrimSpace(generation.ParentGenerationID)
	generation.SourceOCIDigest = strings.TrimSpace(generation.SourceOCIDigest)
	generation.BaseArtifactDigest = strings.TrimSpace(generation.BaseArtifactDigest)
	generation.BaseBlockRoot = strings.TrimSpace(generation.BaseBlockRoot)
	generation.CurrentBlockHead = strings.TrimSpace(generation.CurrentBlockHead)
	generation.DurabilityState = strings.TrimSpace(generation.DurabilityState)
	generation.Descriptor = append([]byte(nil), generation.Descriptor...)
	for name, value := range map[string]string{
		"generation_id": generation.ID, "filesystem_id": generation.FilesystemID,
		"parent_generation_id": generation.ParentGenerationID,
		"source_oci_digest":    generation.SourceOCIDigest,
		"base_artifact_digest": generation.BaseArtifactDigest,
		"base_block_root":      generation.BaseBlockRoot,
		"current_block_head":   generation.CurrentBlockHead,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"source_oci_digest": generation.SourceOCIDigest, "base_artifact_digest": generation.BaseArtifactDigest,
		"base_block_root": generation.BaseBlockRoot, "current_block_head": generation.CurrentBlockHead,
	} {
		parsed, err := digest.Parse(value)
		if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
			return nil, fmt.Errorf("%s must be a canonical sha256 digest", name)
		}
	}
	if generation.ParentGenerationID != strings.TrimSpace(expectedParent) || generation.WriterEpoch <= 0 ||
		generation.FormatGeneration <= 0 || generation.LocatorVersion <= 0 {
		return nil, fmt.Errorf("generation parent, epoch, format, or locator version is invalid")
	}
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("generation descriptor: %w", err)
	}
	if descriptor.MappingRoot.RootDigest != generation.CurrentBlockHead {
		return nil, fmt.Errorf("generation descriptor does not match current_block_head")
	}
	switch generation.DurabilityState {
	case RootFSGenerationStateS3Materialized:
		if descriptor.CompositeTail != nil {
			return nil, fmt.Errorf("s3_materialized generation cannot contain a composite tail")
		}
	case RootFSGenerationStateCompositeDurable:
		if descriptor.CompositeTail == nil {
			return nil, fmt.Errorf("composite_durable generation requires a composite tail")
		}
	default:
		return nil, fmt.Errorf("unsupported generation durability state %q", generation.DurabilityState)
	}
	return &generation, nil
}

func getRootFSFilesystemAndGenerationForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID string,
) (*RootFSFilesystem, *RootFSGeneration, error) {
	filesystem, generation, err := scanRootFSFilesystemAndGeneration(tx.QueryRow(ctx, `
		SELECT f.filesystem_id, f.team_id, f.source_filesystem_id, f.head_layer_id,
			f.writer_epoch, f.base_image_ref, f.base_image_digest, f.storage_format,
			f.base_artifact_digest, f.format_generation, f.head_generation_id,
			f.created_at, f.updated_at,
			g.generation_id, g.filesystem_id, g.parent_generation_id,
			g.source_oci_digest, g.base_artifact_digest, g.base_block_root,
			g.current_block_head, g.writer_epoch, g.format_generation,
			g.durability_state, g.locator_version, g.descriptor, g.created_at
		FROM manager.sandbox_rootfs_bindings binding
		JOIN manager.rootfs_filesystems f ON f.filesystem_id = binding.filesystem_id
		JOIN manager.rootfs_generations g ON g.generation_id = f.head_generation_id
		WHERE binding.sandbox_id = $1
		FOR UPDATE OF f, g
	`, sandboxID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, fmt.Errorf("%w: sandbox %s has no block generation", ErrRootFSGenerationConflict, sandboxID)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("lock paused rootfs rebase source: %w", err)
	}
	return filesystem, generation, nil
}

func loadPublishedRootFSRebaseRetry(
	ctx context.Context,
	tx pgx.Tx,
	filesystem *RootFSFilesystem,
	req *PublishPausedRootFSRebaseRequest,
) (*RootFSFilesystem, error) {
	if filesystem == nil || req == nil || req.Generation == nil {
		return nil, nil
	}
	var expiresAt *time.Time
	var state string
	if err := tx.QueryRow(ctx, `
		SELECT state, expires_at
		FROM manager.rootfs_head_rollbacks
		WHERE operation_id = $1 AND filesystem_id = $2 AND sandbox_id = $3
			AND team_id = $4 AND operation_kind = 'rebase'
			AND old_generation_id = $5 AND new_generation_id = $6
			AND health_check_digest = $7
	`, req.OperationID, filesystem.ID, req.SandboxID, req.TeamID,
		req.ExpectedSourceGenerationID, req.Generation.ID, req.HealthCheckDigest).Scan(&state, &expiresAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("check paused rootfs rebase retry: %w", err)
	}
	if expiresAt == nil || !expiresAt.Equal(req.RollbackExpiresAt) ||
		(state != "available" && state != "rolled_back" && state != "expired") {
		return nil, fmt.Errorf("%w: retried rollback identity changed", ErrRootFSGenerationConflict)
	}
	stored, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1
	`, req.Generation.ID))
	if err != nil {
		return nil, fmt.Errorf("load retried rootfs rebase generation: %w", err)
	}
	if !rootFSGenerationEqual(stored, req.Generation) {
		return nil, fmt.Errorf("%w: retried target generation fields changed", ErrRootFSGenerationConflict)
	}
	clone := *filesystem
	return &clone, nil
}

func committedNomadPausedRebaseLifecycleMatchesRequest(
	lifecycle *SandboxLifecycleTxn,
	record *SandboxRecord,
	request *PublishPausedRootFSRebaseRequest,
) bool {
	return lifecycle != nil && record != nil && request != nil && request.Generation != nil &&
		lifecycle.ID == request.OperationID && lifecycle.SandboxID == record.ID &&
		lifecycle.Kind == SandboxLifecycleKindRebase && lifecycle.Phase == SandboxLifecyclePhaseCommitted &&
		lifecycle.Source == SandboxLifecycleSourceManual && !lifecycle.Cancelable &&
		lifecycle.CancelRequestedAt.IsZero() && lifecycle.FromGeneration == lifecycle.ToGeneration &&
		lifecycle.FromGeneration == record.RuntimeGeneration && lifecycle.FromPodNamespace == "" &&
		lifecycle.FromPodName == "" && lifecycle.ToPodNamespace == "" && lifecycle.ToPodName == "" &&
		lifecycle.TargetSandboxID == "" && len(lifecycle.TargetRecordDigest) == 0 &&
		lifecycle.TargetGenerationID == request.Generation.ID &&
		lifecycle.PreparedHeadLayerID == request.Generation.ID &&
		lifecycle.ExpectedHeadLayerID == request.ExpectedSourceGenerationID &&
		lifecycle.SourceBaseArtifactDigest == request.ExpectedBaseArtifactDigest &&
		lifecycle.TargetBaseArtifactDigest == request.Generation.BaseArtifactDigest &&
		lifecycle.RollbackExpiresAt.Equal(request.RollbackExpiresAt)
}

func validateNomadPausedRebaseOutput(generation *RootFSGeneration, artifact *RootFSBaseArtifact) error {
	if generation == nil || artifact == nil {
		return fmt.Errorf("%w: target generation or Base artifact is missing", ErrRootFSGenerationConflict)
	}
	targetBase, baseErr := rootfsblock.DecodeDescriptor(artifact.Descriptor)
	target, targetErr := rootfsblock.DecodeDescriptor(generation.Descriptor)
	if baseErr != nil || targetErr != nil || targetBase.MappingRoot.RootDigest != artifact.BaseBlockRoot ||
		target.MappingRoot.RootDigest != generation.CurrentBlockHead ||
		target.LogicalSizeBytes != targetBase.LogicalSizeBytes ||
		target.BlockSizeBytes != targetBase.BlockSizeBytes {
		return fmt.Errorf("%w: target generation block geometry or descriptor is invalid",
			ErrRootFSGenerationConflict)
	}
	return nil
}

func insertPreparedRootFSGeneration(ctx context.Context, tx pgx.Tx, generation *RootFSGeneration) error {
	if err := ensureRootFSCompositeBacklogCapacity(ctx, tx, generation); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_generations (
			generation_id, filesystem_id, parent_generation_id, source_oci_digest,
			base_artifact_digest, base_block_root, current_block_head, writer_epoch,
			format_generation, durability_state, locator_version, descriptor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
		ON CONFLICT (generation_id) DO NOTHING
	`, generation.ID, generation.FilesystemID, generation.ParentGenerationID,
		generation.SourceOCIDigest, generation.BaseArtifactDigest, generation.BaseBlockRoot,
		generation.CurrentBlockHead, generation.WriterEpoch, generation.FormatGeneration,
		generation.DurabilityState, generation.LocatorVersion, generation.Descriptor); err != nil {
		return fmt.Errorf("insert prepared rootfs generation: %w", err)
	}
	stored, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1
	`, generation.ID))
	if err != nil {
		return fmt.Errorf("load prepared rootfs generation: %w", err)
	}
	if !rootFSGenerationEqual(stored, generation) {
		return fmt.Errorf("%w: prepared target generation has different immutable fields", ErrRootFSGenerationConflict)
	}
	return nil
}

func rootFSGenerationEqual(left, right *RootFSGeneration) bool {
	return left != nil && right != nil && left.ID == right.ID && left.FilesystemID == right.FilesystemID &&
		left.ParentGenerationID == right.ParentGenerationID && left.SourceOCIDigest == right.SourceOCIDigest &&
		left.BaseArtifactDigest == right.BaseArtifactDigest && left.BaseBlockRoot == right.BaseBlockRoot &&
		left.CurrentBlockHead == right.CurrentBlockHead && left.WriterEpoch == right.WriterEpoch &&
		left.FormatGeneration == right.FormatGeneration && left.DurabilityState == right.DurabilityState &&
		left.LocatorVersion == right.LocatorVersion && bytes.Equal(left.Descriptor, right.Descriptor)
}

func initialRootFSGenerationID(filesystemID, artifactDigest string, formatGeneration int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("sandbox0-rootfs-initial-v1\x00%s\x00%s\x00%d",
		filesystemID, artifactDigest, formatGeneration)))
	return "rootfs-generation-" + hex.EncodeToString(sum[:])
}

func rootFSBaseArtifactMatchesRequest(artifact *RootFSBaseArtifact, req *PutReadyRootFSBaseArtifactRequest) bool {
	return artifact != nil && req != nil && artifact.ArtifactDigest == req.ArtifactDigest &&
		artifact.SourceOCIRef == req.SourceOCIRef && artifact.SourceOCIDigest == req.SourceOCIDigest &&
		artifact.BaseBlockRoot == req.BaseBlockRoot && artifact.FormatGeneration == req.FormatGeneration &&
		artifact.Platform == req.Platform &&
		artifact.State == RootFSBaseArtifactStateReady && string(artifact.Descriptor) == string(req.Descriptor)
}

func initialRootFSGenerationMatches(
	filesystem *RootFSFilesystem,
	generation *RootFSGeneration,
	artifact *RootFSBaseArtifact,
	req *EnsureInitialRootFSGenerationRequest,
) bool {
	return filesystem != nil && generation != nil && artifact != nil && req != nil &&
		filesystem.StorageFormat == RootFSStorageFormatBlockCOWV1 &&
		filesystem.TeamID == req.TeamID && filesystem.BaseImageRef == req.SourceOCIRef &&
		filesystem.BaseImageDigest == req.SourceOCIDigest &&
		filesystem.BaseArtifactDigest == artifact.ArtifactDigest &&
		filesystem.FormatGeneration == artifact.FormatGeneration &&
		filesystem.HeadGenerationID == generation.ID && filesystem.WriterEpoch == 0 &&
		generation.FilesystemID == filesystem.ID && generation.ParentGenerationID == "" &&
		generation.SourceOCIDigest == req.SourceOCIDigest &&
		generation.BaseArtifactDigest == artifact.ArtifactDigest &&
		generation.BaseBlockRoot == artifact.BaseBlockRoot && generation.CurrentBlockHead == artifact.BaseBlockRoot &&
		generation.WriterEpoch == 0 && generation.FormatGeneration == artifact.FormatGeneration &&
		generation.DurabilityState == RootFSGenerationStateS3Materialized
}

func rootFSBaseArtifactSelectSQL() string {
	return `
		SELECT artifact_digest, source_oci_ref, source_oci_digest, base_block_root,
			format_generation, COALESCE(oci_os, ''), COALESCE(oci_architecture, ''),
			COALESCE(oci_variant, ''), state, descriptor, created_at, updated_at
		FROM manager.rootfs_base_artifacts `
}

func rootFSGenerationSelectSQL() string {
	return `
		SELECT generation_id, filesystem_id, parent_generation_id, source_oci_digest,
			base_artifact_digest, base_block_root, current_block_head, writer_epoch,
			format_generation, durability_state, locator_version, descriptor, created_at
		FROM manager.rootfs_generations `
}

func scanRootFSBaseArtifact(row sandboxRecordScanner) (*RootFSBaseArtifact, error) {
	var artifact RootFSBaseArtifact
	if err := row.Scan(&artifact.ArtifactDigest, &artifact.SourceOCIRef, &artifact.SourceOCIDigest,
		&artifact.BaseBlockRoot, &artifact.FormatGeneration, &artifact.Platform.OS,
		&artifact.Platform.Architecture, &artifact.Platform.Variant,
		&artifact.State, &artifact.Descriptor,
		&artifact.CreatedAt, &artifact.UpdatedAt); err != nil {
		return nil, err
	}
	artifact.Descriptor = append([]byte(nil), artifact.Descriptor...)
	return &artifact, nil
}

func scanRootFSGeneration(row sandboxRecordScanner) (*RootFSGeneration, error) {
	var generation RootFSGeneration
	var parent *string
	if err := row.Scan(&generation.ID, &generation.FilesystemID, &parent,
		&generation.SourceOCIDigest, &generation.BaseArtifactDigest, &generation.BaseBlockRoot,
		&generation.CurrentBlockHead, &generation.WriterEpoch, &generation.FormatGeneration,
		&generation.DurabilityState, &generation.LocatorVersion, &generation.Descriptor,
		&generation.CreatedAt); err != nil {
		return nil, err
	}
	if parent != nil {
		generation.ParentGenerationID = *parent
	}
	generation.Descriptor = append([]byte(nil), generation.Descriptor...)
	return &generation, nil
}

func scanRootFSFilesystemAndGeneration(row sandboxRecordScanner) (*RootFSFilesystem, *RootFSGeneration, error) {
	var filesystem RootFSFilesystem
	var generation RootFSGeneration
	var sourceFilesystemID, headLayerID, baseArtifactDigest, headGenerationID, parentGenerationID *string
	var formatGeneration *int
	if err := row.Scan(
		&filesystem.ID, &filesystem.TeamID, &sourceFilesystemID, &headLayerID,
		&filesystem.WriterEpoch, &filesystem.BaseImageRef, &filesystem.BaseImageDigest,
		&filesystem.StorageFormat, &baseArtifactDigest, &formatGeneration, &headGenerationID,
		&filesystem.CreatedAt, &filesystem.UpdatedAt,
		&generation.ID, &generation.FilesystemID, &parentGenerationID,
		&generation.SourceOCIDigest, &generation.BaseArtifactDigest, &generation.BaseBlockRoot,
		&generation.CurrentBlockHead, &generation.WriterEpoch, &generation.FormatGeneration,
		&generation.DurabilityState, &generation.LocatorVersion, &generation.Descriptor,
		&generation.CreatedAt,
	); err != nil {
		return nil, nil, err
	}
	if sourceFilesystemID != nil {
		filesystem.SourceFilesystemID = *sourceFilesystemID
	}
	if headLayerID != nil {
		filesystem.HeadLayerID = *headLayerID
	}
	if baseArtifactDigest != nil {
		filesystem.BaseArtifactDigest = *baseArtifactDigest
	}
	if formatGeneration != nil {
		filesystem.FormatGeneration = *formatGeneration
	}
	if headGenerationID != nil {
		filesystem.HeadGenerationID = *headGenerationID
	}
	if parentGenerationID != nil {
		generation.ParentGenerationID = *parentGenerationID
	}
	generation.Descriptor = append([]byte(nil), generation.Descriptor...)
	return &filesystem, &generation, nil
}
