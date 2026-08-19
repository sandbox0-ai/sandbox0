package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

	RootFSGenerationStateS3Materialized = "s3_materialized"
	RootFSGenerationDescriptorMaxBytes  = 64 << 10
)

var (
	ErrRootFSBaseArtifactNotFound = errors.New("rootfs base artifact not found")
	ErrRootFSBaseArtifactConflict = errors.New("rootfs base artifact conflict")
	ErrRootFSGenerationConflict   = errors.New("rootfs generation conflict")
)

// RootFSBaseArtifact is an immutable, trusted OCI-to-block conversion shared
// by filesystems in one region. The descriptor is bounded control metadata;
// block data and mapping pages remain in the region object store.
type RootFSBaseArtifact struct {
	ArtifactDigest   string
	SourceOCIRef     string
	SourceOCIDigest  string
	BaseBlockRoot    string
	FormatGeneration int
	State            string
	Descriptor       []byte
	CreatedAt        time.Time
	UpdatedAt        time.Time
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
	GetReadyRootFSBaseArtifact(context.Context, string, int) (*RootFSBaseArtifact, error)
	EnsureInitialRootFSGeneration(context.Context, *EnsureInitialRootFSGenerationRequest) (*RootFSFilesystem, *RootFSGeneration, error)
	GetRootFSGeneration(context.Context, string) (*RootFSGeneration, error)
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
			format_generation, state, descriptor, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		ON CONFLICT (artifact_digest) DO NOTHING
	`, normalized.ArtifactDigest, normalized.SourceOCIRef, normalized.SourceOCIDigest,
		normalized.BaseBlockRoot, normalized.FormatGeneration, RootFSBaseArtifactStateReady,
		normalized.Descriptor)
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
	formatGeneration int,
) (*RootFSBaseArtifact, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	sourceOCIDigest = strings.TrimSpace(sourceOCIDigest)
	if _, err := digest.Parse(sourceOCIDigest); err != nil {
		return nil, fmt.Errorf("source_oci_digest: %w", err)
	}
	artifact, err := scanRootFSBaseArtifact(s.pool.QueryRow(ctx, rootFSBaseArtifactSelectSQL()+`
		WHERE source_oci_digest = $1
			AND state = $2
			AND ($3 = 0 OR format_generation = $3)
		ORDER BY format_generation DESC, created_at DESC
		LIMIT 1
	`, sourceOCIDigest, RootFSBaseArtifactStateReady, formatGeneration))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: source %s", ErrRootFSBaseArtifactNotFound, sourceOCIDigest)
	}
	if err != nil {
		return nil, fmt.Errorf("get ready rootfs base artifact: %w", err)
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

func initialRootFSGenerationID(filesystemID, artifactDigest string, formatGeneration int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("sandbox0-rootfs-initial-v1\x00%s\x00%s\x00%d",
		filesystemID, artifactDigest, formatGeneration)))
	return "rootfs-generation-" + hex.EncodeToString(sum[:])
}

func rootFSBaseArtifactMatchesRequest(artifact *RootFSBaseArtifact, req *PutReadyRootFSBaseArtifactRequest) bool {
	return artifact != nil && req != nil && artifact.ArtifactDigest == req.ArtifactDigest &&
		artifact.SourceOCIRef == req.SourceOCIRef && artifact.SourceOCIDigest == req.SourceOCIDigest &&
		artifact.BaseBlockRoot == req.BaseBlockRoot && artifact.FormatGeneration == req.FormatGeneration &&
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
			format_generation, state, descriptor, created_at, updated_at
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
		&artifact.BaseBlockRoot, &artifact.FormatGeneration, &artifact.State, &artifact.Descriptor,
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
