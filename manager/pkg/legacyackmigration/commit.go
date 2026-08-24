package legacyackmigration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// TargetCommitResult is the verified product graph made reachable by one
// atomic migration catalog transaction.
type TargetCommitResult struct {
	CommitDigest string
	Sandboxes    int
	Filesystems  int
	Generations  int
	Snapshots    int
}

type targetCommitPlan struct {
	Version     int                      `json:"version"`
	SessionID   string                   `json:"session_id"`
	ClusterID   string                   `json:"cluster_id"`
	Sandboxes   []targetCommitSandbox    `json:"sandboxes"`
	Filesystems []targetCommitFilesystem `json:"filesystems"`
	Snapshots   []targetCommitSnapshot   `json:"snapshots"`
}

type targetCommitSandbox struct {
	Record       sandboxstore.SandboxRecord `json:"record"`
	FilesystemID string                     `json:"filesystem_id"`
	Config       json.RawMessage            `json:"config"`
	TemplateSpec json.RawMessage            `json:"template_spec"`
}

type targetCommitFilesystem struct {
	ID                 string                   `json:"id"`
	TeamID             string                   `json:"team_id"`
	SourceFilesystemID string                   `json:"source_filesystem_id,omitempty"`
	HeadGenerationID   string                   `json:"head_generation_id"`
	WriterEpoch        int64                    `json:"writer_epoch"`
	BaseArtifactDigest string                   `json:"base_artifact_digest"`
	FormatGeneration   int                      `json:"format_generation"`
	CreatedAt          time.Time                `json:"created_at"`
	UpdatedAt          time.Time                `json:"updated_at"`
	Generations        []targetCommitGeneration `json:"generations"`
}

type targetCommitGeneration struct {
	ID                 string                        `json:"id"`
	BuildID            string                        `json:"build_id,omitempty"`
	FilesystemID       string                        `json:"filesystem_id"`
	ParentGenerationID string                        `json:"parent_generation_id,omitempty"`
	SourceOCIDigest    string                        `json:"source_oci_digest"`
	BaseArtifactDigest string                        `json:"base_artifact_digest"`
	BaseBlockRoot      string                        `json:"base_block_root"`
	CurrentBlockHead   string                        `json:"current_block_head"`
	WriterEpoch        int64                         `json:"writer_epoch"`
	FormatGeneration   int                           `json:"format_generation"`
	LocatorVersion     int64                         `json:"locator_version"`
	Descriptor         []byte                        `json:"descriptor"`
	CreatedAt          time.Time                     `json:"created_at"`
	References         []rootfsblock.ObjectReference `json:"references,omitempty"`
}

type targetCommitSnapshot struct {
	ID               string    `json:"id"`
	FilesystemID     string    `json:"filesystem_id"`
	TeamID           string    `json:"team_id"`
	SourceSandboxID  string    `json:"source_sandbox_id"`
	HeadGenerationID string    `json:"head_generation_id"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	CreatedAt        time.Time `json:"created_at"`
	ExpiresAt        time.Time `json:"expires_at,omitempty"`
}

type targetReadyBaseArtifact struct {
	Digest           string
	SourceOCIRef     string
	SourceOCIDigest  string
	BaseBlockRoot    string
	FormatGeneration int
	OS               string
	Architecture     string
	Variant          string
	ProcdProtocol    string
	ProcdDigest      string
	LogicalSizeBytes int64
	Descriptor       []byte
}

// CommitCatalog atomically installs paused sandboxes and their complete
// block-COW graph. A committed session is verified against the same canonical
// plan so a lost commit response is an exact retry, never a second import.
func (s *TargetStore) CommitCatalog(
	ctx context.Context,
	sessionID string,
	catalog *NormalizedCatalog,
) (*TargetCommitResult, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("target migration store is not configured")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || catalog == nil {
		return nil, fmt.Errorf("target migration session and normalized catalog are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin target catalog commit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state, clusterID string
	var storedCommitDigest *string
	if err := tx.QueryRow(ctx, `
		SELECT state, target_cluster_id, commit_digest
		FROM legacy_ack_migration.sessions
		WHERE session_id = $1
		FOR UPDATE
	`, sessionID).Scan(&state, &clusterID, &storedCommitDigest); err != nil {
		return nil, fmt.Errorf("lock target migration session: %w", err)
	}
	operations, artifacts, err := loadTargetCommitInputs(ctx, tx, sessionID, catalog)
	if err != nil {
		return nil, err
	}
	plan, err := makeTargetCommitPlan(sessionID, clusterID, catalog, operations, artifacts)
	if err != nil {
		return nil, err
	}
	commitDigest, err := plan.digest()
	if err != nil {
		return nil, err
	}
	result := plan.result(commitDigest)

	if state == "committed" {
		if storedCommitDigest == nil || *storedCommitDigest != commitDigest {
			return nil, fmt.Errorf("%w: committed session has another product graph", ErrTargetMigrationConflict)
		}
		if err := verifyTargetCommitPlan(ctx, tx, plan); err != nil {
			return nil, err
		}
		if err := commitTargetTx(ctx, tx, "existing product catalog"); err != nil {
			return nil, err
		}
		return result, nil
	}
	if state != "prepared" && state != "importing" {
		return nil, fmt.Errorf("%w: target session state %q cannot commit", ErrTargetMigrationConflict, state)
	}
	if storedCommitDigest != nil {
		return nil, fmt.Errorf("%w: uncommitted session already has a commit digest", ErrTargetMigrationConflict)
	}
	if err := insertTargetCommitPlan(ctx, tx, plan); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE legacy_ack_migration.sessions
		SET state = 'committed', commit_digest = $2, committed_at = NOW(), updated_at = NOW()
		WHERE session_id = $1 AND state IN ('prepared', 'importing') AND commit_digest IS NULL
	`, sessionID, commitDigest)
	if err != nil {
		return nil, fmt.Errorf("mark target migration session committed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: target migration session commit fence changed", ErrTargetMigrationConflict)
	}
	if err := verifyTargetCommitPlan(ctx, tx, plan); err != nil {
		return nil, err
	}
	if err := commitTargetTx(ctx, tx, "product catalog"); err != nil {
		return nil, err
	}
	return result, nil
}

func loadTargetCommitInputs(
	ctx context.Context,
	tx pgx.Tx,
	sessionID string,
	catalog *NormalizedCatalog,
) (map[string]*TargetBuildOperation, map[string]targetReadyBaseArtifact, error) {
	builds := make(map[string]MaterializedBuild, len(catalog.MaterializedBuilds))
	for _, build := range catalog.MaterializedBuilds {
		if build.ID == "" {
			return nil, nil, fmt.Errorf("normalized catalog has an empty materialized build ID")
		}
		if _, exists := builds[build.ID]; exists {
			return nil, nil, fmt.Errorf("normalized catalog has duplicate materialized build %s", build.ID)
		}
		builds[build.ID] = build
	}
	operations := make(map[string]*TargetBuildOperation, len(builds))
	artifacts := make(map[string]targetReadyBaseArtifact)
	buildIDs := make([]string, 0, len(builds))
	for buildID := range builds {
		buildIDs = append(buildIDs, buildID)
	}
	slices.Sort(buildIDs)
	for _, buildID := range buildIDs {
		operation, err := loadReadyTargetBuildForCommit(ctx, tx, sessionID, builds[buildID])
		if err != nil {
			return nil, nil, err
		}
		operations[buildID] = operation
		if _, exists := artifacts[operation.BaseArtifactDigest]; !exists {
			artifact, err := loadTargetReadyBaseArtifact(ctx, tx, operation.BaseArtifactDigest)
			if err != nil {
				return nil, nil, err
			}
			artifacts[artifact.Digest] = artifact
		}
	}
	return operations, artifacts, nil
}

func loadReadyTargetBuildForCommit(
	ctx context.Context,
	tx pgx.Tx,
	sessionID string,
	expected MaterializedBuild,
) (*TargetBuildOperation, error) {
	operation, err := scanTargetBuild(tx.QueryRow(ctx,
		targetBuildSelectSQL()+" WHERE build_id = $1 FOR UPDATE", expected.ID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: target build %s is missing", ErrTargetMigrationConflict, expected.ID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock target build %s: %w", expected.ID, err)
	}
	if err := validateStoredTargetBuildInput(operation); err != nil {
		return nil, err
	}
	_, _, expectedInputDigest, err := normalizeTargetBuildInput(expected, operation.Contract)
	if err != nil {
		return nil, fmt.Errorf("validate normalized build %s: %w", expected.ID, err)
	}
	if operation.SessionID != sessionID || operation.InputDigest != expectedInputDigest ||
		operation.State != targetBuildStateReady || operation.Result == nil {
		return nil, fmt.Errorf("%w: target build %s is not the expected ready build", ErrTargetMigrationConflict, expected.ID)
	}
	rows, err := tx.Query(ctx, `
		SELECT object.object_key, object.object_kind, object.object_size, object.checksum,
			link.upload_state, object.uploaded_at
		FROM legacy_ack_migration.build_objects link
		JOIN manager.rootfs_materialization_objects object USING (object_key)
		WHERE link.build_id = $1 AND link.result_object
		ORDER BY object.object_key
	`, expected.ID)
	if err != nil {
		return nil, fmt.Errorf("read ready target build objects: %w", err)
	}
	var references []rootfsblock.ObjectReference
	for rows.Next() {
		var reference rootfsblock.ObjectReference
		var uploadState string
		var uploadedAt *time.Time
		if err := rows.Scan(&reference.Key, &reference.Kind, &reference.Size, &reference.Checksum,
			&uploadState, &uploadedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan ready target build object: %w", err)
		}
		if uploadState != "published" || uploadedAt == nil {
			rows.Close()
			return nil, fmt.Errorf("%w: target build object %s is not published", ErrTargetMigrationConflict, reference.Key)
		}
		references = append(references, reference)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate ready target build objects: %w", err)
	}
	rows.Close()
	operation.Result.References = references
	if err := operation.Result.Validate(); err != nil {
		return nil, fmt.Errorf("%w: ready target build %s result is invalid: %v",
			ErrTargetMigrationConflict, expected.ID, err)
	}
	if err := verifyReadyBaseArtifact(ctx, tx, operation, operation.BaseArtifactDigest, *operation.Result); err != nil {
		return nil, err
	}
	if err := verifyReadyTargetBuild(ctx, tx, operation, operation.BaseArtifactDigest, *operation.Result); err != nil {
		return nil, err
	}
	return operation, nil
}

func loadTargetReadyBaseArtifact(
	ctx context.Context,
	tx pgx.Tx,
	artifactDigest string,
) (targetReadyBaseArtifact, error) {
	var artifact targetReadyBaseArtifact
	err := tx.QueryRow(ctx, `
		SELECT artifact_digest, source_oci_ref, source_oci_digest, base_block_root,
			format_generation, oci_os, oci_architecture, oci_variant,
			procd_protocol, procd_digest, logical_size_bytes, descriptor
		FROM manager.rootfs_base_artifacts
		WHERE artifact_digest = $1 AND state = 'ready'
		FOR UPDATE
	`, artifactDigest).Scan(
		&artifact.Digest, &artifact.SourceOCIRef, &artifact.SourceOCIDigest, &artifact.BaseBlockRoot,
		&artifact.FormatGeneration, &artifact.OS, &artifact.Architecture, &artifact.Variant,
		&artifact.ProcdProtocol, &artifact.ProcdDigest, &artifact.LogicalSizeBytes, &artifact.Descriptor,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return targetReadyBaseArtifact{}, fmt.Errorf("%w: ready Base artifact %s is missing",
			ErrTargetMigrationConflict, artifactDigest)
	}
	if err != nil {
		return targetReadyBaseArtifact{}, fmt.Errorf("read target Base artifact: %w", err)
	}
	descriptor, err := rootfsblock.DecodeDescriptor(artifact.Descriptor)
	if err != nil || descriptor.MappingRoot.RootDigest != artifact.BaseBlockRoot ||
		descriptor.LogicalSizeBytes != artifact.LogicalSizeBytes {
		return targetReadyBaseArtifact{}, fmt.Errorf("%w: Base artifact %s descriptor is invalid",
			ErrTargetMigrationConflict, artifactDigest)
	}
	return artifact, nil
}

func makeTargetCommitPlan(
	sessionID, clusterID string,
	catalog *NormalizedCatalog,
	operations map[string]*TargetBuildOperation,
	artifacts map[string]targetReadyBaseArtifact,
) (*targetCommitPlan, error) {
	plan := &targetCommitPlan{Version: 1, SessionID: sessionID, ClusterID: clusterID}
	filesystems := make(map[string]NormalizedFilesystem, len(catalog.Filesystems))
	referencedBuilds := make(map[string]struct{})
	for _, filesystem := range catalog.Filesystems {
		if filesystem.Record.ID == "" || filesystem.Record.TeamID == "" {
			return nil, fmt.Errorf("normalized filesystem identity is incomplete")
		}
		if _, exists := filesystems[filesystem.Record.ID]; exists {
			return nil, fmt.Errorf("normalized catalog has duplicate filesystem %s", filesystem.Record.ID)
		}
		filesystems[filesystem.Record.ID] = filesystem
	}
	for _, filesystem := range catalog.Filesystems {
		if sourceID := filesystem.Record.SourceFilesystemID; sourceID != "" {
			source, ok := filesystems[sourceID]
			if !ok || source.Record.TeamID != filesystem.Record.TeamID {
				return nil, fmt.Errorf("normalized filesystem %s has an invalid source filesystem", filesystem.Record.ID)
			}
		}
		commitFilesystem, buildIDs, err := makeTargetCommitFilesystem(filesystem, operations, artifacts)
		if err != nil {
			return nil, err
		}
		for _, buildID := range buildIDs {
			referencedBuilds[buildID] = struct{}{}
		}
		plan.Filesystems = append(plan.Filesystems, commitFilesystem)
	}
	slices.SortFunc(plan.Filesystems, func(left, right targetCommitFilesystem) int {
		return strings.Compare(left.ID, right.ID)
	})
	if len(referencedBuilds) != len(operations) {
		return nil, fmt.Errorf("normalized catalog contains unreferenced materialized builds")
	}

	sandboxIDs := make(map[string]struct{}, len(catalog.Sandboxes))
	for _, sandbox := range catalog.Sandboxes {
		record := sandbox.Record
		filesystem, ok := filesystems[sandbox.FilesystemID]
		if _, duplicate := sandboxIDs[record.ID]; duplicate || record.ID == "" {
			return nil, fmt.Errorf("normalized catalog has an empty or duplicate sandbox ID")
		}
		sandboxIDs[record.ID] = struct{}{}
		if !ok || filesystem.Record.TeamID != record.TeamID || record.ClusterID != clusterID ||
			record.DesiredState != sandboxstore.SandboxDesiredStatePaused || record.RuntimeID != "" ||
			record.RuntimeNamespace != "" || record.DeletedAt.IsZero() == false ||
			record.ResourceMillicpu <= 0 || record.ResourceMemoryMiB <= 0 {
			return nil, fmt.Errorf("normalized sandbox %s is not a safe paused target record", record.ID)
		}
		config, err := json.Marshal(record.Config)
		if err != nil {
			return nil, fmt.Errorf("marshal normalized sandbox %s config: %w", record.ID, err)
		}
		spec, err := json.Marshal(record.TemplateSpec)
		if err != nil {
			return nil, fmt.Errorf("marshal normalized sandbox %s template: %w", record.ID, err)
		}
		plan.Sandboxes = append(plan.Sandboxes, targetCommitSandbox{
			Record: record, FilesystemID: sandbox.FilesystemID,
			Config: json.RawMessage(config), TemplateSpec: json.RawMessage(spec),
		})
	}
	slices.SortFunc(plan.Sandboxes, func(left, right targetCommitSandbox) int {
		return strings.Compare(left.Record.ID, right.Record.ID)
	})

	snapshotIDs := make(map[string]struct{}, len(catalog.Snapshots))
	generationByBuild := make(map[string]map[string]string, len(plan.Filesystems))
	for _, filesystem := range plan.Filesystems {
		generationByBuild[filesystem.ID] = make(map[string]string)
		for _, generation := range filesystem.Generations {
			if generation.BuildID != "" {
				generationByBuild[filesystem.ID][generation.BuildID] = generation.ID
			}
		}
	}
	for _, snapshot := range catalog.Snapshots {
		record := snapshot.Record
		filesystem, ok := filesystems[record.FilesystemID]
		if _, duplicate := snapshotIDs[record.ID]; duplicate || record.ID == "" {
			return nil, fmt.Errorf("normalized catalog has an empty or duplicate snapshot ID")
		}
		snapshotIDs[record.ID] = struct{}{}
		generationID := generationByBuild[record.FilesystemID][snapshot.BuildID]
		if !ok || filesystem.Record.TeamID != record.TeamID || generationID == "" ||
			filesystem.BuildIDByLayer[record.HeadLayerID] != snapshot.BuildID {
			return nil, fmt.Errorf("normalized snapshot %s has an invalid generation", record.ID)
		}
		plan.Snapshots = append(plan.Snapshots, targetCommitSnapshot{
			ID: record.ID, FilesystemID: record.FilesystemID, TeamID: record.TeamID,
			SourceSandboxID: record.SourceSandboxID, HeadGenerationID: generationID,
			Name: record.Name, Description: record.Description,
			CreatedAt: record.CreatedAt, ExpiresAt: record.ExpiresAt,
		})
	}
	slices.SortFunc(plan.Snapshots, func(left, right targetCommitSnapshot) int {
		return strings.Compare(left.ID, right.ID)
	})
	return plan, nil
}

func makeTargetCommitFilesystem(
	filesystem NormalizedFilesystem,
	operations map[string]*TargetBuildOperation,
	artifacts map[string]targetReadyBaseArtifact,
) (targetCommitFilesystem, []string, error) {
	record := filesystem.Record
	if filesystem.LogicalSizeBytes <= 0 || filesystem.HeadBuildID == "" ||
		filesystem.BuildIDByLayer[record.HeadLayerID] != filesystem.HeadBuildID {
		return targetCommitFilesystem{}, nil, fmt.Errorf("normalized filesystem %s has an invalid head build", record.ID)
	}
	unique := make(map[string]struct{}, len(filesystem.BuildIDByLayer))
	for layerID, buildID := range filesystem.BuildIDByLayer {
		operation := operations[buildID]
		if layerID == "" || operation == nil || operation.Build.TeamID != record.TeamID ||
			operation.Build.LogicalSizeBytes != filesystem.LogicalSizeBytes || operation.Build.HeadLayerID != layerID {
			return targetCommitFilesystem{}, nil, fmt.Errorf("normalized filesystem %s has an invalid build mapping", record.ID)
		}
		unique[buildID] = struct{}{}
	}
	if _, ok := unique[filesystem.HeadBuildID]; !ok {
		return targetCommitFilesystem{}, nil, fmt.Errorf("normalized filesystem %s does not reference its head build", record.ID)
	}
	buildIDs := make([]string, 0, len(unique))
	for buildID := range unique {
		if buildID != filesystem.HeadBuildID {
			buildIDs = append(buildIDs, buildID)
		}
	}
	slices.Sort(buildIDs)
	buildIDs = append(buildIDs, filesystem.HeadBuildID)

	headOperation := operations[filesystem.HeadBuildID]
	headArtifact := artifacts[headOperation.BaseArtifactDigest]
	if headArtifact.Digest == "" {
		return targetCommitFilesystem{}, nil, fmt.Errorf("target filesystem %s has no ready Base artifact", record.ID)
	}
	baseGenerationID := targetGenerationID("legacy-ack-base-v1", record.ID, headArtifact.Digest)
	commitFilesystem := targetCommitFilesystem{
		ID: record.ID, TeamID: record.TeamID, SourceFilesystemID: record.SourceFilesystemID,
		WriterEpoch: int64(len(buildIDs)), BaseArtifactDigest: headArtifact.Digest,
		FormatGeneration: headArtifact.FormatGeneration,
		CreatedAt:        record.CreatedAt, UpdatedAt: record.UpdatedAt,
		Generations: []targetCommitGeneration{{
			ID: baseGenerationID, FilesystemID: record.ID,
			SourceOCIDigest: headArtifact.SourceOCIDigest, BaseArtifactDigest: headArtifact.Digest,
			BaseBlockRoot: headArtifact.BaseBlockRoot, CurrentBlockHead: headArtifact.BaseBlockRoot,
			WriterEpoch: 0, FormatGeneration: headArtifact.FormatGeneration, LocatorVersion: 1,
			Descriptor: append([]byte(nil), headArtifact.Descriptor...), CreatedAt: record.CreatedAt,
		}},
	}
	for index, buildID := range buildIDs {
		operation := operations[buildID]
		artifact := artifacts[operation.BaseArtifactDigest]
		if operation.Result == nil || artifact.Digest != headArtifact.Digest ||
			operation.Build.SourceOCIDigest != headOperation.Build.SourceOCIDigest {
			return targetCommitFilesystem{}, nil, fmt.Errorf("target filesystem %s builds do not share one Base artifact", record.ID)
		}
		generationID := targetGenerationID("legacy-ack-user-v1", record.ID, buildID)
		commitFilesystem.Generations = append(commitFilesystem.Generations, targetCommitGeneration{
			ID: generationID, BuildID: buildID, FilesystemID: record.ID,
			ParentGenerationID: baseGenerationID,
			SourceOCIDigest:    operation.Build.SourceOCIDigest,
			BaseArtifactDigest: artifact.Digest, BaseBlockRoot: artifact.BaseBlockRoot,
			CurrentBlockHead: operation.Result.CurrentBlockHead.String(), WriterEpoch: int64(index + 1),
			FormatGeneration: operation.Contract.FormatGeneration, LocatorVersion: 1,
			Descriptor: append([]byte(nil), operation.Result.DescriptorBytes...),
			CreatedAt:  targetBuildCreatedAt(operation.Build, record.UpdatedAt),
			References: append([]rootfsblock.ObjectReference(nil), operation.Result.References...),
		})
		if buildID == filesystem.HeadBuildID {
			commitFilesystem.HeadGenerationID = generationID
		}
	}
	return commitFilesystem, buildIDs, nil
}

func targetBuildCreatedAt(build MaterializedBuild, fallback time.Time) time.Time {
	if len(build.Layers) != 0 && !build.Layers[len(build.Layers)-1].CreatedAt.IsZero() {
		return build.Layers[len(build.Layers)-1].CreatedAt
	}
	return fallback
}

func targetGenerationID(prefix string, parts ...string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(prefix))
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil))
}

func (p *targetCommitPlan) digest() (string, error) {
	payload, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode target migration commit plan: %w", err)
	}
	return digest.FromBytes(payload).String(), nil
}

func (p *targetCommitPlan) result(commitDigest string) *TargetCommitResult {
	result := &TargetCommitResult{
		CommitDigest: commitDigest, Sandboxes: len(p.Sandboxes),
		Filesystems: len(p.Filesystems), Snapshots: len(p.Snapshots),
	}
	for _, filesystem := range p.Filesystems {
		result.Generations += len(filesystem.Generations)
	}
	return result
}

func insertTargetCommitPlan(ctx context.Context, tx pgx.Tx, plan *targetCommitPlan) error {
	for _, sandbox := range plan.Sandboxes {
		record := sandbox.Record
		_, err := tx.Exec(ctx, `
			INSERT INTO manager.sandboxes (
				sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
				cluster_id, desired_state, config, template_spec,
				runtime_id, runtime_namespace, runtime_generation, lifecycle_epoch,
				owner_kind, resource_millicpu, resource_memory_mib, hot_claim_completed_at,
				claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, 'paused', $8, $9,
				'', '', $10, $11, $12, $13, $14, $15,
				$16, $17, $18, NULL, $19, $20
			)
		`, record.ID, record.TeamID, record.UserID, record.TemplateID, record.TemplateName,
			record.TemplateNamespace, record.ClusterID, sandbox.Config, sandbox.TemplateSpec,
			record.RuntimeGeneration, record.LifecycleEpoch, record.OwnerKind,
			record.ResourceMillicpu, record.ResourceMemoryMiB, nullableTargetTime(record.HotClaimCompletedAt),
			nullableTargetTime(record.ClaimedAt), nullableTargetTime(record.ExpiresAt),
			nullableTargetTime(record.HardExpiresAt), record.CreatedAt, record.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert migrated sandbox %s: %w", record.ID, err)
		}
	}
	for _, filesystem := range plan.Filesystems {
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, writer_epoch, base_artifact_digest,
				format_generation, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, filesystem.ID, filesystem.TeamID, filesystem.WriterEpoch,
			filesystem.BaseArtifactDigest, filesystem.FormatGeneration,
			filesystem.CreatedAt, filesystem.UpdatedAt); err != nil {
			return fmt.Errorf("insert migrated filesystem %s: %w", filesystem.ID, err)
		}
	}
	for _, filesystem := range plan.Filesystems {
		for _, generation := range filesystem.Generations {
			if _, err := tx.Exec(ctx, `
				INSERT INTO manager.rootfs_generations (
					generation_id, filesystem_id, parent_generation_id, source_oci_digest,
					base_artifact_digest, base_block_root, current_block_head, writer_epoch,
					format_generation, durability_state, locator_version, descriptor, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 's3_materialized', $10, $11, $12)
			`, generation.ID, generation.FilesystemID, nullableTargetString(generation.ParentGenerationID),
				generation.SourceOCIDigest, generation.BaseArtifactDigest, generation.BaseBlockRoot,
				generation.CurrentBlockHead, generation.WriterEpoch, generation.FormatGeneration,
				generation.LocatorVersion, generation.Descriptor, generation.CreatedAt); err != nil {
				return fmt.Errorf("insert migrated generation %s: %w", generation.ID, err)
			}
			for _, reference := range generation.References {
				if _, err := tx.Exec(ctx, `
					INSERT INTO manager.rootfs_generation_materialization_objects (
						generation_id, locator_version, object_key, created_at
					) VALUES ($1, $2, $3, $4)
				`, generation.ID, generation.LocatorVersion, reference.Key, generation.CreatedAt); err != nil {
					return fmt.Errorf("link migrated generation object %s: %w", reference.Key, err)
				}
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_filesystems
			SET head_generation_id = $2, updated_at = $3
			WHERE filesystem_id = $1 AND head_generation_id IS NULL
		`, filesystem.ID, filesystem.HeadGenerationID, filesystem.UpdatedAt); err != nil {
			return fmt.Errorf("publish migrated filesystem head %s: %w", filesystem.ID, err)
		}
	}
	for _, filesystem := range plan.Filesystems {
		if filesystem.SourceFilesystemID == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_filesystems
			SET source_filesystem_id = $2, updated_at = $3
			WHERE filesystem_id = $1 AND source_filesystem_id IS NULL
		`, filesystem.ID, filesystem.SourceFilesystemID, filesystem.UpdatedAt); err != nil {
			return fmt.Errorf("link migrated source filesystem %s: %w", filesystem.ID, err)
		}
	}
	for _, sandbox := range plan.Sandboxes {
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.sandbox_rootfs_bindings (
				sandbox_id, filesystem_id, team_id, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5)
		`, sandbox.Record.ID, sandbox.FilesystemID, sandbox.Record.TeamID,
			sandbox.Record.CreatedAt, sandbox.Record.UpdatedAt); err != nil {
			return fmt.Errorf("insert migrated sandbox binding %s: %w", sandbox.Record.ID, err)
		}
	}
	for _, snapshot := range plan.Snapshots {
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.rootfs_snapshots (
				snapshot_id, team_id, source_sandbox_id, name, description,
				created_at, expires_at, filesystem_id, head_generation_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, snapshot.ID, snapshot.TeamID, snapshot.SourceSandboxID, snapshot.Name,
			snapshot.Description, snapshot.CreatedAt, nullableTargetTime(snapshot.ExpiresAt),
			snapshot.FilesystemID, snapshot.HeadGenerationID); err != nil {
			return fmt.Errorf("insert migrated snapshot %s: %w", snapshot.ID, err)
		}
	}
	return nil
}

func verifyTargetCommitPlan(ctx context.Context, tx pgx.Tx, plan *targetCommitPlan) error {
	for _, sandbox := range plan.Sandboxes {
		record := sandbox.Record
		if err := requireTargetRow(ctx, tx, `
			SELECT EXISTS (
				SELECT 1 FROM manager.sandboxes
				WHERE sandbox_id = $1 AND team_id = $2 AND user_id = $3
					AND template_id = $4 AND template_name = $5 AND template_namespace = $6
					AND cluster_id = $7 AND desired_state = 'paused'
					AND config = $8::jsonb AND template_spec = $9::jsonb
					AND runtime_id = '' AND runtime_namespace = ''
					AND runtime_generation = $10 AND lifecycle_epoch = $11 AND owner_kind = $12
					AND resource_millicpu = $13 AND resource_memory_mib = $14
					AND hot_claim_completed_at IS NOT DISTINCT FROM $15::timestamptz
					AND claimed_at IS NOT DISTINCT FROM $16::timestamptz
					AND expires_at IS NOT DISTINCT FROM $17::timestamptz
					AND hard_expires_at IS NOT DISTINCT FROM $18::timestamptz
					AND deleted_at IS NULL AND created_at = $19 AND updated_at = $20
			)
		`, "sandbox "+record.ID, record.ID, record.TeamID, record.UserID, record.TemplateID,
			record.TemplateName, record.TemplateNamespace, record.ClusterID, sandbox.Config, sandbox.TemplateSpec,
			record.RuntimeGeneration, record.LifecycleEpoch, record.OwnerKind,
			record.ResourceMillicpu, record.ResourceMemoryMiB, nullableTargetTime(record.HotClaimCompletedAt),
			nullableTargetTime(record.ClaimedAt), nullableTargetTime(record.ExpiresAt),
			nullableTargetTime(record.HardExpiresAt), record.CreatedAt, record.UpdatedAt); err != nil {
			return err
		}
		if err := requireTargetRow(ctx, tx, `
			SELECT EXISTS (
				SELECT 1 FROM manager.sandbox_rootfs_bindings
				WHERE sandbox_id = $1 AND filesystem_id = $2 AND team_id = $3
					AND created_at = $4 AND updated_at = $5
			)
		`, "sandbox binding "+record.ID, record.ID, sandbox.FilesystemID, record.TeamID,
			record.CreatedAt, record.UpdatedAt); err != nil {
			return err
		}
	}
	for _, filesystem := range plan.Filesystems {
		if err := requireTargetRow(ctx, tx, `
			SELECT EXISTS (
				SELECT 1 FROM manager.rootfs_filesystems
				WHERE filesystem_id = $1 AND team_id = $2
					AND source_filesystem_id IS NOT DISTINCT FROM $3::text
					AND head_generation_id = $4 AND writer_epoch = $5
					AND base_artifact_digest = $6 AND format_generation = $7
					AND created_at = $8
			)
		`, "filesystem "+filesystem.ID, filesystem.ID, filesystem.TeamID,
			nullableTargetString(filesystem.SourceFilesystemID), filesystem.HeadGenerationID,
			filesystem.WriterEpoch, filesystem.BaseArtifactDigest, filesystem.FormatGeneration,
			filesystem.CreatedAt); err != nil {
			return err
		}
		for _, generation := range filesystem.Generations {
			if err := requireTargetRow(ctx, tx, `
				SELECT EXISTS (
					SELECT 1 FROM manager.rootfs_generations
					WHERE generation_id = $1 AND filesystem_id = $2
						AND parent_generation_id IS NOT DISTINCT FROM $3::text
						AND source_oci_digest = $4 AND base_artifact_digest = $5
						AND base_block_root = $6 AND current_block_head = $7
						AND writer_epoch = $8 AND format_generation = $9
						AND durability_state = 's3_materialized' AND locator_version = $10
						AND descriptor = $11 AND created_at = $12
				)
			`, "generation "+generation.ID, generation.ID, generation.FilesystemID,
				nullableTargetString(generation.ParentGenerationID), generation.SourceOCIDigest,
				generation.BaseArtifactDigest, generation.BaseBlockRoot, generation.CurrentBlockHead,
				generation.WriterEpoch, generation.FormatGeneration, generation.LocatorVersion,
				generation.Descriptor, generation.CreatedAt); err != nil {
				return err
			}
			var objectCount int
			if err := tx.QueryRow(ctx, `
				SELECT COUNT(*) FROM manager.rootfs_generation_materialization_objects
				WHERE generation_id = $1 AND locator_version = $2
			`, generation.ID, generation.LocatorVersion).Scan(&objectCount); err != nil {
				return fmt.Errorf("count migrated generation objects: %w", err)
			}
			if objectCount != len(generation.References) {
				return fmt.Errorf("%w: generation %s has %d object links, expected %d",
					ErrTargetMigrationConflict, generation.ID, objectCount, len(generation.References))
			}
			for _, reference := range generation.References {
				if err := requireTargetRow(ctx, tx, `
					SELECT EXISTS (
						SELECT 1 FROM manager.rootfs_generation_materialization_objects
						WHERE generation_id = $1 AND locator_version = $2 AND object_key = $3
					)
				`, "generation object "+reference.Key, generation.ID,
					generation.LocatorVersion, reference.Key); err != nil {
					return err
				}
			}
		}
	}
	for _, snapshot := range plan.Snapshots {
		if err := requireTargetRow(ctx, tx, `
			SELECT EXISTS (
				SELECT 1 FROM manager.rootfs_snapshots
				WHERE snapshot_id = $1 AND team_id = $2 AND source_sandbox_id = $3
					AND name = $4 AND description = $5 AND created_at = $6
					AND expires_at IS NOT DISTINCT FROM $7::timestamptz
					AND filesystem_id = $8 AND head_generation_id = $9
			)
		`, "snapshot "+snapshot.ID, snapshot.ID, snapshot.TeamID, snapshot.SourceSandboxID,
			snapshot.Name, snapshot.Description, snapshot.CreatedAt,
			nullableTargetTime(snapshot.ExpiresAt), snapshot.FilesystemID, snapshot.HeadGenerationID); err != nil {
			return err
		}
	}
	return nil
}

func requireTargetRow(ctx context.Context, tx pgx.Tx, query, identity string, args ...any) error {
	var exists bool
	if err := tx.QueryRow(ctx, query, args...).Scan(&exists); err != nil {
		return fmt.Errorf("verify migrated %s: %w", identity, err)
	}
	if !exists {
		return fmt.Errorf("%w: migrated %s differs from the commit plan", ErrTargetMigrationConflict, identity)
	}
	return nil
}

func nullableTargetTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func nullableTargetString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
