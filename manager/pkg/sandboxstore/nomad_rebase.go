package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	digest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const MaxNomadPausedRebaseRollbackRetention = 7 * 24 * time.Hour

var (
	ErrNomadSandboxRebaseConflict = errors.New("nomad sandbox rebase conflict")
	ErrNomadSandboxRebaseNotReady = errors.New("nomad sandbox rebase is not ready")
)

// NomadPausedRebaseRequest identifies one immutable paused-head migration to
// an already-attested Base artifact. The rollback deadline is persisted before
// a worker may create any output objects.
type NomadPausedRebaseRequest struct {
	OperationID              string
	SandboxID                string
	ExpectedTeamID           string
	TargetBaseArtifactDigest string
	RollbackExpiresAt        time.Time
}

// NomadPausedRebaseCandidate is the complete immutable input to a privileged
// file-aware rebase worker. TargetGenerationID and TargetWriterEpoch are
// reserved by PostgreSQL but do not grant write authority to a sandbox.
type NomadPausedRebaseCandidate struct {
	Completed          bool
	LifecyclePhase     string
	Sandbox            *SandboxRecord
	Filesystem         *RootFSFilesystem
	SourceGeneration   *RootFSGeneration
	SourceBaseArtifact *RootFSBaseArtifact
	TargetBaseArtifact *RootFSBaseArtifact
	TargetGenerationID string
	TargetWriterEpoch  int64
	RollbackExpiresAt  time.Time
}

// NomadPausedRebaseGenerationID derives the immutable output identity from
// all authorities that fence one rebase attempt.
func NomadPausedRebaseGenerationID(
	operationID, sandboxID, sourceGenerationID, targetBaseArtifactDigest string,
) string {
	payload := fmt.Sprintf("sandbox0-nomad-paused-rebase-v1\x00%s\x00%s\x00%s\x00%s",
		operationID, sandboxID, sourceGenerationID, targetBaseArtifactDigest)
	sum := sha256.Sum256([]byte(payload))
	return "rootfs-generation-" + hex.EncodeToString(sum[:])
}

// RequestNomadPausedRebase persists the exact source head, both Base
// artifacts, output generation identity, writer epoch, and rollback deadline
// before any node-side work may start. Exact retries return the same candidate.
func (s *PGSandboxStore) RequestNomadPausedRebase(
	ctx context.Context,
	request *NomadPausedRebaseRequest,
) (*NomadPausedRebaseCandidate, error) {
	normalized, err := normalizeNomadPausedRebaseRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad paused-rebase tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	record, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SandboxID)
	if err != nil {
		return nil, err
	}
	if record.TeamID != normalized.ExpectedTeamID {
		return nil, fmt.Errorf("%w: sandbox team identity changed", ErrNomadSandboxRebaseConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 FOR UPDATE
	`, normalized.OperationID))
	if err != nil {
		return nil, fmt.Errorf("lock Nomad paused-rebase lifecycle: %w", err)
	}
	if lifecycle != nil && lifecycle.Phase == SandboxLifecyclePhaseCommitted {
		candidate, retryErr := loadCompletedNomadPausedRebase(ctx, tx, record, lifecycle, normalized)
		if retryErr != nil {
			return nil, retryErr
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit completed Nomad paused-rebase retry: %w", err)
		}
		return candidate, nil
	}
	if err := validateNomadPausedRebaseSandbox(record); err != nil {
		return nil, err
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if claim.OperationID == "" || claim.Phase != SandboxRuntimeClaimPhaseReady ||
		!claim.LeaseExpiresAt.IsZero() || !claim.CleanupStartedAt.IsZero() || !claim.CleanedAt.IsZero() {
		return nil, fmt.Errorf("%w: sandbox runtime claim is not a canonical ready claim", ErrNomadSandboxRebaseNotReady)
	}
	filesystem, source, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, record.ID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNomadSandboxRebaseNotReady, err)
	}
	if err := validateNomadPausedRebaseSource(filesystem, source, record.TeamID); err != nil {
		return nil, err
	}
	sourceArtifact, targetArtifact, err := lockNomadPausedRebaseArtifacts(
		ctx, tx, source.BaseArtifactDigest, normalized.TargetBaseArtifactDigest,
	)
	if err != nil {
		return nil, err
	}
	if err := validateNomadPausedRebaseArtifacts(source, sourceArtifact, targetArtifact); err != nil {
		return nil, err
	}
	if err := ensureNomadPausedRebasePhysicalStateTerminal(ctx, tx, record.ID, filesystem.ID); err != nil {
		return nil, err
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, fmt.Errorf("read Nomad paused-rebase authority time: %w", err)
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: sandbox hard TTL has expired", ErrNomadSandboxRebaseNotReady)
	}
	if !normalized.RollbackExpiresAt.After(authorityNow) ||
		normalized.RollbackExpiresAt.After(authorityNow.Add(MaxNomadPausedRebaseRollbackRetention)) {
		return nil, fmt.Errorf("%w: rollback deadline must be within the next %s",
			ErrNomadSandboxRebaseConflict, MaxNomadPausedRebaseRollbackRetention)
	}
	if filesystem.WriterEpoch == math.MaxInt64 {
		return nil, fmt.Errorf("%w: RootFS writer epoch is exhausted", ErrNomadSandboxRebaseConflict)
	}
	targetGenerationID := NomadPausedRebaseGenerationID(
		normalized.OperationID, record.ID, source.ID, targetArtifact.ArtifactDigest,
	)
	if lifecycle != nil {
		if !nomadPausedRebaseLifecycleMatches(
			lifecycle, record, source, sourceArtifact, targetArtifact,
			targetGenerationID, normalized.RollbackExpiresAt, false,
		) {
			return nil, fmt.Errorf("%w: lifecycle %s does not match the exact rebase request",
				ErrNomadSandboxRebaseConflict, lifecycle.ID)
		}
		if err := ensureNomadPausedRebaseTargetGenerationAbsent(ctx, tx, targetGenerationID); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit active Nomad paused-rebase retry: %w", err)
		}
		return nomadPausedRebaseCandidate(
			record, filesystem, source, sourceArtifact, targetArtifact, lifecycle, false,
		), nil
	}
	active, err := getActiveLifecycleTxn(ctx, tx, record.ID)
	if err != nil {
		return nil, fmt.Errorf("load active Nomad paused-rebase lifecycle: %w", err)
	}
	if active != nil {
		return nil, fmt.Errorf("%w: lifecycle %s owns the paused sandbox",
			ErrNomadSandboxRebaseConflict, active.ID)
	}
	if err := ensureNomadPausedRebaseTargetGenerationAbsent(ctx, tx, targetGenerationID); err != nil {
		return nil, err
	}
	lifecycle = &SandboxLifecycleTxn{
		ID: normalized.OperationID, SandboxID: record.ID, Kind: SandboxLifecycleKindRebase,
		Phase: SandboxLifecyclePhasePreparing, Source: SandboxLifecycleSourceManual, Cancelable: false,
		FromGeneration: record.RuntimeGeneration, ToGeneration: record.RuntimeGeneration,
		TargetGenerationID: targetGenerationID, ExpectedHeadLayerID: source.ID,
		SourceBaseArtifactDigest: sourceArtifact.ArtifactDigest,
		TargetBaseArtifactDigest: targetArtifact.ArtifactDigest,
		RollbackExpiresAt:        normalized.RollbackExpiresAt,
	}
	if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, lifecycle); err != nil {
		return nil, fmt.Errorf("begin Nomad paused-rebase lifecycle: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad paused-rebase request: %w", err)
	}
	return nomadPausedRebaseCandidate(
		record, filesystem, source, sourceArtifact, targetArtifact, lifecycle, false,
	), nil
}

func normalizeNomadPausedRebaseRequest(request *NomadPausedRebaseRequest) (*NomadPausedRebaseRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("Nomad paused-rebase request is required")
	}
	normalized := *request
	normalized.OperationID = strings.TrimSpace(request.OperationID)
	normalized.SandboxID = strings.TrimSpace(request.SandboxID)
	normalized.ExpectedTeamID = strings.TrimSpace(request.ExpectedTeamID)
	normalized.TargetBaseArtifactDigest = strings.TrimSpace(request.TargetBaseArtifactDigest)
	for name, value := range map[string]string{
		"operation_id": normalized.OperationID, "sandbox_id": normalized.SandboxID,
		"expected_team_id": normalized.ExpectedTeamID,
	} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
			return nil, fmt.Errorf("%s is required, canonical, and at most 512 bytes", name)
		}
	}
	parsed, err := digest.Parse(normalized.TargetBaseArtifactDigest)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != normalized.TargetBaseArtifactDigest {
		return nil, fmt.Errorf("target_base_artifact_digest must be a canonical sha256 digest")
	}
	if request.RollbackExpiresAt.IsZero() {
		return nil, fmt.Errorf("rollback_expires_at is required")
	}
	normalized.RollbackExpiresAt = request.RollbackExpiresAt.UTC().Truncate(time.Microsecond)
	return &normalized, nil
}

func validateNomadPausedRebaseSandbox(record *SandboxRecord) error {
	if record == nil || record.RuntimeBackend != SandboxRuntimeBackendNomad ||
		record.DesiredState != SandboxDesiredStatePaused || !record.DeletedAt.IsZero() ||
		record.RuntimeGeneration < 0 || record.CurrentPodNamespace != "" || record.CurrentPodName != "" {
		return fmt.Errorf("%w: sandbox is not a canonical paused Nomad runtime", ErrNomadSandboxRebaseNotReady)
	}
	return nil
}

func validateNomadPausedRebaseSource(
	filesystem *RootFSFilesystem,
	source *RootFSGeneration,
	teamID string,
) error {
	if filesystem == nil || source == nil || filesystem.StorageFormat != RootFSStorageFormatBlockCOWV1 ||
		filesystem.TeamID != teamID || filesystem.HeadGenerationID != source.ID ||
		filesystem.BaseArtifactDigest == "" || filesystem.BaseArtifactDigest != source.BaseArtifactDigest ||
		filesystem.FormatGeneration != source.FormatGeneration || filesystem.WriterEpoch != source.WriterEpoch ||
		(source.DurabilityState != RootFSGenerationStateCompositeDurable &&
			source.DurabilityState != RootFSGenerationStateS3Materialized) {
		return fmt.Errorf("%w: paused sandbox has no exact durable block-COW head", ErrNomadSandboxRebaseNotReady)
	}
	descriptor, err := rootfsblock.DecodeDescriptor(source.Descriptor)
	if err != nil || descriptor.MappingRoot.RootDigest != source.CurrentBlockHead {
		return fmt.Errorf("%w: paused source generation descriptor is invalid", ErrNomadSandboxRebaseNotReady)
	}
	return nil
}

func lockNomadPausedRebaseArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	sourceDigest, targetDigest string,
) (*RootFSBaseArtifact, *RootFSBaseArtifact, error) {
	if sourceDigest == targetDigest {
		return nil, nil, fmt.Errorf("%w: target Base artifact is already installed", ErrNomadSandboxRebaseConflict)
	}
	rows, err := tx.Query(ctx, rootFSBaseArtifactSelectSQL()+`
		WHERE artifact_digest IN ($1, $2) AND state = $3
		ORDER BY artifact_digest
		FOR SHARE
	`, sourceDigest, targetDigest, RootFSBaseArtifactStateReady)
	if err != nil {
		return nil, nil, fmt.Errorf("lock Nomad paused-rebase Base artifacts: %w", err)
	}
	defer rows.Close()
	var sourceArtifact, targetArtifact *RootFSBaseArtifact
	for rows.Next() {
		artifact, scanErr := scanRootFSBaseArtifact(rows)
		if scanErr != nil {
			return nil, nil, fmt.Errorf("scan Nomad paused-rebase Base artifact: %w", scanErr)
		}
		switch artifact.ArtifactDigest {
		case sourceDigest:
			sourceArtifact = artifact
		case targetDigest:
			targetArtifact = artifact
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate Nomad paused-rebase Base artifacts: %w", err)
	}
	if sourceArtifact == nil {
		return nil, nil, fmt.Errorf("%w: source artifact %s", ErrRootFSBaseArtifactNotFound, sourceDigest)
	}
	if targetArtifact == nil {
		return nil, nil, fmt.Errorf("%w: target artifact %s", ErrRootFSBaseArtifactNotFound, targetDigest)
	}
	return sourceArtifact, targetArtifact, nil
}

func validateNomadPausedRebaseArtifacts(
	source *RootFSGeneration,
	sourceArtifact, targetArtifact *RootFSBaseArtifact,
) error {
	if source == nil || sourceArtifact == nil || targetArtifact == nil ||
		source.BaseArtifactDigest != sourceArtifact.ArtifactDigest ||
		source.SourceOCIDigest != sourceArtifact.SourceOCIDigest ||
		source.BaseBlockRoot != sourceArtifact.BaseBlockRoot ||
		source.FormatGeneration != sourceArtifact.FormatGeneration ||
		sourceArtifact.Platform != targetArtifact.Platform ||
		sourceArtifact.FormatGeneration != targetArtifact.FormatGeneration {
		return fmt.Errorf("%w: source and target Base artifact identities are incompatible", ErrNomadSandboxRebaseConflict)
	}
	sourceBase, sourceErr := rootfsblock.DecodeDescriptor(sourceArtifact.Descriptor)
	targetBase, targetErr := rootfsblock.DecodeDescriptor(targetArtifact.Descriptor)
	sourceGeneration, generationErr := rootfsblock.DecodeDescriptor(source.Descriptor)
	if sourceErr != nil || targetErr != nil || generationErr != nil ||
		sourceBase.MappingRoot.RootDigest != sourceArtifact.BaseBlockRoot ||
		targetBase.MappingRoot.RootDigest != targetArtifact.BaseBlockRoot ||
		sourceGeneration.MappingRoot.RootDigest != source.CurrentBlockHead ||
		sourceBase.LogicalSizeBytes != targetBase.LogicalSizeBytes ||
		sourceBase.BlockSizeBytes != targetBase.BlockSizeBytes ||
		sourceGeneration.LogicalSizeBytes != sourceBase.LogicalSizeBytes ||
		sourceGeneration.BlockSizeBytes != sourceBase.BlockSizeBytes {
		return fmt.Errorf("%w: source and target block geometry or descriptors are incompatible",
			ErrNomadSandboxRebaseConflict)
	}
	return nil
}

func ensureNomadPausedRebasePhysicalStateTerminal(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID, filesystemID string,
) error {
	var nonterminalSlots, activeWriters int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.runtime_slots
		WHERE sandbox_id = $1 AND state <> $2
	`, sandboxID, RuntimeSlotStateTerminal).Scan(&nonterminalSlots); err != nil {
		return fmt.Errorf("count Nomad paused-rebase runtime slots: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_writer_grants
		WHERE filesystem_id = $1 AND state IN ($2, $3, $4)
	`, filesystemID, RootFSWriterGrantStateIssued, RootFSWriterGrantStateConsumed,
		RootFSWriterGrantStateRetiring).Scan(&activeWriters); err != nil {
		return fmt.Errorf("count Nomad paused-rebase RootFS writers: %w", err)
	}
	if nonterminalSlots != 0 || activeWriters != 0 {
		return fmt.Errorf("%w: runtime slot or RootFS writer is not terminal", ErrNomadSandboxRebaseNotReady)
	}
	return nil
}

func ensureNomadPausedRebaseTargetGenerationAbsent(ctx context.Context, tx pgx.Tx, generationID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_generations WHERE generation_id = $1)
	`, generationID).Scan(&exists); err != nil {
		return fmt.Errorf("check Nomad paused-rebase target generation: %w", err)
	}
	if exists {
		return fmt.Errorf("%w: unpublished target generation already exists", ErrNomadSandboxRebaseConflict)
	}
	return nil
}

func nomadPausedRebaseLifecycleMatches(
	lifecycle *SandboxLifecycleTxn,
	record *SandboxRecord,
	source *RootFSGeneration,
	sourceArtifact, targetArtifact *RootFSBaseArtifact,
	targetGenerationID string,
	rollbackExpiresAt time.Time,
	committed bool,
) bool {
	if lifecycle == nil || record == nil || source == nil || sourceArtifact == nil || targetArtifact == nil ||
		lifecycle.SandboxID != record.ID || lifecycle.Kind != SandboxLifecycleKindRebase ||
		lifecycle.Source != SandboxLifecycleSourceManual || lifecycle.Cancelable ||
		!lifecycle.CancelRequestedAt.IsZero() || lifecycle.FromGeneration != lifecycle.ToGeneration ||
		lifecycle.FromGeneration != record.RuntimeGeneration || lifecycle.FromPodNamespace != "" ||
		lifecycle.FromPodName != "" || lifecycle.ToPodNamespace != "" || lifecycle.ToPodName != "" ||
		lifecycle.TargetSandboxID != "" || len(lifecycle.TargetRecordDigest) != 0 ||
		lifecycle.TargetGenerationID != targetGenerationID || lifecycle.ExpectedHeadLayerID != source.ID ||
		lifecycle.SourceBaseArtifactDigest != sourceArtifact.ArtifactDigest ||
		lifecycle.TargetBaseArtifactDigest != targetArtifact.ArtifactDigest ||
		!lifecycle.RollbackExpiresAt.Equal(rollbackExpiresAt) {
		return false
	}
	if committed {
		return lifecycle.Phase == SandboxLifecyclePhaseCommitted &&
			lifecycle.PreparedHeadLayerID == lifecycle.TargetGenerationID
	}
	return lifecycle.PreparedHeadLayerID == "" &&
		(lifecycle.Phase == SandboxLifecyclePhasePreparing ||
			lifecycle.Phase == SandboxLifecyclePhaseBarriered ||
			lifecycle.Phase == SandboxLifecyclePhasePublishing ||
			lifecycle.Phase == SandboxLifecyclePhaseCommitting)
}

func loadCompletedNomadPausedRebase(
	ctx context.Context,
	tx pgx.Tx,
	record *SandboxRecord,
	lifecycle *SandboxLifecycleTxn,
	request *NomadPausedRebaseRequest,
) (*NomadPausedRebaseCandidate, error) {
	if lifecycle.SandboxID != request.SandboxID || lifecycle.Kind != SandboxLifecycleKindRebase ||
		lifecycle.TargetBaseArtifactDigest != request.TargetBaseArtifactDigest ||
		!lifecycle.RollbackExpiresAt.Equal(request.RollbackExpiresAt) {
		return nil, fmt.Errorf("%w: committed lifecycle does not match the rebase request",
			ErrNomadSandboxRebaseConflict)
	}
	filesystem, _, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	source, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1 FOR SHARE
	`, lifecycle.ExpectedHeadLayerID))
	if err != nil {
		return nil, fmt.Errorf("load completed Nomad paused-rebase source generation: %w", err)
	}
	target, err := scanRootFSGeneration(tx.QueryRow(ctx, rootFSGenerationSelectSQL()+`
		WHERE generation_id = $1 FOR SHARE
	`, lifecycle.TargetGenerationID))
	if err != nil {
		return nil, fmt.Errorf("load completed Nomad paused-rebase target generation: %w", err)
	}
	sourceArtifact, targetArtifact, err := lockNomadPausedRebaseArtifacts(
		ctx, tx, lifecycle.SourceBaseArtifactDigest, lifecycle.TargetBaseArtifactDigest,
	)
	if err != nil {
		return nil, err
	}
	if !nomadPausedRebaseLifecycleMatches(
		lifecycle, record, source, sourceArtifact, targetArtifact,
		lifecycle.TargetGenerationID, request.RollbackExpiresAt, true,
	) || target.ID != lifecycle.TargetGenerationID || target.ParentGenerationID != source.ID ||
		target.BaseArtifactDigest != targetArtifact.ArtifactDigest || target.FilesystemID != filesystem.ID {
		return nil, fmt.Errorf("%w: committed rebase output identity changed", ErrNomadSandboxRebaseConflict)
	}
	var rollbackExpiresAt *time.Time
	var rollbackState string
	if err := tx.QueryRow(ctx, `
		SELECT state, expires_at
		FROM manager.rootfs_head_rollbacks
		WHERE operation_id = $1 AND filesystem_id = $2 AND sandbox_id = $3
			AND team_id = $4 AND operation_kind = 'rebase'
			AND old_generation_id = $5 AND new_generation_id = $6
		FOR SHARE
	`, lifecycle.ID, filesystem.ID, record.ID, record.TeamID,
		source.ID, target.ID).Scan(&rollbackState, &rollbackExpiresAt); err != nil {
		return nil, fmt.Errorf("load completed Nomad paused-rebase rollback pin: %w", err)
	}
	if rollbackExpiresAt == nil || !rollbackExpiresAt.Equal(request.RollbackExpiresAt) ||
		(rollbackState != "available" && rollbackState != "rolled_back" && rollbackState != "expired") {
		return nil, fmt.Errorf("%w: committed rebase rollback identity changed", ErrNomadSandboxRebaseConflict)
	}
	candidate := nomadPausedRebaseCandidate(
		record, filesystem, source, sourceArtifact, targetArtifact, lifecycle, true,
	)
	candidate.TargetWriterEpoch = target.WriterEpoch
	return candidate, nil
}

func nomadPausedRebaseCandidate(
	record *SandboxRecord,
	filesystem *RootFSFilesystem,
	source *RootFSGeneration,
	sourceArtifact, targetArtifact *RootFSBaseArtifact,
	lifecycle *SandboxLifecycleTxn,
	completed bool,
) *NomadPausedRebaseCandidate {
	return &NomadPausedRebaseCandidate{
		Completed: completed, LifecyclePhase: lifecycle.Phase,
		Sandbox: cloneSandboxRecord(record), Filesystem: cloneRootFSFilesystem(filesystem),
		SourceGeneration:   cloneRootFSGeneration(source),
		SourceBaseArtifact: cloneRootFSBaseArtifact(sourceArtifact),
		TargetBaseArtifact: cloneRootFSBaseArtifact(targetArtifact),
		TargetGenerationID: lifecycle.TargetGenerationID,
		TargetWriterEpoch:  filesystem.WriterEpoch + 1,
		RollbackExpiresAt:  lifecycle.RollbackExpiresAt,
	}
}

func cloneSandboxRecord(record *SandboxRecord) *SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	return &clone
}

func cloneRootFSFilesystem(filesystem *RootFSFilesystem) *RootFSFilesystem {
	if filesystem == nil {
		return nil
	}
	clone := *filesystem
	return &clone
}

func cloneRootFSGeneration(generation *RootFSGeneration) *RootFSGeneration {
	if generation == nil {
		return nil
	}
	clone := *generation
	clone.Descriptor = append([]byte(nil), generation.Descriptor...)
	return &clone
}

func cloneRootFSBaseArtifact(artifact *RootFSBaseArtifact) *RootFSBaseArtifact {
	if artifact == nil {
		return nil
	}
	clone := *artifact
	clone.Descriptor = append([]byte(nil), artifact.Descriptor...)
	return &clone
}
