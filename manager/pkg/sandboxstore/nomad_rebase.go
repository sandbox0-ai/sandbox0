package sandboxstore

import (
	"bytes"
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
	WorkerClusterID          string
	WorkerNodeID             string
	WorkerNodeUID            string
}

// NomadPausedRebaseCandidate is the complete immutable input to a privileged
// file-aware rebase worker. TargetGenerationID and TargetWriterEpoch are
// reserved by PostgreSQL but do not grant write authority to a sandbox.
type NomadPausedRebaseCandidate struct {
	Completed            bool
	Rejected             bool
	LifecyclePhase       string
	Sandbox              *SandboxRecord
	Filesystem           *RootFSFilesystem
	SourceGeneration     *RootFSGeneration
	SourceBaseArtifact   *RootFSBaseArtifact
	TargetBaseArtifact   *RootFSBaseArtifact
	TargetGenerationID   string
	TargetWriterEpoch    int64
	RollbackExpiresAt    time.Time
	WorkerClusterID      string
	WorkerNodeID         string
	WorkerNodeUID        string
	WorkerProofDigest    []byte
	WorkerAcknowledgedAt time.Time
}

// ListPendingNomadPausedRebases returns unpublished work and terminal worker
// outcomes whose exact node-journal acknowledgement is still pending.
func (s *PGSandboxStore) ListPendingNomadPausedRebases(
	ctx context.Context,
	limit int,
) ([]*SandboxLifecycleTxn, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, lifecycleTxnSelectSQL()+`
		WHERE kind = $1
			AND (
				phase IN ('preparing', 'barriered', 'publishing', 'committing')
				OR (phase IN ('committed', 'aborted') AND worker_acknowledged_at IS NULL
					AND octet_length(worker_proof_digest) = $2)
			)
		ORDER BY updated_at ASC
		LIMIT $3
	`, SandboxLifecycleKindRebase, sha256.Size, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending Nomad paused rebases: %w", err)
	}
	defer rows.Close()
	result := make([]*SandboxLifecycleTxn, 0, limit)
	for rows.Next() {
		txn, scanErr := scanLifecycleTxnRows(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan pending Nomad paused rebase: %w", scanErr)
		}
		result = append(result, txn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending Nomad paused rebases: %w", err)
	}
	return result, nil
}

// GetPendingNomadPausedRebase returns the single active operation owned by a
// sandbox, or its latest terminal operation awaiting exact worker ack.
func (s *PGSandboxStore) GetPendingNomadPausedRebase(
	ctx context.Context,
	sandboxID string,
) (*SandboxLifecycleTxn, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	return scanLifecycleTxn(s.pool.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE sandbox_id = $1 AND kind = $2
			AND (
				phase IN ('preparing', 'barriered', 'publishing', 'committing')
				OR (phase IN ('committed', 'aborted') AND worker_acknowledged_at IS NULL
					AND octet_length(worker_proof_digest) = $3)
			)
		ORDER BY epoch DESC
		LIMIT 1
	`, sandboxID, SandboxLifecycleKindRebase, sha256.Size))
}

// AcknowledgeNomadPausedRebaseWorker records that the exact durable worker
// proof was released only after the node confirmed acknowledgement.
func (s *PGSandboxStore) AcknowledgeNomadPausedRebaseWorker(
	ctx context.Context,
	operationID, sandboxID, workerClusterID, workerNodeID, workerNodeUID string,
	proofDigest []byte,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	values := map[string]string{
		"operation_id": operationID, "sandbox_id": sandboxID, "worker_cluster_id": workerClusterID,
		"worker_node_id": workerNodeID, "worker_node_uid": workerNodeUID,
	}
	for name, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || value != values[name] || len(value) > 512 {
			return fmt.Errorf("%s is required, canonical, and at most 512 bytes", name)
		}
	}
	if len(proofDigest) != sha256.Size {
		return fmt.Errorf("worker proof digest must be a 32-byte SHA-256 digest")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET worker_acknowledged_at = COALESCE(worker_acknowledged_at, NOW()),
			updated_at = CASE WHEN worker_acknowledged_at IS NULL THEN NOW() ELSE updated_at END
		WHERE txn_id = $1 AND sandbox_id = $2 AND kind = $3 AND phase IN ($4, $5)
			AND worker_cluster_id = $6 AND worker_node_id = $7 AND worker_node_uid = $8
			AND worker_proof_digest = $9
	`, operationID, sandboxID, SandboxLifecycleKindRebase, SandboxLifecyclePhaseCommitted,
		SandboxLifecyclePhaseAborted, workerClusterID, workerNodeID, workerNodeUID, proofDigest)
	if err != nil {
		return fmt.Errorf("acknowledge Nomad paused-rebase worker: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: terminal worker proof identity changed", ErrNomadSandboxRebaseConflict)
	}
	return nil
}

// RejectNomadPausedRebaseWorker records an exact node proof instead of losing
// a result when sandbox deletion races worker execution. Cleanup remains
// fenced until the same proof is acknowledged on the same durable node.
func (s *PGSandboxStore) RejectNomadPausedRebaseWorker(
	ctx context.Context,
	request *NomadPausedRebaseRequest,
	proofDigest []byte,
) error {
	normalized, err := normalizeNomadPausedRebaseRequest(request)
	if err != nil {
		return err
	}
	if len(proofDigest) != sha256.Size {
		return fmt.Errorf("worker rejection proof must be a 32-byte SHA-256 digest")
	}
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin Nomad paused-rebase rejection tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SandboxID)
	if err != nil {
		return err
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, record.ID)
	if err != nil {
		return err
	}
	if record.TeamID != normalized.ExpectedTeamID || record.ClusterID != normalized.WorkerClusterID ||
		record.DesiredState != SandboxDesiredStateTerminating || !record.DeletedAt.IsZero() ||
		claim.Phase != SandboxRuntimeClaimPhaseCleanupPending || claim.CleanupStartedAt.IsZero() ||
		!claim.CleanedAt.IsZero() {
		return fmt.Errorf("%w: sandbox deletion does not own the worker rejection", ErrNomadSandboxRebaseConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
	`, normalized.OperationID, normalized.SandboxID))
	if err != nil {
		return fmt.Errorf("lock Nomad paused-rebase rejection lifecycle: %w", err)
	}
	if !nomadPausedRebaseRejectionIdentityMatches(lifecycle, record, normalized) {
		return fmt.Errorf("%w: worker rejection lifecycle identity changed", ErrNomadSandboxRebaseConflict)
	}
	filesystem, source, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, record.ID)
	if err != nil {
		return fmt.Errorf("load Nomad paused-rebase rejection source: %w", err)
	}
	if err := validateNomadPausedRebaseSource(filesystem, source, record.TeamID); err != nil {
		return err
	}
	sourceArtifact, targetArtifact, err := lockNomadPausedRebaseArtifacts(
		ctx, tx, source.BaseArtifactDigest, normalized.TargetBaseArtifactDigest,
	)
	if err != nil {
		return err
	}
	if err := validateNomadPausedRebaseArtifacts(source, sourceArtifact, targetArtifact); err != nil {
		return err
	}
	targetGenerationID := NomadPausedRebaseGenerationID(
		normalized.OperationID, record.ID, source.ID, targetArtifact.ArtifactDigest,
	)
	if lifecycle.ExpectedGenerationID != source.ID ||
		lifecycle.SourceBaseArtifactDigest != sourceArtifact.ArtifactDigest ||
		lifecycle.TargetGenerationID != targetGenerationID || lifecycle.PreparedGenerationID != "" {
		return fmt.Errorf("%w: worker rejection source lineage changed", ErrNomadSandboxRebaseConflict)
	}
	if err := ensureNomadPausedRebaseTargetGenerationAbsent(ctx, tx, targetGenerationID); err != nil {
		return err
	}
	if lifecycle.Phase == SandboxLifecyclePhaseAborted {
		if !bytes.Equal(lifecycle.WorkerProofDigest, proofDigest) || lifecycle.Error != "sandbox termination requested" {
			return fmt.Errorf("%w: worker rejection proof changed", ErrNomadSandboxRebaseConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit Nomad paused-rebase rejection retry: %w", err)
		}
		return nil
	}
	if !isActiveSandboxLifecyclePhase(lifecycle.Phase) || len(lifecycle.WorkerProofDigest) != 0 {
		return fmt.Errorf("%w: rebase lifecycle is not rejectable", ErrNomadSandboxRebaseConflict)
	}
	if !nomadPausedRebaseLifecycleMatches(
		lifecycle, record, source, sourceArtifact, targetArtifact,
		targetGenerationID, normalized.RollbackExpiresAt,
		normalized.WorkerClusterID, normalized.WorkerNodeID, normalized.WorkerNodeUID, false,
	) {
		return fmt.Errorf("%w: worker rejection request changed", ErrNomadSandboxRebaseConflict)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2, worker_proof_digest = $3,
			error = $4, aborted_at = NOW(), updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND octet_length(worker_proof_digest) = 0
	`, lifecycle.ID, SandboxLifecyclePhaseAborted, proofDigest, "sandbox termination requested")
	if err != nil {
		return fmt.Errorf("persist Nomad paused-rebase worker rejection: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: rebase lifecycle changed during rejection", ErrNomadSandboxRebaseConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Nomad paused-rebase worker rejection: %w", err)
	}
	return nil
}

func nomadPausedRebaseRejectionIdentityMatches(
	lifecycle *SandboxLifecycleTxn,
	record *SandboxRecord,
	request *NomadPausedRebaseRequest,
) bool {
	return lifecycle != nil && record != nil && request != nil &&
		lifecycle.SandboxID == record.ID && lifecycle.Kind == SandboxLifecycleKindRebase &&
		lifecycle.Source == SandboxLifecycleSourceManual && !lifecycle.Cancelable &&
		lifecycle.CancelRequestedAt.IsZero() && lifecycle.FromGeneration == lifecycle.ToGeneration &&
		lifecycle.FromGeneration == record.RuntimeGeneration && lifecycle.FromRuntimeNamespace == "" &&
		lifecycle.FromRuntimeID == "" && lifecycle.ToRuntimeNamespace == "" && lifecycle.ToRuntimeID == "" &&
		lifecycle.TargetSandboxID == "" && len(lifecycle.TargetRecordDigest) == 0 &&
		lifecycle.TargetGenerationID != "" && lifecycle.ExpectedGenerationID != "" &&
		lifecycle.SourceBaseArtifactDigest != "" &&
		lifecycle.TargetBaseArtifactDigest == request.TargetBaseArtifactDigest &&
		lifecycle.WorkerClusterID == request.WorkerClusterID &&
		lifecycle.WorkerNodeID == request.WorkerNodeID && lifecycle.WorkerNodeUID == request.WorkerNodeUID &&
		lifecycle.RollbackExpiresAt.Equal(request.RollbackExpiresAt)
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
	if record.ClusterID != normalized.WorkerClusterID {
		return nil, fmt.Errorf("%w: worker cluster does not own the sandbox", ErrNomadSandboxRebaseConflict)
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
	claim, err := lockSandboxRuntimeClaim(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if err := validateNomadPausedRebaseClaimState(record, claim, lifecycle); err != nil {
		return nil, err
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
	targetGenerationID := NomadPausedRebaseGenerationID(
		normalized.OperationID, record.ID, source.ID, targetArtifact.ArtifactDigest,
	)
	if lifecycle != nil && lifecycle.Phase == SandboxLifecyclePhaseAborted {
		if !nomadPausedRebaseRejectionIdentityMatches(lifecycle, record, normalized) ||
			lifecycle.ExpectedGenerationID != source.ID || lifecycle.SourceBaseArtifactDigest != sourceArtifact.ArtifactDigest ||
			lifecycle.TargetGenerationID != targetGenerationID || lifecycle.PreparedGenerationID != "" ||
			len(lifecycle.WorkerProofDigest) != sha256.Size ||
			lifecycle.Error != "sandbox termination requested" {
			return nil, fmt.Errorf("%w: rejected lifecycle does not match the exact rebase request",
				ErrNomadSandboxRebaseConflict)
		}
		if err := ensureNomadPausedRebaseTargetGenerationAbsent(ctx, tx, targetGenerationID); err != nil {
			return nil, err
		}
		candidate := nomadPausedRebaseCandidate(
			record, filesystem, source, sourceArtifact, targetArtifact, lifecycle, false,
		)
		candidate.Rejected = true
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit rejected Nomad paused-rebase retry: %w", err)
		}
		return candidate, nil
	}
	if err := ensureNomadPausedRebasePhysicalStateTerminal(ctx, tx, record.ID, filesystem.ID); err != nil {
		return nil, err
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, fmt.Errorf("read Nomad paused-rebase authority time: %w", err)
	}
	terminating := record.DesiredState == SandboxDesiredStateTerminating
	if !terminating && !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: sandbox hard TTL has expired", ErrNomadSandboxRebaseNotReady)
	}
	if !terminating && (!normalized.RollbackExpiresAt.After(authorityNow) ||
		normalized.RollbackExpiresAt.After(authorityNow.Add(MaxNomadPausedRebaseRollbackRetention))) {
		return nil, fmt.Errorf("%w: rollback deadline must be within the next %s",
			ErrNomadSandboxRebaseConflict, MaxNomadPausedRebaseRollbackRetention)
	}
	if filesystem.WriterEpoch == math.MaxInt64 {
		return nil, fmt.Errorf("%w: RootFS writer epoch is exhausted", ErrNomadSandboxRebaseConflict)
	}
	if lifecycle != nil {
		if !nomadPausedRebaseLifecycleMatches(
			lifecycle, record, source, sourceArtifact, targetArtifact,
			targetGenerationID, normalized.RollbackExpiresAt,
			normalized.WorkerClusterID, normalized.WorkerNodeID, normalized.WorkerNodeUID, false,
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
		TargetGenerationID: targetGenerationID, ExpectedGenerationID: source.ID,
		SourceBaseArtifactDigest: sourceArtifact.ArtifactDigest,
		TargetBaseArtifactDigest: targetArtifact.ArtifactDigest,
		RollbackExpiresAt:        normalized.RollbackExpiresAt,
		WorkerClusterID:          normalized.WorkerClusterID,
		WorkerNodeID:             normalized.WorkerNodeID,
		WorkerNodeUID:            normalized.WorkerNodeUID,
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
	normalized.WorkerClusterID = strings.TrimSpace(request.WorkerClusterID)
	normalized.WorkerNodeID = strings.TrimSpace(request.WorkerNodeID)
	normalized.WorkerNodeUID = strings.TrimSpace(request.WorkerNodeUID)
	for name, value := range map[string]string{
		"operation_id": normalized.OperationID, "sandbox_id": normalized.SandboxID,
		"expected_team_id":  normalized.ExpectedTeamID,
		"worker_cluster_id": normalized.WorkerClusterID, "worker_node_id": normalized.WorkerNodeID,
		"worker_node_uid": normalized.WorkerNodeUID,
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

func validateNomadPausedRebaseClaimState(
	record *SandboxRecord,
	claim *SandboxRuntimeClaim,
	lifecycle *SandboxLifecycleTxn,
) error {
	if record == nil ||
		!record.DeletedAt.IsZero() ||
		record.RuntimeGeneration < 0 || record.RuntimeNamespace != "" || record.RuntimeID != "" {
		return fmt.Errorf("%w: sandbox is not a canonical paused Nomad runtime", ErrNomadSandboxRebaseNotReady)
	}
	if claim == nil || claim.OperationID == "" || !claim.LeaseExpiresAt.IsZero() || !claim.CleanedAt.IsZero() {
		return fmt.Errorf("%w: sandbox runtime claim is not canonical", ErrNomadSandboxRebaseNotReady)
	}
	if record.DesiredState == SandboxDesiredStatePaused && claim.Phase == SandboxRuntimeClaimPhaseReady &&
		claim.CleanupStartedAt.IsZero() {
		return nil
	}
	if record.DesiredState == SandboxDesiredStateTerminating &&
		claim.Phase == SandboxRuntimeClaimPhaseCleanupPending && !claim.CleanupStartedAt.IsZero() &&
		lifecycle != nil && lifecycle.Kind == SandboxLifecycleKindRebase &&
		(isActiveSandboxLifecyclePhase(lifecycle.Phase) || lifecycle.Phase == SandboxLifecyclePhaseAborted) {
		return nil
	}
	return fmt.Errorf("%w: sandbox runtime claim is not a canonical paused or terminating rebase claim",
		ErrNomadSandboxRebaseNotReady)
}

func validateNomadPausedRebaseSource(
	filesystem *RootFSFilesystem,
	source *RootFSGeneration,
	teamID string,
) error {
	if filesystem == nil || source == nil ||
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
	workerClusterID, workerNodeID, workerNodeUID string,
	committed bool,
) bool {
	if lifecycle == nil || record == nil || source == nil || sourceArtifact == nil || targetArtifact == nil ||
		lifecycle.SandboxID != record.ID || lifecycle.Kind != SandboxLifecycleKindRebase ||
		lifecycle.Source != SandboxLifecycleSourceManual || lifecycle.Cancelable ||
		!lifecycle.CancelRequestedAt.IsZero() || lifecycle.FromGeneration != lifecycle.ToGeneration ||
		lifecycle.FromGeneration != record.RuntimeGeneration || lifecycle.FromRuntimeNamespace != "" ||
		lifecycle.FromRuntimeID != "" || lifecycle.ToRuntimeNamespace != "" || lifecycle.ToRuntimeID != "" ||
		lifecycle.TargetSandboxID != "" || len(lifecycle.TargetRecordDigest) != 0 ||
		lifecycle.TargetGenerationID != targetGenerationID || lifecycle.ExpectedGenerationID != source.ID ||
		lifecycle.SourceBaseArtifactDigest != sourceArtifact.ArtifactDigest ||
		lifecycle.TargetBaseArtifactDigest != targetArtifact.ArtifactDigest ||
		lifecycle.WorkerClusterID != record.ClusterID || lifecycle.WorkerClusterID != workerClusterID ||
		lifecycle.WorkerNodeID != workerNodeID ||
		lifecycle.WorkerNodeUID != workerNodeUID ||
		!lifecycle.RollbackExpiresAt.Equal(rollbackExpiresAt) {
		return false
	}
	if committed {
		return lifecycle.Phase == SandboxLifecyclePhaseCommitted &&
			lifecycle.PreparedGenerationID == lifecycle.TargetGenerationID &&
			len(lifecycle.WorkerProofDigest) == sha256.Size
	}
	return lifecycle.PreparedGenerationID == "" &&
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
		lifecycle.WorkerClusterID != request.WorkerClusterID || lifecycle.WorkerNodeID != request.WorkerNodeID ||
		lifecycle.WorkerNodeUID != request.WorkerNodeUID ||
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
	`, lifecycle.ExpectedGenerationID))
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
		lifecycle.TargetGenerationID, request.RollbackExpiresAt,
		request.WorkerClusterID, request.WorkerNodeID, request.WorkerNodeUID, true,
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
		SourceGeneration:     cloneRootFSGeneration(source),
		SourceBaseArtifact:   cloneRootFSBaseArtifact(sourceArtifact),
		TargetBaseArtifact:   cloneRootFSBaseArtifact(targetArtifact),
		TargetGenerationID:   lifecycle.TargetGenerationID,
		TargetWriterEpoch:    filesystem.WriterEpoch + 1,
		RollbackExpiresAt:    lifecycle.RollbackExpiresAt,
		WorkerClusterID:      lifecycle.WorkerClusterID,
		WorkerNodeID:         lifecycle.WorkerNodeID,
		WorkerNodeUID:        lifecycle.WorkerNodeUID,
		WorkerProofDigest:    append([]byte(nil), lifecycle.WorkerProofDigest...),
		WorkerAcknowledgedAt: lifecycle.WorkerAcknowledgedAt,
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
	clone.Attestation = append([]byte(nil), artifact.Attestation...)
	return &clone
}
