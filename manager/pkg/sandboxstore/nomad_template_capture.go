package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNomadTemplateCaptureConflict = errors.New("nomad template capture conflict")
	ErrNomadTemplateCaptureNotReady = errors.New("nomad template capture is not ready")
)

const (
	nomadTemplateCaptureStatePending   = "pending"
	nomadTemplateCaptureStatePublished = "published"

	NomadRunningRootFSCaptureKindTemplate = "template"
	NomadRunningRootFSCaptureKindSnapshot = "snapshot"
)

// NomadTemplateCaptureRequest identifies one deterministic internal snapshot
// of an exact live writer.
type NomadTemplateCaptureRequest struct {
	OperationID     string
	SourceSandboxID string
	TeamID          string
	SnapshotID      string
}

// NomadRunningRootFSCaptureRequest identifies one deterministic immutable
// checkpoint and the public or internal snapshot metadata published with it.
type NomadRunningRootFSCaptureRequest struct {
	OperationID     string
	SourceSandboxID string
	TeamID          string
	SnapshotID      string
	CaptureKind     string
	Name            string
	Description     string
	ExpiresAt       time.Time
}

// NomadTemplateCaptureCandidate authorizes a node checkpoint or returns its
// already-published immutable snapshot.
type NomadTemplateCaptureCandidate struct {
	OperationID         string
	SnapshotID          string
	TargetFilesystemID  string
	TargetGenerationID  string
	Completed           bool
	Snapshot            *RootFSSnapshot
	Source              *SandboxRecord
	Slot                *RuntimeSlot
	SourceFilesystemID  string
	SourceGenerationID  string
	SourceWriterGrantID string
	SourceWriterEpoch   int64
	BindingVersion      int
	BindingDigest       []byte
	TeamID              string
	CaptureKind         string
	Name                string
	Description         string
	ExpiresAt           time.Time
}

type nomadTemplateCaptureIntent struct {
	OperationID          string
	SnapshotID           string
	TeamID               string
	SourceSandboxID      string
	SourceFilesystemID   string
	SourceGrantID        string
	SourceWriterEpoch    int64
	SourceGenerationID   string
	TargetFilesystemID   string
	CheckpointGeneration string
	RequestDigest        []byte
	BindingVersion       int
	BindingDigest        []byte
	State                string
	CheckpointSequence   *int64
	DescriptorDigest     *string
	ProofDigest          []byte
	CaptureKind          string
	Name                 string
	Description          string
	ExpiresAt            time.Time
}

// NomadTemplateCaptureFilesystemID derives the immutable unbound filesystem
// retained by a running-source template snapshot.
func NomadTemplateCaptureFilesystemID(operationID, snapshotID string) string {
	sum := sha256.Sum256([]byte(operationID + "\x00" + snapshotID + "\x00filesystem"))
	return "nomad-template-capture-fs-" + hex.EncodeToString(sum[:16])
}

// NomadTemplateCaptureGenerationID derives the checkpoint generation before
// the node creates any block object.
func NomadTemplateCaptureGenerationID(operationID, snapshotID string) string {
	sum := sha256.Sum256([]byte(operationID + "\x00" + snapshotID + "\x00generation"))
	return "nomad-template-capture-gen-" + hex.EncodeToString(sum[:16])
}

// RequestNomadRunningTemplateCapture reserves one exact source lifecycle and
// returns the authoritative writer/node identity for checkpoint dispatch.
func (s *PGSandboxStore) RequestNomadRunningTemplateCapture(
	ctx context.Context,
	request *NomadTemplateCaptureRequest,
) (*NomadTemplateCaptureCandidate, error) {
	if request == nil {
		return nil, fmt.Errorf("nomad template capture request is required")
	}
	return s.RequestNomadRunningRootFSCapture(ctx, &NomadRunningRootFSCaptureRequest{
		OperationID: request.OperationID, SourceSandboxID: request.SourceSandboxID,
		TeamID: request.TeamID, SnapshotID: request.SnapshotID,
		CaptureKind: NomadRunningRootFSCaptureKindTemplate,
		Name:        "Template RootFS capture",
		Description: "Internal immutable live-writer checkpoint retained by a template.",
	})
}

// RequestNomadRunningRootFSCapture reserves one exact source lifecycle and
// returns the authoritative writer/node identity for checkpoint dispatch.
func (s *PGSandboxStore) RequestNomadRunningRootFSCapture(
	ctx context.Context,
	request *NomadRunningRootFSCaptureRequest,
) (*NomadTemplateCaptureCandidate, error) {
	normalized, requestDigest, targetFilesystemID, targetGenerationID, err :=
		normalizeNomadTemplateCaptureRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad template capture request tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	source, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SourceSandboxID)
	if err != nil {
		return nil, err
	}
	if source.TeamID != normalized.TeamID {
		return nil, fmt.Errorf("%w: source team identity changed", ErrNomadTemplateCaptureConflict)
	}
	intent, err := getNomadTemplateCaptureIntentForUpdate(ctx, tx, normalized.OperationID)
	if err != nil {
		return nil, err
	}
	if intent != nil {
		if !nomadTemplateCaptureIntentMatches(
			intent, normalized, requestDigest, targetFilesystemID, targetGenerationID,
		) {
			return nil, fmt.Errorf("%w: capture operation identity changed", ErrNomadTemplateCaptureConflict)
		}
		if intent.State == nomadTemplateCaptureStatePublished {
			candidate, err := loadCompletedNomadTemplateCapture(ctx, tx, source, intent)
			if err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit completed Nomad template capture retry: %w", err)
			}
			return candidate, nil
		}
		candidate, err := lockPendingNomadTemplateCapture(ctx, tx, source, intent)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit pending Nomad template capture retry: %w", err)
		}
		return candidate, nil
	}

	active, err := getActiveLifecycleTxn(ctx, tx, source.ID)
	if err != nil {
		return nil, fmt.Errorf("load active Nomad template capture lifecycle: %w", err)
	}
	if active != nil {
		return nil, fmt.Errorf("%w: lifecycle %s owns the source sandbox",
			ErrNomadTemplateCaptureConflict, active.ID)
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, source)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNomadTemplateCaptureNotReady, err)
	}
	lifecycle := &SandboxLifecycleTxn{
		ID: normalized.OperationID, SandboxID: source.ID,
		Kind: SandboxLifecycleKindSnapshot, Phase: SandboxLifecyclePhasePublishing,
		Source: SandboxLifecycleSourceManual, Cancelable: false,
		FromGeneration: source.RuntimeGeneration, ToGeneration: source.RuntimeGeneration,
		FromRuntimeNamespace: source.RuntimeNamespace, FromRuntimeID: source.RuntimeID,
		TargetSandboxID: targetFilesystemID, TargetGenerationID: targetGenerationID,
		TargetRecordDigest: requestDigest, ExpectedGenerationID: writer.generation.ID,
	}
	if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, lifecycle); err != nil {
		return nil, fmt.Errorf("begin Nomad template capture lifecycle: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_running_template_captures (
			operation_id, snapshot_id, team_id, source_sandbox_id,
			source_filesystem_id, source_grant_id, source_writer_epoch,
			source_generation_id, target_filesystem_id, checkpoint_generation_id,
			request_digest, binding_version, binding_digest, state,
			capture_kind, snapshot_name, snapshot_description, snapshot_expires_at,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, NOW(), NOW())
	`, normalized.OperationID, normalized.SnapshotID, normalized.TeamID, source.ID,
		writer.filesystem.ID, writer.grant.ID, writer.grant.WriterEpoch, writer.generation.ID,
		targetFilesystemID, targetGenerationID, requestDigest,
		writer.grant.BindingVersion, writer.grant.BindingDigest, nomadTemplateCaptureStatePending,
		normalized.CaptureKind, normalized.Name, normalized.Description, nullableTime(normalized.ExpiresAt)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, fmt.Errorf("%w: capture identity is already reserved", ErrNomadTemplateCaptureConflict)
		}
		return nil, fmt.Errorf("insert Nomad template capture intent: %w", err)
	}
	intent = &nomadTemplateCaptureIntent{
		OperationID: normalized.OperationID, SnapshotID: normalized.SnapshotID, TeamID: normalized.TeamID,
		SourceSandboxID: source.ID, SourceFilesystemID: writer.filesystem.ID,
		SourceGrantID: writer.grant.ID, SourceWriterEpoch: writer.grant.WriterEpoch,
		SourceGenerationID: writer.generation.ID, TargetFilesystemID: targetFilesystemID,
		CheckpointGeneration: targetGenerationID, RequestDigest: append([]byte(nil), requestDigest...),
		BindingVersion: writer.grant.BindingVersion,
		BindingDigest:  append([]byte(nil), writer.grant.BindingDigest...), State: nomadTemplateCaptureStatePending,
		CaptureKind: normalized.CaptureKind, Name: normalized.Name,
		Description: normalized.Description, ExpiresAt: normalized.ExpiresAt,
	}
	candidate := nomadTemplateCaptureCandidate(source, writer, intent)
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad template capture request: %w", err)
	}
	return candidate, nil
}

// ContinueNomadRunningRootFSCapture reconstructs a pending checkpoint from
// PostgreSQL so the controller can finish it after an API response or manager
// process is lost.
func (s *PGSandboxStore) ContinueNomadRunningRootFSCapture(
	ctx context.Context,
	sourceSandboxID string,
) (*NomadTemplateCaptureCandidate, error) {
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	if sourceSandboxID == "" {
		return nil, fmt.Errorf("source_sandbox_id is required")
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad running capture continuation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	source, err := lockNomadSandboxClaimRecord(ctx, tx, sourceSandboxID)
	if err != nil {
		if errors.Is(err, ErrSandboxRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	lifecycle, err := getActiveLifecycleTxn(ctx, tx, sourceSandboxID)
	if err != nil {
		return nil, fmt.Errorf("load active Nomad running capture lifecycle: %w", err)
	}
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindSnapshot {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit absent Nomad running capture continuation: %w", err)
		}
		return nil, nil
	}
	intent, err := getNomadTemplateCaptureIntentForUpdate(ctx, tx, lifecycle.ID)
	if err != nil {
		return nil, err
	}
	if intent == nil {
		return nil, fmt.Errorf("%w: running capture intent is missing", ErrNomadTemplateCaptureConflict)
	}
	candidate, err := lockPendingNomadTemplateCapture(ctx, tx, source, intent)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad running capture continuation: %w", err)
	}
	return candidate, nil
}

// AbortStaleNomadRunningRootFSCapture releases a pending capture only after
// its exact live writer identity has changed. Publication and abort serialize
// on the source row, so a committed checkpoint always wins the race.
func (s *PGSandboxStore) AbortStaleNomadRunningRootFSCapture(
	ctx context.Context,
	operationID, sourceSandboxID, reason string,
) (bool, error) {
	operationID = strings.TrimSpace(operationID)
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	reason = strings.TrimSpace(reason)
	if operationID == "" || sourceSandboxID == "" || reason == "" {
		return false, fmt.Errorf("operation, source sandbox, and abort reason are required")
	}
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin stale Nomad running capture abort tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	source, err := lockNomadSandboxClaimRecord(ctx, tx, sourceSandboxID)
	if err != nil {
		return false, err
	}
	intent, err := getNomadTemplateCaptureIntentForUpdate(ctx, tx, operationID)
	if err != nil {
		return false, err
	}
	if intent == nil || intent.State == nomadTemplateCaptureStatePublished {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit terminal Nomad running capture abort: %w", err)
		}
		return false, nil
	}
	if intent.SourceSandboxID != sourceSandboxID {
		return false, fmt.Errorf("%w: capture source identity changed", ErrNomadTemplateCaptureConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
	`, operationID, sourceSandboxID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("lock stale Nomad running capture lifecycle: %w", err)
	}
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindSnapshot ||
		lifecycle.Phase != SandboxLifecyclePhasePublishing ||
		lifecycle.TargetSandboxID != intent.TargetFilesystemID ||
		lifecycle.TargetGenerationID != intent.CheckpointGeneration ||
		!bytes.Equal(lifecycle.TargetRecordDigest, intent.RequestDigest) {
		return false, fmt.Errorf("%w: pending capture lifecycle changed", ErrNomadTemplateCaptureConflict)
	}
	writer, writerErr := lockExactNomadLiveWriter(ctx, tx, source)
	if writerErr == nil && lifecycle.FromGeneration == source.RuntimeGeneration &&
		lifecycle.FromRuntimeNamespace == source.RuntimeNamespace && lifecycle.FromRuntimeID == source.RuntimeID &&
		writer.filesystem.ID == intent.SourceFilesystemID && writer.generation.ID == intent.SourceGenerationID &&
		writer.grant.ID == intent.SourceGrantID && writer.grant.WriterEpoch == intent.SourceWriterEpoch &&
		writer.grant.BindingVersion == intent.BindingVersion && bytes.Equal(writer.grant.BindingDigest, intent.BindingDigest) {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit recoverable Nomad running capture check: %w", err)
		}
		return false, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2, error = $3, aborted_at = NOW(), updated_at = NOW()
		WHERE txn_id = $1 AND sandbox_id = $4 AND phase = $5
	`, operationID, SandboxLifecyclePhaseAborted, reason, sourceSandboxID,
		SandboxLifecyclePhasePublishing)
	if err != nil {
		return false, fmt.Errorf("abort stale Nomad running capture lifecycle: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("%w: stale capture lifecycle changed", ErrNomadTemplateCaptureConflict)
	}
	tag, err = tx.Exec(ctx, `
		DELETE FROM manager.rootfs_running_template_captures
		WHERE operation_id = $1 AND state = $2
	`, operationID, nomadTemplateCaptureStatePending)
	if err != nil {
		return false, fmt.Errorf("delete stale Nomad running capture intent: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("%w: stale capture intent changed", ErrNomadTemplateCaptureConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit stale Nomad running capture abort: %w", err)
	}
	return true, nil
}

func normalizeNomadTemplateCaptureRequest(
	request *NomadRunningRootFSCaptureRequest,
) (*NomadRunningRootFSCaptureRequest, []byte, string, string, error) {
	if request == nil {
		return nil, nil, "", "", fmt.Errorf("nomad template capture request is required")
	}
	normalized := *request
	normalized.OperationID = strings.TrimSpace(request.OperationID)
	normalized.SourceSandboxID = strings.TrimSpace(request.SourceSandboxID)
	normalized.TeamID = strings.TrimSpace(request.TeamID)
	normalized.SnapshotID = strings.TrimSpace(request.SnapshotID)
	normalized.CaptureKind = strings.TrimSpace(request.CaptureKind)
	normalized.Name = strings.TrimSpace(request.Name)
	normalized.Description = strings.TrimSpace(request.Description)
	if !normalized.ExpiresAt.IsZero() {
		// PostgreSQL timestamps have microsecond precision. Normalize before the
		// intent comparison so an idempotent retry cannot conflict only because
		// the original HTTP timestamp carried nanoseconds.
		normalized.ExpiresAt = normalized.ExpiresAt.UTC().Truncate(time.Microsecond)
	}
	for name, value := range map[string]string{
		"operation_id": normalized.OperationID, "source_sandbox_id": normalized.SourceSandboxID,
		"team_id": normalized.TeamID, "snapshot_id": normalized.SnapshotID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return nil, nil, "", "", fmt.Errorf("%s must be canonical and at most 512 bytes", name)
		}
	}
	if normalized.CaptureKind != NomadRunningRootFSCaptureKindTemplate &&
		normalized.CaptureKind != NomadRunningRootFSCaptureKindSnapshot {
		return nil, nil, "", "", fmt.Errorf("capture_kind must be template or snapshot")
	}
	targetFilesystemID := NomadTemplateCaptureFilesystemID(normalized.OperationID, normalized.SnapshotID)
	targetGenerationID := NomadTemplateCaptureGenerationID(normalized.OperationID, normalized.SnapshotID)
	payload := strings.Join([]string{
		normalized.OperationID, normalized.SourceSandboxID, normalized.TeamID, normalized.SnapshotID,
		targetFilesystemID, targetGenerationID,
	}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return &normalized, digest[:], targetFilesystemID, targetGenerationID, nil
}

func getNomadTemplateCaptureIntentForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	operationID string,
) (*nomadTemplateCaptureIntent, error) {
	var intent nomadTemplateCaptureIntent
	var expiresAt *time.Time
	err := tx.QueryRow(ctx, `
		SELECT operation_id, snapshot_id, team_id, source_sandbox_id,
			source_filesystem_id, source_grant_id, source_writer_epoch,
			source_generation_id, target_filesystem_id, checkpoint_generation_id,
			request_digest, binding_version, binding_digest, state,
			checkpoint_sequence, checkpoint_descriptor_digest, checkpoint_proof_digest,
			capture_kind, snapshot_name, snapshot_description, snapshot_expires_at
		FROM manager.rootfs_running_template_captures
		WHERE operation_id = $1
		FOR UPDATE
	`, operationID).Scan(
		&intent.OperationID, &intent.SnapshotID, &intent.TeamID, &intent.SourceSandboxID,
		&intent.SourceFilesystemID, &intent.SourceGrantID, &intent.SourceWriterEpoch,
		&intent.SourceGenerationID, &intent.TargetFilesystemID, &intent.CheckpointGeneration,
		&intent.RequestDigest, &intent.BindingVersion, &intent.BindingDigest, &intent.State,
		&intent.CheckpointSequence, &intent.DescriptorDigest, &intent.ProofDigest,
		&intent.CaptureKind, &intent.Name, &intent.Description, &expiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load Nomad template capture intent: %w", err)
	}
	intent.ExpiresAt = derefTime(expiresAt)
	return &intent, nil
}

func nomadTemplateCaptureIntentMatches(
	intent *nomadTemplateCaptureIntent,
	request *NomadRunningRootFSCaptureRequest,
	requestDigest []byte,
	targetFilesystemID, targetGenerationID string,
) bool {
	return intent != nil && request != nil && intent.OperationID == request.OperationID &&
		intent.SnapshotID == request.SnapshotID && intent.TeamID == request.TeamID &&
		intent.SourceSandboxID == request.SourceSandboxID &&
		intent.TargetFilesystemID == targetFilesystemID &&
		intent.CheckpointGeneration == targetGenerationID && bytes.Equal(intent.RequestDigest, requestDigest) &&
		intent.CaptureKind == request.CaptureKind && intent.Name == request.Name &&
		intent.Description == request.Description && intent.ExpiresAt.Equal(request.ExpiresAt)
}

func lockPendingNomadTemplateCapture(
	ctx context.Context,
	tx pgx.Tx,
	source *SandboxRecord,
	intent *nomadTemplateCaptureIntent,
) (*NomadTemplateCaptureCandidate, error) {
	lifecycle, err := getActiveLifecycleTxn(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	if !nomadTemplateCaptureLifecycleMatches(lifecycle, source, intent, false) {
		return nil, fmt.Errorf("%w: pending capture lifecycle changed", ErrNomadTemplateCaptureConflict)
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, source)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNomadTemplateCaptureNotReady, err)
	}
	if writer.filesystem.ID != intent.SourceFilesystemID || writer.generation.ID != intent.SourceGenerationID ||
		writer.grant.ID != intent.SourceGrantID || writer.grant.WriterEpoch != intent.SourceWriterEpoch ||
		writer.grant.BindingVersion != intent.BindingVersion ||
		!bytes.Equal(writer.grant.BindingDigest, intent.BindingDigest) {
		return nil, fmt.Errorf("%w: live writer changed", ErrNomadTemplateCaptureConflict)
	}
	return nomadTemplateCaptureCandidate(source, writer, intent), nil
}

func nomadTemplateCaptureCandidate(
	source *SandboxRecord,
	writer *exactNomadLiveWriter,
	intent *nomadTemplateCaptureIntent,
) *NomadTemplateCaptureCandidate {
	return &NomadTemplateCaptureCandidate{
		OperationID: intent.OperationID, SnapshotID: intent.SnapshotID,
		TargetFilesystemID: intent.TargetFilesystemID, TargetGenerationID: intent.CheckpointGeneration,
		Source: source, Slot: writer.slot, SourceFilesystemID: writer.filesystem.ID,
		SourceGenerationID: writer.generation.ID, SourceWriterGrantID: writer.grant.ID,
		SourceWriterEpoch: writer.grant.WriterEpoch, BindingVersion: writer.grant.BindingVersion,
		BindingDigest: append([]byte(nil), writer.grant.BindingDigest...),
		TeamID:        intent.TeamID, CaptureKind: intent.CaptureKind, Name: intent.Name,
		Description: intent.Description, ExpiresAt: intent.ExpiresAt,
	}
}

func loadCompletedNomadTemplateCapture(
	ctx context.Context,
	tx pgx.Tx,
	source *SandboxRecord,
	intent *nomadTemplateCaptureIntent,
) (*NomadTemplateCaptureCandidate, error) {
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR SHARE
	`, intent.OperationID, source.ID))
	if err != nil {
		return nil, fmt.Errorf("load committed Nomad template capture lifecycle: %w", err)
	}
	if !nomadTemplateCaptureLifecycleMatches(lifecycle, source, intent, true) {
		return nil, fmt.Errorf("%w: committed capture lifecycle changed", ErrNomadTemplateCaptureConflict)
	}
	snapshot, err := scanRootFSSnapshot(tx.QueryRow(ctx, `
		SELECT s.snapshot_id, s.filesystem_id, s.team_id, s.source_sandbox_id,
			s.head_generation_id,
			g.base_artifact_digest, g.format_generation, g.source_oci_digest,
			s.name, s.description, s.created_at, s.expires_at
		FROM manager.rootfs_snapshots s
		JOIN manager.rootfs_generations g
		  ON g.generation_id = s.head_generation_id
		WHERE s.snapshot_id = $1 AND s.team_id = $2
	`, intent.SnapshotID, intent.TeamID))
	if err != nil {
		return nil, fmt.Errorf("load published Nomad template snapshot: %w", err)
	}
	if snapshot.FilesystemID != intent.TargetFilesystemID ||
		snapshot.HeadGenerationID != intent.CheckpointGeneration ||
		snapshot.SourceSandboxID != intent.SourceSandboxID || snapshot.Name != intent.Name ||
		snapshot.Description != intent.Description || !snapshot.ExpiresAt.Equal(intent.ExpiresAt) {
		return nil, fmt.Errorf("%w: published snapshot identity changed", ErrNomadTemplateCaptureConflict)
	}
	return &NomadTemplateCaptureCandidate{
		OperationID: intent.OperationID, SnapshotID: intent.SnapshotID,
		TargetFilesystemID: intent.TargetFilesystemID, TargetGenerationID: intent.CheckpointGeneration,
		Completed: true, Snapshot: snapshot, Source: source,
		TeamID: intent.TeamID, CaptureKind: intent.CaptureKind, Name: intent.Name,
		Description: intent.Description, ExpiresAt: intent.ExpiresAt,
	}, nil
}

func nomadTemplateCaptureLifecycleMatches(
	lifecycle *SandboxLifecycleTxn,
	source *SandboxRecord,
	intent *nomadTemplateCaptureIntent,
	committed bool,
) bool {
	if lifecycle == nil || source == nil || intent == nil ||
		source.ID != intent.SourceSandboxID || source.TeamID != intent.TeamID ||
		lifecycle.ID != intent.OperationID || lifecycle.SandboxID != source.ID ||
		lifecycle.Kind != SandboxLifecycleKindSnapshot || lifecycle.Source != SandboxLifecycleSourceManual ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		lifecycle.FromGeneration != lifecycle.ToGeneration ||
		lifecycle.ToRuntimeNamespace != "" || lifecycle.ToRuntimeID != "" ||
		lifecycle.TargetSandboxID != intent.TargetFilesystemID ||
		lifecycle.TargetGenerationID != intent.CheckpointGeneration ||
		lifecycle.ExpectedGenerationID != intent.SourceGenerationID ||
		!bytes.Equal(lifecycle.TargetRecordDigest, intent.RequestDigest) {
		return false
	}
	if committed {
		return lifecycle.Phase == SandboxLifecyclePhaseCommitted &&
			lifecycle.PreparedGenerationID == intent.CheckpointGeneration
	}
	return lifecycle.Phase == SandboxLifecyclePhasePublishing && lifecycle.PreparedGenerationID == "" &&
		lifecycle.FromGeneration == source.RuntimeGeneration &&
		lifecycle.FromRuntimeNamespace == source.RuntimeNamespace &&
		lifecycle.FromRuntimeID == source.RuntimeID
}

// DeleteTemplateBuildRootFSCapture cancels an unpublished exact-writer
// capture or releases a published or paused-source internal snapshot. Both
// worker modes use this entry point so migration-time cleanup cannot
// misinterpret a block capture as a legacy snapshot.
func (s *PGSandboxStore) DeleteTemplateBuildRootFSCapture(
	ctx context.Context,
	snapshotID, teamID string,
) error {
	snapshotID = strings.TrimSpace(snapshotID)
	teamID = strings.TrimSpace(teamID)
	if snapshotID == "" || teamID == "" {
		return fmt.Errorf("template capture snapshot and team identities are required")
	}
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	var sourceSandboxID string
	err := s.pool.QueryRow(ctx, `
		SELECT source_sandbox_id
		FROM manager.rootfs_running_template_captures
		WHERE snapshot_id = $1 AND team_id = $2
	`, snapshotID, teamID).Scan(&sourceSandboxID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.DeleteRootFSSnapshot(ctx, snapshotID, teamID)
		if errors.Is(err, ErrRootFSSnapshotNotFound) {
			return nil
		}
		return err
	}
	if err != nil {
		return fmt.Errorf("locate Nomad template capture cleanup: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin Nomad template capture cleanup tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Publication locks the source row before the intent. Preserve that order
	// so cleanup and node callbacks cannot deadlock or both commit.
	var sourceExists bool
	err = tx.QueryRow(ctx, `
		SELECT TRUE FROM manager.sandboxes WHERE sandbox_id = $1 FOR UPDATE
	`, sourceSandboxID).Scan(&sourceExists)
	if errors.Is(err, pgx.ErrNoRows) {
		sourceExists = false
	} else if err != nil {
		return fmt.Errorf("lock Nomad template capture source for cleanup: %w", err)
	}
	intent, err := getNomadTemplateCaptureIntentForUpdateBySnapshot(ctx, tx, snapshotID, teamID)
	if err != nil {
		return err
	}
	if intent == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit absent Nomad template capture cleanup: %w", err)
		}
		return nil
	}
	if intent.State == nomadTemplateCaptureStatePending && sourceExists {
		lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
			WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
		`, intent.OperationID, intent.SourceSandboxID))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock pending template capture lifecycle for cleanup: %w", err)
		}
		if lifecycle != nil && lifecycle.Phase == SandboxLifecyclePhasePublishing &&
			lifecycle.Kind == SandboxLifecycleKindSnapshot &&
			lifecycle.TargetSandboxID == intent.TargetFilesystemID &&
			lifecycle.TargetGenerationID == intent.CheckpointGeneration {
			if _, err := tx.Exec(ctx, `
				UPDATE manager.sandbox_lifecycle_txns
				SET phase = $2, error = $3, aborted_at = NOW(), updated_at = NOW()
				WHERE txn_id = $1 AND phase = $4
			`, intent.OperationID, SandboxLifecyclePhaseAborted,
				"template build was canceled before capture publication",
				SandboxLifecyclePhasePublishing); err != nil {
				return fmt.Errorf("abort pending template capture lifecycle: %w", err)
			}
		}
	}
	if intent.State == nomadTemplateCaptureStatePublished {
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_snapshots
			WHERE snapshot_id = $1 AND team_id = $2
		`, snapshotID, teamID); err != nil {
			return fmt.Errorf("delete published template capture snapshot: %w", err)
		}
	}
	if intent.State != nomadTemplateCaptureStatePublished {
		if _, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_running_template_captures
			WHERE operation_id = $1
		`, intent.OperationID); err != nil {
			return fmt.Errorf("delete Nomad template capture intent: %w", err)
		}
	} else if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_running_template_captures
		SET cancel_reason = 'template released its RootFS snapshot', updated_at = NOW()
		WHERE operation_id = $1 AND state = $2
	`, intent.OperationID, nomadTemplateCaptureStatePublished); err != nil {
		return fmt.Errorf("mark Nomad template capture released: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Nomad template capture cleanup: %w", err)
	}
	return nil
}

func getNomadTemplateCaptureIntentForUpdateBySnapshot(
	ctx context.Context,
	tx pgx.Tx,
	snapshotID, teamID string,
) (*nomadTemplateCaptureIntent, error) {
	var operationID string
	err := tx.QueryRow(ctx, `
		SELECT operation_id
		FROM manager.rootfs_running_template_captures
		WHERE snapshot_id = $1 AND team_id = $2
	`, snapshotID, teamID).Scan(&operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load Nomad template capture cleanup intent: %w", err)
	}
	return getNomadTemplateCaptureIntentForUpdate(ctx, tx, operationID)
}
