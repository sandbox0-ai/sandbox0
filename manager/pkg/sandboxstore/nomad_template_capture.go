package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

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
)

// NomadTemplateCaptureRequest identifies one deterministic internal snapshot
// of an exact live writer.
type NomadTemplateCaptureRequest struct {
	OperationID     string
	SourceSandboxID string
	TeamID          string
	SnapshotID      string
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
		FromPodNamespace: source.CurrentPodNamespace, FromPodName: source.CurrentPodName,
		TargetSandboxID: targetFilesystemID, TargetGenerationID: targetGenerationID,
		TargetRecordDigest: requestDigest, ExpectedHeadLayerID: writer.generation.ID,
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
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())
	`, normalized.OperationID, normalized.SnapshotID, normalized.TeamID, source.ID,
		writer.filesystem.ID, writer.grant.ID, writer.grant.WriterEpoch, writer.generation.ID,
		targetFilesystemID, targetGenerationID, requestDigest,
		writer.grant.BindingVersion, writer.grant.BindingDigest, nomadTemplateCaptureStatePending); err != nil {
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
	}
	candidate := nomadTemplateCaptureCandidate(source, writer, intent)
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad template capture request: %w", err)
	}
	return candidate, nil
}

func normalizeNomadTemplateCaptureRequest(
	request *NomadTemplateCaptureRequest,
) (*NomadTemplateCaptureRequest, []byte, string, string, error) {
	if request == nil {
		return nil, nil, "", "", fmt.Errorf("Nomad template capture request is required")
	}
	normalized := *request
	normalized.OperationID = strings.TrimSpace(request.OperationID)
	normalized.SourceSandboxID = strings.TrimSpace(request.SourceSandboxID)
	normalized.TeamID = strings.TrimSpace(request.TeamID)
	normalized.SnapshotID = strings.TrimSpace(request.SnapshotID)
	for name, value := range map[string]string{
		"operation_id": normalized.OperationID, "source_sandbox_id": normalized.SourceSandboxID,
		"team_id": normalized.TeamID, "snapshot_id": normalized.SnapshotID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return nil, nil, "", "", fmt.Errorf("%s must be canonical and at most 512 bytes", name)
		}
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
	err := tx.QueryRow(ctx, `
		SELECT operation_id, snapshot_id, team_id, source_sandbox_id,
			source_filesystem_id, source_grant_id, source_writer_epoch,
			source_generation_id, target_filesystem_id, checkpoint_generation_id,
			request_digest, binding_version, binding_digest, state,
			checkpoint_sequence, checkpoint_descriptor_digest, checkpoint_proof_digest
		FROM manager.rootfs_running_template_captures
		WHERE operation_id = $1
		FOR UPDATE
	`, operationID).Scan(
		&intent.OperationID, &intent.SnapshotID, &intent.TeamID, &intent.SourceSandboxID,
		&intent.SourceFilesystemID, &intent.SourceGrantID, &intent.SourceWriterEpoch,
		&intent.SourceGenerationID, &intent.TargetFilesystemID, &intent.CheckpointGeneration,
		&intent.RequestDigest, &intent.BindingVersion, &intent.BindingDigest, &intent.State,
		&intent.CheckpointSequence, &intent.DescriptorDigest, &intent.ProofDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load Nomad template capture intent: %w", err)
	}
	return &intent, nil
}

func nomadTemplateCaptureIntentMatches(
	intent *nomadTemplateCaptureIntent,
	request *NomadTemplateCaptureRequest,
	requestDigest []byte,
	targetFilesystemID, targetGenerationID string,
) bool {
	return intent != nil && request != nil && intent.OperationID == request.OperationID &&
		intent.SnapshotID == request.SnapshotID && intent.TeamID == request.TeamID &&
		intent.SourceSandboxID == request.SourceSandboxID &&
		intent.TargetFilesystemID == targetFilesystemID &&
		intent.CheckpointGeneration == targetGenerationID && bytes.Equal(intent.RequestDigest, requestDigest)
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
			s.head_layer_id, s.head_generation_id,
			CASE WHEN s.head_generation_id IS NULL THEN 'legacy-layer' ELSE 'block-cow-v1' END,
			g.base_artifact_digest, g.format_generation, g.source_oci_digest,
			s.name, s.description, s.created_at, s.expires_at
		FROM manager.rootfs_snapshots s
		JOIN manager.rootfs_generations g
		  ON g.generation_id = s.head_generation_id
		 AND g.filesystem_id = s.filesystem_id
		WHERE s.snapshot_id = $1 AND s.team_id = $2
	`, intent.SnapshotID, intent.TeamID))
	if err != nil {
		return nil, fmt.Errorf("load published Nomad template snapshot: %w", err)
	}
	if snapshot.FilesystemID != intent.TargetFilesystemID ||
		snapshot.HeadGenerationID != intent.CheckpointGeneration ||
		snapshot.SourceSandboxID != intent.SourceSandboxID {
		return nil, fmt.Errorf("%w: published snapshot identity changed", ErrNomadTemplateCaptureConflict)
	}
	return &NomadTemplateCaptureCandidate{
		OperationID: intent.OperationID, SnapshotID: intent.SnapshotID,
		TargetFilesystemID: intent.TargetFilesystemID, TargetGenerationID: intent.CheckpointGeneration,
		Completed: true, Snapshot: snapshot, Source: source,
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
		lifecycle.ToPodNamespace != "" || lifecycle.ToPodName != "" ||
		lifecycle.TargetSandboxID != intent.TargetFilesystemID ||
		lifecycle.TargetGenerationID != intent.CheckpointGeneration ||
		lifecycle.ExpectedHeadLayerID != intent.SourceGenerationID ||
		!bytes.Equal(lifecycle.TargetRecordDigest, intent.RequestDigest) {
		return false
	}
	if committed {
		return lifecycle.Phase == SandboxLifecyclePhaseCommitted &&
			lifecycle.PreparedHeadLayerID == intent.CheckpointGeneration
	}
	return lifecycle.Phase == SandboxLifecyclePhasePublishing && lifecycle.PreparedHeadLayerID == "" &&
		lifecycle.FromGeneration == source.RuntimeGeneration &&
		lifecycle.FromPodNamespace == source.CurrentPodNamespace &&
		lifecycle.FromPodName == source.CurrentPodName
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
