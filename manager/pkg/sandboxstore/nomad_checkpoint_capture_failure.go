package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Source-only memory operations reuse migration's writer retirement, node
// cleanup, finalization and allocation proof. Only evidence storage differs.
func lockNomadCheckpointCaptureFailure(ctx context.Context, tx pgx.Tx, life *SandboxLifecycleTxn) (*nomadCaptureFailure, error) {
	c, err := loadNomadCheckpoint(ctx, tx, life)
	if err != nil {
		return nil, err
	}
	e := c.Evidence
	if e.CaptureFailure != nil && e.CaptureFailure.Cleanup.WriterGrantID != c.SourceWriterGrantID {
		return nil, ErrNomadCheckpointConflict
	}
	if life.Cancelable || !e.CaptureAuthorized || e.CancelAuthorized || e.Publication != nil ||
		(life.Phase != SandboxLifecyclePhasePublishing && life.Phase != SandboxLifecyclePhaseAborted) {
		return nil, ErrNomadCheckpointConflict
	}
	binding, err := hex.DecodeString(e.Preflight.Source.BindingDigest)
	if err != nil {
		return nil, err
	}
	return &nomadCaptureFailure{checkpoint: c, life: life, capture: e.Preflight.Source, sourceSlot: c.SourceSlotID,
		sourceWriter: c.SourceWriterGrantID, sourceBinding: binding, request: e.CaptureFailure, cleanup: e.CaptureFailureCleanup, finalized: e.CaptureFailureFinalized}, nil
}

func (w *nomadCaptureFailure) saveCaptureFailureEvidence(ctx context.Context, tx pgx.Tx, key string, payload []byte, migrationSQL string, extra ...any) error {
	if w.checkpoint != nil {
		_, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=evidence || jsonb_build_object($2::text,$3::jsonb) WHERE operation_id=$1 AND evidence->$2::text IS DISTINCT FROM $3::jsonb`, w.life.ID, key, payload)
		return err
	}
	args := []any{w.life.ID, payload}
	args = append(args, extra...)
	_, err := tx.Exec(ctx, migrationSQL, args...)
	return err
}

func (e NomadCheckpointEvidence) validateCaptureFailure() error {
	if e.CaptureFailure == nil {
		if e.CaptureFailureCleanup != nil || e.CaptureFailureFinalized != nil || e.CaptureFailureStagingReleased != nil || e.CaptureFailureGC != nil || e.CaptureFailureGCAck != nil {
			return ErrNomadCheckpointConflict
		}
		return nil
	}
	request := e.CaptureFailure
	if _, err := request.Digest(); err != nil {
		return err
	}
	if !e.CaptureAuthorized || e.CancelAuthorized || e.Publication != nil || request.Capture.Request != e.Preflight.Source {
		return ErrNomadCheckpointConflict
	}
	if e.CaptureFailureCleanup != nil && e.CaptureFailureCleanup.ValidateFor(*request) != nil {
		return ErrNomadCheckpointConflict
	}
	if e.CaptureFailureFinalized != nil && (e.CaptureFailureCleanup == nil || e.CaptureFailureFinalized.ValidateFor(protocol.MigrationCaptureFailureFinalizeRequest{Request: *request, Proof: *e.CaptureFailureCleanup}) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.CaptureFailureStagingReleased != nil && (e.CaptureFailureFinalized == nil || e.Staging == nil || e.CaptureFailureStagingReleased.ValidateFor(*e.Staging) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.CaptureFailureGC != nil || e.CaptureFailureGCAck != nil {
		if e.CaptureFailureGC == nil || e.CaptureFailureGCAck == nil || e.CaptureFailureStagingReleased == nil || e.CaptureFailureGCAck.ValidateFor(*e.CaptureFailureGC) != nil {
			return ErrNomadCheckpointConflict
		}
		gc := e.CaptureFailureGC
		if gc.Target != request.Capture.Request.Target || gc.FinalizationDigest != e.CaptureFailureFinalized.RequestDigest || gc.CleanupProofDigest != e.CaptureFailureCleanup.Cleanup.ProofDigest {
			return ErrNomadCheckpointConflict
		}
	}
	return nil
}

// Finalization is the immutable authorization for this exact staging release.
// A lost release response can be replayed after allocation retirement.
func (s *PGSandboxStore) CommitNomadCheckpointFailedStagingRelease(ctx context.Context, request protocol.MigrationStagingRequest, receipt protocol.MigrationStagingReleased) error {
	if receipt.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	w, err := lockNomadCaptureFailure(ctx, tx, request.Source.OperationID)
	if err != nil {
		return err
	}
	if w.checkpoint == nil || w.finalized == nil || w.checkpoint.Evidence.Staging == nil || *w.checkpoint.Evidence.Staging != request {
		return ErrNomadCheckpointConflict
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := w.saveCaptureFailureEvidence(ctx, tx, "capture_failure_staging_released", payload, ""); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// NomadCheckpointFailed projects physically completed capture or restore
// failures for this exact owner snapshot. A paused recovery head does not mean
// the requested memory operation succeeded, and history cannot hide a new attempt.
func (s *PGSandboxStore) NomadCheckpointFailed(ctx context.Context, sandbox string, generation, epoch int64) (bool, error) {
	var failed bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM manager.sandbox_lifecycle_txns l
		JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=l.txn_id
		WHERE l.sandbox_id=$1 AND l.from_generation=$2 AND l.epoch=$3
			AND l.kind='pause' AND l.source='manual' AND l.phase='aborted'
			AND c.evidence ? 'capture_failure_gc_ack'
	) OR EXISTS (
		SELECT 1 FROM manager.sandbox_lifecycle_txns l
		JOIN manager.sandbox_runtime_checkpoint_restores r ON r.operation_id=l.txn_id
		WHERE l.sandbox_id=$1 AND l.to_generation=$2 AND l.epoch=$3
			AND l.kind='resume' AND l.source='manual' AND l.phase='aborted'
			AND r.evidence ? 'failure_gc_ack'
	)`, sandbox, generation, epoch).Scan(&failed)
	return failed, err
}
