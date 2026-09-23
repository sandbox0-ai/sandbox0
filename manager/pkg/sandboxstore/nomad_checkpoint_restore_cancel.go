package sandboxstore

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// A pre-execution restore owns a speculative node image even if no writer was
// issued. Quiescing first stops renewed claims; the node tombstone later makes
// generic physical cleanup safe against a delayed image-preparation command.
func beginNomadCheckpointRestoreCancellation(ctx context.Context, tx pgx.Tx, record *SandboxRecord, life *SandboxLifecycleTxn, reason string) (bool, error) {
	if life.Kind != SandboxLifecycleKindResume || life.Source != SandboxLifecycleSourceManual || life.Phase != SandboxLifecyclePhasePreparing {
		return false, nil
	}
	var payload []byte
	err := tx.QueryRow(ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1 FOR UPDATE`, life.ID).Scan(&payload)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var evidence NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &evidence) != nil {
		return true, ErrNomadCheckpointConflict
	}
	if evidence.Restore != nil {
		return true, ErrNomadCheckpointConflict
	}
	if evidence.Image == nil {
		// No image authority was ever published, so no node can start or revive
		// a download. Close this admitted operation even after owner expiry.
		_, head, _, err := lockNomadSandboxResumeHead(ctx, tx, record.ID)
		if err != nil {
			return true, err
		}
		if life.ExpectedGenerationID != head || life.Epoch != record.LifecycleEpoch ||
			life.FromGeneration != record.RuntimeGeneration || life.ToGeneration != life.FromGeneration+1 ||
			record.RuntimeID != "" || record.RuntimeNamespace != "" ||
			(record.DesiredState != SandboxDesiredStatePaused && record.DesiredState != SandboxDesiredStateTerminating) {
			return true, ErrNomadCheckpointConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',error=$2,
			aborted_at=clock_timestamp(),updated_at=clock_timestamp() WHERE txn_id=$1 AND phase='preparing'`, life.ID, reason); err != nil {
			return true, err
		}
		return true, quiesceAbortedNomadResumeSlots(ctx, tx, record.ID, life.ID)
	}
	if record.ID != life.SandboxID || record.RuntimeGeneration != life.FromGeneration || record.LifecycleEpoch != life.Epoch ||
		record.RuntimeID != "" || record.RuntimeNamespace != "" {
		return true, ErrNomadCheckpointConflict
	}
	if evidence.CancelRequest == nil {
		request := protocol.CheckpointImageCancelRequest{Image: *evidence.Image}
		if _, err := request.Digest(); err != nil {
			return true, err
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			return true, err
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores
			SET evidence=evidence || jsonb_build_object('cancel_request',$2::jsonb) WHERE operation_id=$1`, life.ID, encoded); err != nil {
			return true, err
		}
	}
	target, err := lockRuntimeSlotByID(ctx, tx, evidence.Image.Target.SlotID)
	if err != nil {
		return true, err
	}
	if target.ClaimOperationID != life.ID || target.SandboxID != record.ID || nomadMigrationSlotTarget(target) != evidence.Image.Target ||
		target.ResourceLease != evidence.Image.Resources {
		return true, ErrNomadCheckpointConflict
	}
	if target.State != RuntimeSlotStateTerminal {
		if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state=CASE WHEN state IN ('claiming','starting','active') THEN 'quiescing' ELSE state END,
			carrier_retired=true,quiescing_at=COALESCE(quiescing_at,clock_timestamp()),revision=revision+1,updated_at=clock_timestamp()
			WHERE slot_id=$1`, target.ID); err != nil {
			return true, err
		}
	}
	if evidence.CancelProof == nil || target.State != RuntimeSlotStateTerminal || target.ResourceLeaseState != RuntimeResourceLeaseReleased {
		return true, nil
	}
	if target.TerminalReason != "prelaunch_abort" && target.TerminalReason != "reconciled_orphan" && target.TerminalReason != "allocation_missing" {
		return true, ErrNomadCheckpointConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',error=$2,aborted_at=clock_timestamp(),updated_at=clock_timestamp()
		WHERE txn_id=$1 AND phase='preparing'`, life.ID, reason); err != nil {
		return true, err
	}
	return true, nil
}

type NomadCheckpointRestoreCancellationWork struct {
	OperationID string
	SandboxID   string
}

// The keyset scan is bounded and includes both requested and newly due work.
func (s *PGSandboxStore) ListNomadCheckpointRestoreCancellations(ctx context.Context, after string, limit int) ([]NomadCheckpointRestoreCancellationWork, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 512 {
		return nil, ErrNomadCheckpointConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT r.operation_id,l.sandbox_id FROM manager.sandbox_runtime_checkpoint_restores r
		JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
		JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id
		WHERE l.phase='preparing' AND NOT r.evidence ? 'restore'
		AND (r.evidence ? 'cancel_request' OR s.desired_state<>'paused' OR s.deleted_at IS NOT NULL
			OR s.hard_expires_at<=clock_timestamp() OR EXISTS (SELECT 1 FROM manager.runtime_slots t
				WHERE t.claim_operation_id=r.operation_id AND (t.claim_lease_expires_at<=clock_timestamp()
					OR t.state IN ('quiescing','orphaned','terminal'))))
		AND r.operation_id COLLATE "C">$1 COLLATE "C" ORDER BY r.operation_id COLLATE "C" LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []NomadCheckpointRestoreCancellationWork
	for rows.Next() {
		var item NomadCheckpointRestoreCancellationWork
		if err := rows.Scan(&item.OperationID, &item.SandboxID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *PGSandboxStore) GetNomadCheckpointRestoreCancellation(ctx context.Context, operation string) (*protocol.CheckpointImageCancelRequest, *protocol.CheckpointImageCancelProof, error) {
	var payload []byte
	if err := s.pool.QueryRow(ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, operation).Scan(&payload); err != nil {
		return nil, nil, err
	}
	var evidence NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &evidence) != nil || evidence.Restore != nil {
		return nil, nil, ErrNomadCheckpointConflict
	}
	if evidence.Image == nil && evidence.CancelRequest == nil {
		return nil, nil, nil
	}
	if evidence.Image == nil || evidence.CancelRequest == nil ||
		!reflect.DeepEqual(evidence.CancelRequest.Image, *evidence.Image) {
		return nil, nil, ErrNomadCheckpointConflict
	}
	if _, err := evidence.CancelRequest.Digest(); err != nil {
		return nil, nil, err
	}
	if evidence.CancelProof != nil && evidence.CancelProof.ValidateFor(*evidence.CancelRequest) != nil {
		return nil, nil, ErrNomadCheckpointConflict
	}
	return evidence.CancelRequest, evidence.CancelProof, nil
}

// A late node reply is accepted only for the same durable cancellation and
// cannot authorize restore, replace the image or waive physical cleanup.
func (s *PGSandboxStore) CommitNomadCheckpointRestoreCancellation(ctx context.Context, operation string, request protocol.CheckpointImageCancelRequest, proof protocol.CheckpointImageCancelProof) error {
	if proof.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var sandboxID string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, operation).Scan(&sandboxID); err != nil {
		return err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return err
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, operation))
	if err != nil {
		return err
	}
	if life.Phase != SandboxLifecyclePhasePreparing || life.SandboxID != record.ID || life.FromGeneration != record.RuntimeGeneration {
		return ErrNomadCheckpointConflict
	}
	var payload []byte
	if err := tx.QueryRow(ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1 FOR UPDATE`, operation).Scan(&payload); err != nil {
		return err
	}
	var evidence NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &evidence) != nil || evidence.Restore != nil || !reflect.DeepEqual(evidence.CancelRequest, &request) ||
		(evidence.CancelProof != nil && *evidence.CancelProof != proof) {
		return ErrNomadCheckpointConflict
	}
	if evidence.CancelProof == nil {
		encoded, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=evidence || jsonb_build_object('cancel_proof',$2::jsonb)
			WHERE operation_id=$1`, operation, encoded); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
