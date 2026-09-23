package sandboxstore

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const checkpointRestoreFailureDue = `(EXISTS (SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=l.sandbox_id
    AND (s.desired_state<>'paused' OR s.deleted_at IS NOT NULL OR s.hard_expires_at<=clock_timestamp()))
    OR EXISTS (SELECT 1 FROM manager.runtime_slots t WHERE t.slot_id=r.evidence->'image'->'target'->>'slot_id'
        AND (t.claim_lease_expires_at<=clock_timestamp() OR t.state IN ('quiescing','orphaned','terminal'))))`

func (e NomadCheckpointRestoreEvidence) validateFailure() error {
	if e.Failure == nil {
		if e.FailureStopped != nil || e.FailureCleanup != nil || e.FailureCleaned != nil || e.FailureFinalized != nil || e.FailureGC != nil || e.FailureGCAck != nil {
			return ErrNomadCheckpointConflict
		}
		return nil
	}
	if e.Restore == nil || !reflect.DeepEqual(e.Failure.Restore, *e.Restore) || e.Adoption != nil {
		return ErrNomadCheckpointConflict
	}
	if _, err := e.Failure.Digest(); err != nil {
		return err
	}
	if e.FailureStopped != nil && e.FailureStopped.ValidateFor(*e.Failure) != nil {
		return ErrNomadCheckpointConflict
	}
	if e.FailureCleanup != nil {
		if e.FailureStopped == nil || !reflect.DeepEqual(e.FailureCleanup.Failure.Request, *e.Failure) || e.FailureCleanup.Failure.Proof != *e.FailureStopped {
			return ErrNomadCheckpointConflict
		}
		if _, err := e.FailureCleanup.Digest(); err != nil {
			return err
		}
	}
	if e.FailureCleaned != nil && (e.FailureCleanup == nil || e.FailureCleaned.ValidateFor(*e.FailureCleanup) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.FailureFinalized != nil && (e.FailureCleaned == nil || e.FailureFinalized.ValidateFor(protocol.MigrationFailureFinalizeRequest{Request: *e.FailureCleanup, Proof: *e.FailureCleaned}) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.FailureGC != nil || e.FailureGCAck != nil {
		if e.FailureGC == nil || e.FailureGCAck == nil || e.FailureFinalized == nil || e.FailureGCAck.ValidateFor(*e.FailureGC) != nil ||
			e.FailureGC.Target != e.Restore.Image.Target || e.FailureGC.FinalizationDigest != e.FailureFinalized.RequestDigest || e.FailureGC.CleanupProofDigest != e.FailureCleaned.Cleanup.ProofDigest {
			return ErrNomadCheckpointConflict
		}
	}
	return nil
}

// The caller already holds operation, owner and lifecycle locks. Recovery uses
// historical capture authority, so expiry cannot strand an executed target.
func loadNomadCheckpointRestoreFailure(ctx context.Context, tx pgx.Tx, life *SandboxLifecycleTxn) (*NomadCheckpointRestoreEvidence, error) {
	if life == nil || life.Kind != SandboxLifecycleKindResume || life.Source != SandboxLifecycleSourceManual || life.Cancelable ||
		(life.Phase != SandboxLifecyclePhasePreparing && life.Phase != SandboxLifecyclePhaseAborted) {
		return nil, ErrNomadCheckpointConflict
	}
	var payload, authorityJSON, captureJSON []byte
	if err := tx.QueryRow(ctx, `SELECT r.evidence,r.authority,c.evidence FROM manager.sandbox_runtime_checkpoint_restores r
        JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=r.checkpoint_id WHERE r.operation_id=$1 FOR UPDATE OF r`, life.ID).Scan(&payload, &authorityJSON, &captureJSON); err != nil {
		return nil, err
	}
	var e NomadCheckpointRestoreEvidence
	var a protocol.CheckpointRestoreAuthority
	var c NomadCheckpointEvidence
	if json.Unmarshal(payload, &e) != nil || json.Unmarshal(authorityJSON, &a) != nil || json.Unmarshal(captureJSON, &c) != nil ||
		c.validate() != nil || c.Finalized == nil || a.ValidateFor(*c.Publication, *c.Published) != nil || e.validate(a, c) != nil || e.Restore == nil || e.Adoption != nil ||
		a.Assignment.OperationID != life.ID || a.Assignment.Target.SandboxID != life.SandboxID || a.Assignment.Target.RuntimeGeneration != life.ToGeneration ||
		a.Assignment.PreviousGeneration() != life.FromGeneration ||
		a.LifecycleEpoch != life.Epoch || life.ToGeneration != life.FromGeneration+1 || e.Restore.Stage.InitialGeneration != life.ExpectedGenerationID {
		return nil, ErrNomadCheckpointConflict
	}
	return &e, nil
}

func authorizeNomadCheckpointRestoreFailure(ctx context.Context, tx pgx.Tx, record *SandboxRecord, life *SandboxLifecycleTxn) (*protocol.MigrationFailureRequest, error) {
	e, err := loadNomadCheckpointRestoreFailure(ctx, tx, life)
	if err != nil {
		return nil, err
	}
	if e.Failure != nil {
		return e.Failure, nil
	}
	if !executionFailurePending(life) || record.RuntimeGeneration != life.FromGeneration || record.LifecycleEpoch != life.Epoch || record.RuntimeID != "" || record.RuntimeNamespace != "" ||
		record.TeamID != e.Restore.Image.RuntimeAssignment().TeamID {
		return nil, ErrNomadCheckpointConflict
	}
	slot, err := lockRuntimeSlotByID(ctx, tx, e.Restore.Image.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if slot.ClaimOperationID != life.ID || slot.SandboxID != record.ID || nomadMigrationSlotTarget(slot) != e.Restore.Image.Target || slot.ResourceLease != e.Restore.Image.Resources ||
		slot.ResourceLeaseState != RuntimeResourceLeaseActive || slot.WriterGrantID != e.Restore.Stage.Identity.WriterGrantID {
		return nil, ErrNomadCheckpointConflict
	}
	var due, terminating bool
	if err := tx.QueryRow(ctx, `SELECT `+checkpointRestoreFailureDue+`,s.desired_state<>'paused' OR s.deleted_at IS NOT NULL OR COALESCE(s.hard_expires_at<=clock_timestamp(),false)
        FROM manager.sandbox_runtime_checkpoint_restores r JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id WHERE r.operation_id=$1`, life.ID).Scan(&due, &terminating); err != nil {
		return nil, err
	}
	if !due {
		return nil, nil
	}
	request := &protocol.MigrationFailureRequest{Restore: *e.Restore, Reason: protocol.MigrationFailureDestinationUnavailable}
	if terminating {
		request.Reason = protocol.MigrationFailureTermination
	}
	if _, err := request.Digest(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state=CASE WHEN state IN ('claiming','starting','active') THEN 'quiescing' ELSE state END,
        carrier_retired=true,quiescing_at=COALESCE(quiescing_at,clock_timestamp()),revision=revision+1,updated_at=clock_timestamp() WHERE slot_id=$1`, slot.ID); err != nil {
		return nil, err
	}
	if err := saveNomadExecutionFailureEvidence(ctx, tx, life, "failure", payload, ""); err != nil {
		return nil, err
	}
	return request, nil
}

func lockNomadCheckpointRestoreFailureCleanup(ctx context.Context, tx pgx.Tx, life *SandboxLifecycleTxn) (*protocol.MigrationFailureCleanupRequest, *SandboxLifecycleTxn, bool, error) {
	e, err := loadNomadCheckpointRestoreFailure(ctx, tx, life)
	if err != nil {
		return nil, nil, false, err
	}
	if e.Failure == nil || e.FailureStopped == nil {
		return nil, nil, false, ErrNomadCheckpointConflict
	}
	if e.FailureCleanup != nil {
		return e.FailureCleanup, life, true, nil
	}
	return &protocol.MigrationFailureCleanupRequest{Failure: protocol.MigrationFailureStopReceipt{Request: *e.Failure, Proof: *e.FailureStopped}}, life, false, nil
}
