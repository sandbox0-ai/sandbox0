package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func (s *PGSandboxStore) GetNomadMigrationCaptureFailureForSlot(ctx context.Context, slot string) (bool, *protocol.MigrationCaptureFailureReceipt, error) {
	if protocol.ValidateSlotID(slot) != nil {
		return false, nil, ErrRuntimeSlotInvalid
	}
	var id string
	var request, cleanup, final []byte
	err := s.pool.QueryRow(ctx, `SELECT operation_id,capture_failure_request,capture_failure_cleanup_receipt,capture_failure_finalization_receipt
        FROM manager.sandbox_runtime_migrations WHERE source_slot_id=$1 AND capture_failure_request IS NOT NULL
        UNION ALL SELECT operation_id,evidence->'capture_failure',evidence->'capture_failure_cleanup',
            CASE WHEN evidence ? 'capture_failure_staging_released' THEN evidence->'capture_failure_finalized' ELSE NULL END
        FROM manager.sandbox_runtime_checkpoints WHERE source_slot_id=$1 AND evidence ? 'capture_failure'`, slot).Scan(&id, &request, &cleanup, &final)
	if err == pgx.ErrNoRows {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if len(final) == 0 {
		return true, nil, nil
	}
	var receipt protocol.MigrationCaptureFailureReceipt
	if json.Unmarshal(request, &receipt.Request.Request) != nil || json.Unmarshal(cleanup, &receipt.Request.Proof) != nil ||
		json.Unmarshal(final, &receipt.Proof) != nil || receipt.Validate() != nil || receipt.Request.Request.Cleanup.SlotID != slot ||
		receipt.Request.Request.Capture.Request.OperationID != id {
		return true, nil, ErrNomadSandboxMigrationConflict
	}
	return true, &receipt, nil
}

// Completion cannot make the failed memory image resumable. It consumes the
// reserved generation, keeps the last committed filesystem head and releases
// the source lease only alongside exact physical and allocation evidence.
func (s *PGSandboxStore) CompleteNomadSandboxMigrationCaptureFailure(ctx context.Context, id string, gc protocol.MigrationSourceGCRequest, ack protocol.MigrationSourceGCAcknowledgement) (*RuntimeSlot, error) {
	if ack.ValidateFor(gc) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	w, err := lockNomadCaptureFailure(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if w.finalized == nil || gc.Target != w.capture.Target || gc.FinalizationDigest != w.finalized.RequestDigest || gc.CleanupProofDigest != w.cleanup.Cleanup.ProofDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	source, err := lockRuntimeSlotByID(ctx, tx, w.sourceSlot)
	if err != nil {
		return nil, err
	}
	c := w.request.Cleanup
	if source.SandboxID != w.life.SandboxID || source.AllocationID != c.AllocationID || source.AllocationNamespace != w.life.FromRuntimeNamespace ||
		source.ClusterID != c.ClusterID || source.NodeID != c.NodeID || source.NodeUID != c.NodeUID || source.NodeBootID != c.NodeBootID ||
		source.NetNSIdentity != c.NetNSIdentity || source.RunscContainerID != c.RunscContainerID || source.WriterGrantID != c.WriterGrantID ||
		source.ResourceLease != c.Resources || hex.EncodeToString(source.ResourceLeaseDigest) != c.ResourceLeaseDigest ||
		len(source.OrphanObservationDigest) != sha256.Size || hex.EncodeToString(source.OrphanObservationDigest) != gc.AllocationAbsenceDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if w.checkpoint == nil {
		target, err := lockRuntimeSlotByID(ctx, tx, w.targetSlot)
		if err != nil {
			return nil, err
		}
		if target.State != RuntimeSlotStateTerminal || len(target.TerminalProofDigest) != sha256.Size || target.ClaimID != "" || target.ClaimOperationID != "" ||
			target.WriterGrantID != "" || !target.ResourceLease.IsZero() || target.SandboxID != "" {
			return nil, ErrNomadSandboxMigrationConflict
		}
		lease, _, state, err := loadMigrationResourceLease(ctx, tx, w.targetLease)
		if err != nil {
			return nil, err
		}
		if state != RuntimeResourceLeaseReleased || lease.SlotID != target.ID || lease.OperationID != id {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else if w.checkpoint.Evidence.CaptureFailureStagingReleased == nil {
		return nil, ErrNomadCheckpointConflict
	}
	var completed *time.Time
	var priorGC, priorAck []byte
	if w.checkpoint != nil {
		e := w.checkpoint.Evidence
		if e.CaptureFailureGCAck != nil {
			completed = &w.life.AbortedAt
			priorGC, _ = json.Marshal(e.CaptureFailureGC)
			priorAck, _ = json.Marshal(e.CaptureFailureGCAck)
		}
	} else {
		if err := tx.QueryRow(ctx, `SELECT capture_failure_completed_at,capture_failure_allocation_gc_request,capture_failure_allocation_gc_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).Scan(&completed, &priorGC, &priorAck); err != nil {
			return nil, err
		}
	}
	if completed != nil {
		var old protocol.MigrationSourceGCRequest
		var oldAck protocol.MigrationSourceGCAcknowledgement
		if json.Unmarshal(priorGC, &old) != nil || json.Unmarshal(priorAck, &oldAck) != nil || old != gc || oldAck != ack ||
			w.life.Phase != SandboxLifecyclePhaseAborted || source.State != RuntimeSlotStateTerminal || source.TerminalReason != "migration_capture_failed" || source.ResourceLeaseState != RuntimeResourceLeaseReleased {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return source, tx.Commit(ctx)
	}
	if w.life.Phase != SandboxLifecyclePhasePublishing || source.State != RuntimeSlotStateOrphaned || source.ResourceLeaseState != RuntimeResourceLeaseActive {
		return nil, ErrNomadSandboxMigrationConflict
	}
	g, err := w.lockSourceGrant(ctx, tx)
	if err != nil {
		return nil, err
	}
	if g.State != RootFSWriterGrantStateRetired || g.RetireKind != RootFSWriterRetireKindCrashAbandon || g.RetireOperationID != c.WriterOperationID || hex.EncodeToString(g.RetireProofDigest) != w.cleanup.Cleanup.ProofDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	payload, _ := json.Marshal(struct {
		Domain string
		GC     protocol.MigrationSourceGCRequest
		Ack    protocol.MigrationSourceGCAcknowledgement
	}{"sandbox0-failed-capture-completion-v1", gc, ack})
	terminal := sha256.Sum256(payload)
	normalized, err := normalizeFinalizeRuntimeSlotRequest(&FinalizeRuntimeSlotRequest{SlotID: source.ID, OperationID: source.ClaimOperationID, ClaimID: source.ClaimID,
		Reason: "migration_capture_failed", ProofDigest: terminal[:], ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: source.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	if err != nil {
		return nil, err
	}
	if _, err := finalizeRuntimeSlotTx(ctx, tx, source, normalized); err != nil {
		return nil, err
	}
	gcJSON, _ := json.Marshal(gc)
	ackJSON, _ := json.Marshal(ack)
	if w.checkpoint != nil {
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=evidence || jsonb_build_object('capture_failure_gc',$2::jsonb,'capture_failure_gc_ack',$3::jsonb) WHERE operation_id=$1`, id, gcJSON, ackJSON); err != nil {
			return nil, err
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_failure_allocation_gc_request=$2,capture_failure_allocation_gc_receipt=$3,capture_failure_completed_at=clock_timestamp() WHERE operation_id=$1`, id, gcJSON, ackJSON); err != nil {
			return nil, err
		}
	}
	if err := completeFailedMigrationLifecycle(ctx, tx, w.life, source.AuthorityObservedAt, "migration capture failed; execution cannot be replayed"); err != nil {
		return nil, err
	}
	source, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, source.ID))
	if err != nil {
		return nil, err
	}
	return source, tx.Commit(ctx)
}

// Both failed capture and failed restore leave only a filesystem recovery
// point. Keep their public lifecycle transition and generation rule identical.
func completeFailedMigrationLifecycle(ctx context.Context, tx pgx.Tx, life *SandboxLifecycleTxn, now time.Time, reason string) error {
	record, err := lockNomadSandboxClaimRecord(ctx, tx, life.SandboxID)
	if err != nil {
		return err
	}
	if record.RuntimeGeneration != life.FromGeneration || record.RuntimeID != life.FromRuntimeID || record.RuntimeNamespace != life.FromRuntimeNamespace || !record.DeletedAt.IsZero() {
		return ErrNomadSandboxMigrationConflict
	}
	generation := life.ToGeneration
	if life.Kind == SandboxLifecycleKindPause {
		generation = life.FromGeneration
	}
	if record.DesiredState == SandboxDesiredStateActive && (record.HardExpiresAt.IsZero() || record.HardExpiresAt.After(now)) {
		if err := (sandboxStoreTx{tx: tx}).MarkRuntimePaused(ctx, life.SandboxID, generation, now); err != nil {
			return err
		}
	} else {
		if record.DesiredState != SandboxDesiredStateTerminating && record.DesiredState != SandboxDesiredStateActive {
			return ErrNomadSandboxMigrationConflict
		}
		if err := (sandboxStoreTx{tx: tx}).MarkRuntimeTerminating(ctx, life.SandboxID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.sandboxes SET runtime_id='',runtime_namespace='',runtime_generation=$2 WHERE sandbox_id=$1`, life.SandboxID, generation); err != nil {
			return err
		}
	}
	return (sandboxStoreTx{tx: tx}).AbortLifecycleTxn(ctx, life.ID, reason)
}
