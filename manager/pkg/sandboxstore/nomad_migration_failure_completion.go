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

// GetNomadMigrationFailureForSlot keeps failed targets out of generic writer
// cleanup even while finalization is pending. Historical receipts are readable
// without renewing a grant or depending on the sandbox's current generation.
func (s *PGSandboxStore) GetNomadMigrationFailureForSlot(ctx context.Context, slot string) (bool, *protocol.MigrationFailureFinalizationReceipt, error) {
	if protocol.ValidateSlotID(slot) != nil {
		return false, nil, ErrRuntimeSlotInvalid
	}
	var operation string
	var command, cleanup, finalized []byte
	err := s.pool.QueryRow(ctx, `SELECT operation_id,failure_cleanup_request,failure_cleanup_receipt,failure_finalization_receipt
        FROM manager.sandbox_runtime_migrations WHERE target_slot_id=$1 AND failure_request IS NOT NULL
        UNION ALL SELECT r.operation_id,r.evidence->'failure_cleanup',r.evidence->'failure_cleaned',r.evidence->'failure_finalized'
        FROM manager.sandbox_runtime_checkpoint_restores r JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
        WHERE r.evidence->'image'->'target'->>'slot_id'=$1 AND r.evidence ? 'restore' AND l.phase<>'committed'`, slot).Scan(&operation, &command, &cleanup, &finalized)
	if err == pgx.ErrNoRows {
		// A speculative checkpoint image can still arrive after the slot's
		// ordinary cleanup starts. The node cancellation proof is the gate for
		// generic writer, image, cgroup and allocation reconciliation.
		var pending bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_restores r
			JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
			WHERE r.evidence->'image'->'target'->>'slot_id'=$1 AND r.evidence ? 'image'
				AND NOT r.evidence ? 'restore' AND NOT r.evidence ? 'cancel_proof' AND l.phase<>'committed')`, slot).Scan(&pending); err != nil {
			return false, nil, err
		}
		return pending, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if len(finalized) == 0 {
		return true, nil, nil
	}
	var receipt protocol.MigrationFailureFinalizationReceipt
	if json.Unmarshal(command, &receipt.Request.Request) != nil || json.Unmarshal(cleanup, &receipt.Request.Proof) != nil ||
		json.Unmarshal(finalized, &receipt.Proof) != nil || receipt.Validate() != nil ||
		receipt.Request.Request.Cleanup.SlotID != slot || receipt.Request.Request.Failure.Request.Restore.Image.OperationID() != operation {
		return true, nil, ErrNomadSandboxMigrationConflict
	}
	return true, &receipt, nil
}

// CompleteNomadSandboxMigrationFailure joins both physical finalizations,
// target allocation absence and node acknowledgement in one terminal region
// transaction. A failed, possibly executed generation is consumed permanently;
// the last committed filesystem cut remains the only resumable state.
func (s *PGSandboxStore) CompleteNomadSandboxMigrationFailure(ctx context.Context, operation string, gc protocol.MigrationSourceGCRequest, ack protocol.MigrationSourceGCAcknowledgement) (*RuntimeSlot, error) {
	if ack.ValidateFor(gc) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	cleanup, life, authorized, err := lockNomadMigrationFailureCleanup(ctx, tx, operation)
	if err != nil {
		return nil, err
	}
	if !authorized {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if life.Kind == SandboxLifecycleKindResume {
		terminal, err := completeNomadCheckpointRestoreFailure(ctx, tx, life, cleanup, gc, ack)
		if err != nil {
			return nil, err
		}
		return terminal, tx.Commit(ctx)
	}
	var sourceID string
	var finalized, cleanupProof, sourceCommand, sourceProof, oldGC, oldAck []byte
	var completed *time.Time
	if err := tx.QueryRow(ctx, `SELECT source_slot_id,failure_cleanup_receipt,failure_finalization_receipt,
        source_finalization_request,source_finalization_receipt,failure_allocation_gc_request,failure_allocation_gc_receipt,failure_completed_at
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, operation).Scan(&sourceID, &cleanupProof, &finalized, &sourceCommand, &sourceProof, &oldGC, &oldAck, &completed); err != nil {
		return nil, err
	}
	var receipt protocol.MigrationFailureFinalizationReceipt
	receipt.Request.Request = *cleanup
	if json.Unmarshal(cleanupProof, &receipt.Request.Proof) != nil || json.Unmarshal(finalized, &receipt.Proof) != nil || receipt.Validate() != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	c := cleanup.Cleanup
	restore := cleanup.Failure.Request.Restore
	if gc.Target != restore.Image.Target || gc.FinalizationDigest != receipt.Proof.RequestDigest || gc.CleanupProofDigest != receipt.Request.Proof.Cleanup.ProofDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	source, err := lockRuntimeSlotByID(ctx, tx, sourceID)
	if err != nil {
		return nil, err
	}
	target, err := lockRuntimeSlotByID(ctx, tx, c.SlotID)
	if err != nil {
		return nil, err
	}
	if target.SandboxID != life.SandboxID || target.ClaimOperationID != operation || target.AllocationID != c.AllocationID ||
		target.ClusterID != c.ClusterID || target.NodeID != c.NodeID || target.NodeUID != c.NodeUID || target.NodeBootID != c.NodeBootID ||
		target.NetNSIdentity != c.NetNSIdentity || target.WriterGrantID != c.WriterGrantID || target.ResourceLease != c.Resources ||
		hex.EncodeToString(target.ResourceLeaseDigest) != c.ResourceLeaseDigest || len(target.OrphanObservationDigest) != sha256.Size ||
		hex.EncodeToString(target.OrphanObservationDigest) != gc.AllocationAbsenceDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if completed != nil {
		var previous protocol.MigrationSourceGCRequest
		var previousAck protocol.MigrationSourceGCAcknowledgement
		if json.Unmarshal(oldGC, &previous) != nil || json.Unmarshal(oldAck, &previousAck) != nil || previous != gc || previousAck != ack ||
			life.Phase != SandboxLifecyclePhaseAborted || target.State != RuntimeSlotStateTerminal || target.TerminalReason != "migration_failed" || target.ResourceLeaseState != RuntimeResourceLeaseReleased {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return target, tx.Commit(ctx)
	}
	if life.Phase != SandboxLifecyclePhaseCommitting || target.State != RuntimeSlotStateOrphaned || target.ResourceLeaseState != RuntimeResourceLeaseActive ||
		source.State != RuntimeSlotStateTerminal || source.TerminalReason != "migration_source" || source.ResourceLeaseState != RuntimeResourceLeaseReleased ||
		len(source.TerminalProofDigest) != sha256.Size || len(source.OrphanObservationDigest) != sha256.Size {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var sourceReceipt protocol.MigrationSourceFinalizationReceipt
	if json.Unmarshal(sourceCommand, &sourceReceipt.Request) != nil || json.Unmarshal(sourceProof, &sourceReceipt.Proof) != nil || sourceReceipt.Validate() != nil ||
		sourceReceipt.Request.Failure == nil || sourceReceipt.Request.Failure.Proof != cleanup.Failure.Proof || sourceReceipt.Request.Cleanup.SlotID != source.ID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if _, err := lockNomadMigrationFailureFinalization(ctx, tx, operation); err != nil {
		return nil, err
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, c.WriterGrantID)
	if err != nil {
		return nil, err
	}
	if err := lockRootFSWriterCrashFallbackGeneration(ctx, tx, grant, restore.Stage.InitialGeneration, false); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(struct {
		Domain  string
		Request protocol.MigrationSourceGCRequest
		Ack     protocol.MigrationSourceGCAcknowledgement
	}{"sandbox0-failed-migration-completion-v1", gc, ack})
	if err != nil {
		return nil, err
	}
	terminal := sha256.Sum256(payload)
	normalized, err := normalizeFinalizeRuntimeSlotRequest(&FinalizeRuntimeSlotRequest{SlotID: target.ID, OperationID: operation, ClaimID: target.ClaimID, Reason: "migration_failed",
		ProofDigest: terminal[:], ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	if err != nil {
		return nil, err
	}
	if _, err := finalizeRuntimeSlotTx(ctx, tx, target, normalized); err != nil {
		return nil, err
	}
	gcJSON, _ := json.Marshal(gc)
	ackJSON, _ := json.Marshal(ack)
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET failure_allocation_gc_request=$2,failure_allocation_gc_receipt=$3,failure_completed_at=clock_timestamp() WHERE operation_id=$1`, operation, gcJSON, ackJSON); err != nil {
		return nil, err
	}
	if err := completeFailedMigrationLifecycle(ctx, tx, life, target.AuthorityObservedAt, "migration destination failed; execution cannot be replayed"); err != nil {
		return nil, err
	}
	target, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, target.ID))
	if err != nil {
		return nil, err
	}
	return target, tx.Commit(ctx)
}
