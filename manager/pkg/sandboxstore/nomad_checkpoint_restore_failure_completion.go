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

// A failed independent restore has no live source to reclaim. It consumes its
// attempted generation, retains the archived image and disk head, and permits
// only a later explicit resume to create another execution operation.
func completeNomadCheckpointRestoreFailure(ctx context.Context, tx pgx.Tx, life *SandboxLifecycleTxn, cleanup *protocol.MigrationFailureCleanupRequest, gc protocol.MigrationSourceGCRequest, ack protocol.MigrationSourceGCAcknowledgement) (*RuntimeSlot, error) {
	e, err := loadNomadCheckpointRestoreFailure(ctx, tx, life)
	if err != nil {
		return nil, err
	}
	if e.FailureFinalized == nil || gc.Target != e.Restore.Image.Target || gc.FinalizationDigest != e.FailureFinalized.RequestDigest || gc.CleanupProofDigest != e.FailureCleaned.Cleanup.ProofDigest {
		return nil, ErrNomadCheckpointConflict
	}
	c := cleanup.Cleanup
	target, err := lockRuntimeSlotByID(ctx, tx, c.SlotID)
	if err != nil {
		return nil, err
	}
	if target.SandboxID != life.SandboxID || target.ClaimOperationID != life.ID || nomadMigrationSlotTarget(target) != gc.Target ||
		target.NetNSIdentity != c.NetNSIdentity || target.WriterGrantID != c.WriterGrantID || target.ResourceLease != c.Resources ||
		hex.EncodeToString(target.ResourceLeaseDigest) != c.ResourceLeaseDigest || len(target.OrphanObservationDigest) != sha256.Size ||
		hex.EncodeToString(target.OrphanObservationDigest) != gc.AllocationAbsenceDigest {
		return nil, ErrNomadCheckpointConflict
	}
	if e.FailureGCAck != nil {
		if *e.FailureGC != gc || *e.FailureGCAck != ack || life.Phase != SandboxLifecyclePhaseAborted || target.State != RuntimeSlotStateTerminal || target.TerminalReason != "migration_failed" || target.ResourceLeaseState != RuntimeResourceLeaseReleased {
			return nil, ErrNomadCheckpointConflict
		}
		return target, nil
	}
	if !executionFailurePending(life) || target.State != RuntimeSlotStateOrphaned || target.ResourceLeaseState != RuntimeResourceLeaseActive {
		return nil, ErrNomadCheckpointConflict
	}
	if _, err := lockNomadMigrationFailureFinalization(ctx, tx, life.ID); err != nil {
		return nil, err
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, c.WriterGrantID)
	if err != nil {
		return nil, err
	}
	if err := lockRootFSWriterCrashFallbackGeneration(ctx, tx, grant, e.Restore.Stage.InitialGeneration, false); err != nil {
		return nil, err
	}
	owner, err := lockNomadSandboxClaimRecord(ctx, tx, life.SandboxID)
	if err != nil {
		return nil, err
	}
	if owner.RuntimeID != "" || owner.RuntimeNamespace != "" || owner.RuntimeGeneration != life.FromGeneration || owner.LifecycleEpoch != life.Epoch || !owner.DeletedAt.IsZero() ||
		(owner.DesiredState != SandboxDesiredStatePaused && owner.DesiredState != SandboxDesiredStateTerminating) {
		return nil, ErrNomadCheckpointConflict
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	desired := owner.DesiredState
	if !owner.HardExpiresAt.IsZero() && !owner.HardExpiresAt.After(now) {
		desired = SandboxDesiredStateTerminating
	}
	body, _ := json.Marshal(struct {
		Domain string
		GC     protocol.MigrationSourceGCRequest
		Ack    protocol.MigrationSourceGCAcknowledgement
	}{"sandbox0-failed-checkpoint-restore-v1", gc, ack})
	terminal := sha256.Sum256(body)
	request, err := normalizeFinalizeRuntimeSlotRequest(&FinalizeRuntimeSlotRequest{SlotID: target.ID, OperationID: life.ID, ClaimID: target.ClaimID, Reason: "migration_failed",
		ProofDigest: terminal[:], ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest, ResourceCgroupAbsent: true})
	if err != nil {
		return nil, err
	}
	if _, err := finalizeRuntimeSlotTx(ctx, tx, target, request); err != nil {
		return nil, err
	}
	gcJSON, _ := json.Marshal(gc)
	ackJSON, _ := json.Marshal(ack)
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=evidence || jsonb_build_object('failure_gc',$2::jsonb,'failure_gc_ack',$3::jsonb) WHERE operation_id=$1`, life.ID, gcJSON, ackJSON); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandboxes SET runtime_generation=$2,desired_state=$3,updated_at=clock_timestamp() WHERE sandbox_id=$1`, owner.ID, life.ToGeneration, desired); err != nil {
		return nil, err
	}
	// This is the same owner's frozen checkpoint, rebased only to the consumed
	// attempt number. No memory bytes, source identity or disk head are changed.
	tag, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_refs SET runtime_generation=$2 WHERE sandbox_id=$1 AND runtime_generation=$3`, owner.ID, life.ToGeneration, life.FromGeneration)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrNomadCheckpointConflict
	}
	if err := (sandboxStoreTx{tx: tx}).AbortLifecycleTxn(ctx, life.ID, "memory restore failed; target execution was physically reclaimed"); err != nil {
		return nil, err
	}
	return scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, target.ID))
}
