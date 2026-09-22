package runtimeslotreconciler

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationCaptureFailureCompletionStore interface {
	GetNomadMigrationCaptureFailureForSlot(context.Context, string) (bool, *protocol.MigrationCaptureFailureReceipt, error)
	CompleteNomadSandboxMigrationCaptureFailure(context.Context, string, protocol.MigrationSourceGCRequest, protocol.MigrationSourceGCAcknowledgement) (*sandboxstore.RuntimeSlot, error)
}

// A failed capture keeps the ordinary durable reconciliation queue, but cannot
// use its generic writer/cleanup path. Only the committed finalization receipt
// permits allocation purge; direct-client absence and node ack precede release.
func (r *Reconciler) reconcileMigrationCaptureFailure(ctx context.Context, store migrationCaptureFailureCompletionStore, slot *sandboxstore.RuntimeSlot, receipt *protocol.MigrationCaptureFailureReceipt) (bool, error) {
	if receipt == nil {
		return false, nil
	}
	if receipt.Validate() != nil {
		return false, errors.New("failed capture finalization is invalid")
	}
	c := receipt.Request.Request.Cleanup
	if c.SlotID != slot.ID || c.AllocationID != slot.AllocationID || c.ClusterID != slot.ClusterID || c.NodeID != slot.NodeID ||
		c.NodeUID != slot.NodeUID || c.NodeBootID != slot.NodeBootID || c.NetNSIdentity != slot.NetNSIdentity ||
		c.WriterGrantID != slot.WriterGrantID || c.Resources != slot.ResourceLease || c.ResourceLeaseDigest != hex.EncodeToString(slot.ResourceLeaseDigest) {
		return false, errors.New("failed capture finalization changed physical custody")
	}
	target := allocationTarget(slot)
	if err := r.allocation.Purge(ctx, AllocationPurgeRequest{OperationID: operationIDs(slot).purge, Target: target}); err != nil {
		return false, fmt.Errorf("purge failed capture allocation: %w", err)
	}
	observation, err := r.allocation.Observe(ctx, target)
	if err != nil {
		return false, err
	}
	if err := validateAllocationObservation(observation, target); err != nil {
		return false, err
	}
	if observation.PhysicalPresent {
		return false, ErrAllocationStillPresent
	}
	if slot.State != sandboxstore.RuntimeSlotStateOrphaned {
		slot, err = r.store.MarkRuntimeSlotAllocationMissing(ctx, &sandboxstore.MarkRuntimeSlotAllocationMissingRequest{SlotID: slot.ID, AllocationID: slot.AllocationID,
			NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ObservationDigest: observation.ProofDigest})
		if err != nil {
			return false, err
		}
	}
	gc, ok := r.node.(MigrationSourceGC)
	if !ok {
		return false, errors.New("failed capture allocation acknowledgement is unavailable")
	}
	request := protocol.MigrationSourceGCRequest{Target: receipt.Request.Request.Capture.Request.Target,
		FinalizationDigest: receipt.Proof.RequestDigest, CleanupProofDigest: receipt.Request.Proof.Cleanup.ProofDigest,
		AllocationAbsenceDigest: hex.EncodeToString(slot.OrphanObservationDigest)}
	ack, err := gc.AcknowledgeMigrationSourceGC(ctx, request)
	if err != nil {
		return false, err
	}
	if ack == nil || ack.ValidateFor(request) != nil {
		return false, errors.New("failed capture allocation acknowledgement changed identity")
	}
	operation := receipt.Request.Request.Capture.Request.OperationID
	terminal, err := store.CompleteNomadSandboxMigrationCaptureFailure(ctx, operation, request, *ack)
	if err != nil {
		return false, err
	}
	if terminal == nil || terminal.ID != slot.ID || terminal.State != sandboxstore.RuntimeSlotStateTerminal || terminal.TerminalReason != "migration_capture_failed" ||
		terminal.ResourceLease != c.Resources || terminal.ResourceLeaseState != sandboxstore.RuntimeResourceLeaseReleased {
		return false, errors.New("failed capture authority did not release the exact carrier")
	}
	return true, nil
}
