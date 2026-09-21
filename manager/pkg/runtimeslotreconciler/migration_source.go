package runtimeslotreconciler

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationCompletionStore interface {
	GetNomadSandboxMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error)
	AuthorizeNomadSandboxMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizeRequest, error)
	CommitNomadSandboxMigrationSourceFinalization(context.Context, protocol.MigrationSourceFinalizeRequest, protocol.MigrationSourceFinalizeProof) error
	CompleteNomadSandboxMigrationSource(context.Context, string) (*sandboxstore.RuntimeSlot, error)
}

// MigrationSourceFinalizer is optional during rolling upgrades. Unsupported
// transports retain source custody instead of invoking ordinary crash cleanup.
type MigrationSourceFinalizer interface {
	FinalizeMigrationSource(context.Context, protocol.MigrationSourceFinalizeRequest) (*protocol.MigrationSourceFinalizeProof, error)
}

type MigrationSourceGC interface {
	AcknowledgeMigrationSourceGC(context.Context, protocol.MigrationSourceGCRequest) (*protocol.MigrationSourceGCAcknowledgement, error)
}

// reconcileMigrationSource uses the same allocation controller and durable
// candidate queue as ordinary terminal cleanup. A retired migration writer is
// never translated into crash abandonment or a fresh RootFS publication.
func (r *Reconciler) reconcileMigrationSource(ctx context.Context, slot *sandboxstore.RuntimeSlot, grant *sandboxstore.RootFSWriterGrant) (bool, error) {
	store, ok := r.store.(migrationCompletionStore)
	if !ok {
		return false, errors.New("migration completion store is unavailable")
	}
	operation := grant.RetireOperationID
	if operation == "" || len(grant.RetireProofDigest) != 32 {
		return false, errors.New("migration source lacks terminal writer evidence")
	}
	receipt, err := store.GetNomadSandboxMigrationSourceFinalization(ctx, operation)
	if err != nil {
		return false, fmt.Errorf("read migration source receipt: %w", err)
	}
	var request protocol.MigrationSourceFinalizeRequest
	if receipt != nil {
		if receipt.Validate() != nil {
			return false, errors.New("stored migration source receipt is invalid")
		}
		request = receipt.Request
	} else {
		command, err := store.AuthorizeNomadSandboxMigrationSourceFinalization(ctx, operation)
		if errors.Is(err, sandboxstore.ErrNomadSandboxMigrationNotReady) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("authorize migration source finalization: %w", err)
		}
		if command == nil {
			return false, errors.New("migration source authorization is absent")
		}
		request = *command
	}
	c := request.Cleanup
	if request.Validate() != nil || request.Fence.PublicationRequest.Assignment.OperationID != operation ||
		c.SlotID != slot.ID || c.ClusterID != slot.ClusterID || c.AllocationID != slot.AllocationID || c.NodeID != slot.NodeID ||
		c.NodeUID != slot.NodeUID || c.NodeBootID != slot.NodeBootID || c.NetNSIdentity != slot.NetNSIdentity || c.RunscContainerID != slot.RunscContainerID ||
		c.WriterGrantID != grant.ID || c.WriterAuthorityDigest != hex.EncodeToString(grant.RetireProofDigest) || c.Resources != slot.ResourceLease || c.ResourceLeaseDigest != hex.EncodeToString(slot.ResourceLeaseDigest) {
		return false, errors.New("migration source finalization changed physical custody")
	}
	if receipt == nil {
		node, ok := r.node.(MigrationSourceFinalizer)
		if !ok {
			return false, errors.New("migration source node cleanup is unavailable")
		}
		proof, err := node.FinalizeMigrationSource(ctx, request)
		if err != nil {
			return false, fmt.Errorf("finalize migration source node: %w", err)
		}
		if proof == nil || proof.ValidateFor(request) != nil {
			return false, errors.New("migration source node returned invalid evidence")
		}
		if err := store.CommitNomadSandboxMigrationSourceFinalization(ctx, request, *proof); err != nil {
			return false, fmt.Errorf("commit migration source receipt: %w", err)
		}
		receipt = &protocol.MigrationSourceFinalizationReceipt{Request: request, Proof: *proof}
	}
	target := allocationTarget(slot)
	// Even a previously absent allocation requires an acknowledged purge, as
	// in ordinary cleanup, so scheduling notifications cannot be skipped.
	if err := r.allocation.Purge(ctx, AllocationPurgeRequest{OperationID: operationIDs(slot).purge, Target: target}); err != nil {
		return false, fmt.Errorf("purge migration source allocation: %w", err)
	}
	observation, err := r.allocation.Observe(ctx, target)
	if err != nil {
		return false, fmt.Errorf("confirm migration source allocation purge: %w", err)
	}
	if err := validateAllocationObservation(observation, target); err != nil {
		return false, err
	}
	if observation.PhysicalPresent {
		return false, ErrAllocationStillPresent
	}
	if slot.State != sandboxstore.RuntimeSlotStateOrphaned {
		_, err = r.store.MarkRuntimeSlotAllocationMissing(ctx, &sandboxstore.MarkRuntimeSlotAllocationMissingRequest{SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ObservationDigest: observation.ProofDigest})
		if err != nil {
			return false, fmt.Errorf("persist migration allocation absence: %w", err)
		}
	}
	// Keep this source in the existing durable reconciliation queue until ctld
	// acknowledges allocation GC. A lost reply retries before terminal commit;
	// the regional receipt already survives any subsequent node journal prune.
	gc, ok := r.node.(MigrationSourceGC)
	if !ok {
		return false, errors.New("migration source GC acknowledgement is unavailable")
	}
	gcRequest := protocol.MigrationSourceGCRequest{Target: request.Fence.PublicationRequest.Capture.Request.Target,
		FinalizationDigest: receipt.Proof.RequestDigest, CleanupProofDigest: receipt.Proof.Cleanup.ProofDigest,
		AllocationAbsenceDigest: hex.EncodeToString(observation.ProofDigest)}
	ack, err := gc.AcknowledgeMigrationSourceGC(ctx, gcRequest)
	if err != nil {
		return false, fmt.Errorf("acknowledge migration source allocation GC: %w", err)
	}
	if ack == nil || ack.ValidateFor(gcRequest) != nil {
		return false, errors.New("migration source GC acknowledgement changed request")
	}
	terminal, err := store.CompleteNomadSandboxMigrationSource(ctx, operation)
	if err != nil {
		return false, fmt.Errorf("complete migration source release: %w", err)
	}
	if terminal == nil || terminal.ID != c.SlotID || terminal.State != sandboxstore.RuntimeSlotStateTerminal || terminal.TerminalReason != "migration_source" ||
		terminal.ResourceLease != c.Resources || terminal.ResourceLeaseState != sandboxstore.RuntimeResourceLeaseReleased {
		return false, errors.New("migration source authority did not commit terminal release")
	}
	return true, nil
}
