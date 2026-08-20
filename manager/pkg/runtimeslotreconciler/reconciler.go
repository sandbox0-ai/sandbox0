// Package runtimeslotreconciler terminally cleans expired and orphaned Nomad
// runtime slots without depending on the task driver plugin process.
package runtimeslotreconciler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

const defaultLimit = 100

// ErrAllocationStillPresent means purge was accepted but physical client state
// has not disappeared yet. A later pass must retry observation.
var ErrAllocationStillPresent = errors.New("runtime slot allocation remains physically present")

// Store is the durable region authority used by the terminal reconciler.
type Store interface {
	ListRuntimeSlotsForReconcile(context.Context, int) ([]sandboxstore.RuntimeSlot, error)
	GetRuntimeSlot(context.Context, string) (*sandboxstore.RuntimeSlot, error)
	FenceRuntimeSlotForReconcile(context.Context, *sandboxstore.FenceRuntimeSlotForReconcileRequest) (*sandboxstore.RuntimeSlot, error)
	GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error)
	MarkRuntimeSlotAllocationMissing(context.Context, *sandboxstore.MarkRuntimeSlotAllocationMissingRequest) (*sandboxstore.RuntimeSlot, error)
	FinalizeRuntimeSlot(context.Context, *sandboxstore.FinalizeRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error)
}

// AllocationTarget is the immutable Nomad allocation incarnation. Allocation
// implementations must use the Nomad control API, not the task driver plugin.
type AllocationTarget struct {
	ClusterID           string
	AllocationID        string
	AllocationNamespace string
	NodeID              string
}

// AllocationObservation is canonical evidence about whether the physical
// allocation still owns client-side runtime state.
type AllocationObservation struct {
	Target          AllocationTarget
	PhysicalPresent bool
	ProofDigest     []byte
}

// AllocationController observes and purges Nomad allocation state without
// calling the task driver plugin. Purge must be exactly idempotent by
// OperationID and return only after stop/client-GC has been accepted.
type AllocationController interface {
	Observe(context.Context, AllocationTarget) (AllocationObservation, error)
	Purge(context.Context, AllocationPurgeRequest) error
}

// AllocationPurgeRequest identifies one exact physical purge operation.
type AllocationPurgeRequest struct {
	OperationID string
	Target      AllocationTarget
}

// NodeCleanupRequest asks ctld or an equivalent root-owned node controller to
// kill runsc and remove RootFS/network state without using the driver plugin.
type NodeCleanupRequest struct {
	OperationID       string
	SlotID            string
	ClusterID         string
	AllocationID      string
	NodeID            string
	NodeUID           string
	NodeBootID        string
	NetNSIdentity     string
	RunscContainerID  string
	WriterGrantID     string
	WriterFenceDigest []byte
}

// NodeCleanupProof is stable evidence that no node-local runtime, mount, or
// network ownership remains for the exact slot incarnation.
type NodeCleanupProof struct {
	OperationID  string
	SlotID       string
	AllocationID string
	NodeUID      string
	NodeBootID   string
	ProofDigest  []byte
}

// NodeCleaner is the plugin-independent node teardown boundary. Cleanup must
// be exactly idempotent by OperationID.
type NodeCleaner interface {
	Cleanup(context.Context, NodeCleanupRequest) (NodeCleanupProof, error)
}

// WriterFenceRequest removes regional renewal authority before destructive
// node cleanup starts.
type WriterFenceRequest struct {
	OperationID         string
	IssueOperationID    string
	SlotID              string
	SandboxID           string
	ClaimID             string
	GrantID             string
	WriterEpoch         int64
	BindingVersion      int
	BindingDigest       []byte
	NodeUID             string
	NodeBootID          string
	InitialGenerationID string
}

// WriterFenceProof is stable evidence that regional renewal is disabled. It
// must remain byte-identical after the grant advances from retiring to a
// terminal state.
type WriterFenceProof struct {
	OperationID string
	GrantID     string
	ProofDigest []byte
}

// WriterCompleteRequest terminally retires a fenced consumed writer after
// node cleanup proves every physical writer is gone.
type WriterCompleteRequest struct {
	OperationID       string
	GrantID           string
	SlotID            string
	WriterEpoch       int64
	WriterFenceDigest []byte
	NodeCleanupDigest []byte
}

// WriterFinalizeProof identifies the exact retired or canceled writer grant.
type WriterFinalizeProof struct {
	OperationID string
	GrantID     string
	State       string
	ProofDigest []byte
}

// WriterController must exactly retry cancellation of issued grants and the
// fence/complete sequence for consumed grants.
type WriterController interface {
	Fence(context.Context, WriterFenceRequest) (WriterFenceProof, error)
	Complete(context.Context, WriterCompleteRequest) (WriterFinalizeProof, error)
}

// Config wires independent Nomad, node, writer, and PostgreSQL authorities.
type Config struct {
	Store      Store
	Allocation AllocationController
	Node       NodeCleaner
	Writer     WriterController
	Limit      int
}

// Result summarizes one bounded reconciliation pass.
type Result struct {
	Candidates int
	Completed  int
	Skipped    int
	Failed     int
}

// Reconciler executes plugin-independent terminal cleanup.
type Reconciler struct {
	store      Store
	allocation AllocationController
	node       NodeCleaner
	writer     WriterController
	limit      int
}

// New constructs a bounded runtime slot reconciler.
func New(config Config) (*Reconciler, error) {
	if config.Store == nil || config.Allocation == nil || config.Node == nil || config.Writer == nil {
		return nil, errors.New("runtime slot reconcile dependencies are required")
	}
	limit := config.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	if limit < 1 || limit > sandboxstore.MaxRuntimeSlotReconcileLimit {
		return nil, fmt.Errorf("runtime slot reconcile limit must be between 1 and %d", sandboxstore.MaxRuntimeSlotReconcileLimit)
	}
	return &Reconciler{
		store: config.Store, allocation: config.Allocation, node: config.Node,
		writer: config.Writer, limit: limit,
	}, nil
}

// RunOnce processes a bounded candidate batch. One failed slot does not block
// independent slots; the returned error joins every per-slot failure.
func (r *Reconciler) RunOnce(ctx context.Context) (Result, error) {
	candidates, err := r.store.ListRuntimeSlotsForReconcile(ctx, r.limit)
	if err != nil {
		return Result{}, fmt.Errorf("list runtime slots for reconcile: %w", err)
	}
	result := Result{Candidates: len(candidates)}
	errs := make([]error, 0)
	for index := range candidates {
		completed, err := r.reconcile(ctx, candidates[index].ID)
		if err != nil {
			result.Failed++
			errs = append(errs, fmt.Errorf("reconcile runtime slot %s: %w", candidates[index].ID, err))
			continue
		}
		if completed {
			result.Completed++
		} else {
			result.Skipped++
		}
	}
	return result, errors.Join(errs...)
}

func (r *Reconciler) reconcile(ctx context.Context, slotID string) (bool, error) {
	slot, err := r.store.GetRuntimeSlot(ctx, slotID)
	if err != nil {
		if errors.Is(err, sandboxstore.ErrRuntimeSlotNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("reload candidate: %w", err)
	}
	if slot.State == sandboxstore.RuntimeSlotStateTerminal {
		return false, nil
	}
	if !runtimeSlotDue(slot) {
		return false, nil
	}
	if slot.ClaimID != "" && slot.State != sandboxstore.RuntimeSlotStateQuiescing &&
		slot.State != sandboxstore.RuntimeSlotStateOrphaned {
		slot, err = r.store.FenceRuntimeSlotForReconcile(ctx, &sandboxstore.FenceRuntimeSlotForReconcileRequest{
			SlotID: slot.ID, ExpectedRevision: slot.Revision,
		})
		if errors.Is(err, sandboxstore.ErrRuntimeSlotNotDue) || errors.Is(err, sandboxstore.ErrRuntimeSlotConflict) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("fence expired claim: %w", err)
		}
	}
	if err := validateSlotIdentity(slot); err != nil {
		return false, err
	}

	target := allocationTarget(slot)
	allocationObservation, err := r.allocation.Observe(ctx, target)
	if err != nil {
		return false, fmt.Errorf("observe Nomad allocation: %w", err)
	}
	if err := validateAllocationObservation(allocationObservation, target); err != nil {
		return false, err
	}

	ids := operationIDs(slot)
	var grant *sandboxstore.RootFSWriterGrant
	var writerFence WriterFenceProof
	if slot.WriterGrantID != "" {
		grant, err = r.store.GetRootFSWriterGrant(ctx, slot.WriterGrantID)
		if err != nil {
			return false, fmt.Errorf("load writer grant: %w", err)
		}
		if err := validateWriterGrant(grant, slot); err != nil {
			return false, err
		}
		writerFence, err = r.writer.Fence(ctx, WriterFenceRequest{
			OperationID: ids.writer, IssueOperationID: grant.IssueOperationID,
			SlotID: slot.ID, SandboxID: slot.SandboxID,
			ClaimID: slot.ClaimID, GrantID: grant.ID, WriterEpoch: grant.WriterEpoch,
			BindingVersion: grant.BindingVersion, BindingDigest: append([]byte(nil), grant.BindingDigest...),
			NodeUID: grant.NodeUID, NodeBootID: grant.NodeBootID,
			InitialGenerationID: grant.InitialGenerationID,
		})
		if err != nil {
			return false, fmt.Errorf("fence writer grant: %w", err)
		}
		if err := validateWriterFenceProof(writerFence, grant, ids.writer); err != nil {
			return false, err
		}
		storedGrant, err := r.store.GetRootFSWriterGrant(ctx, slot.WriterGrantID)
		if err != nil {
			return false, fmt.Errorf("reload fenced writer grant: %w", err)
		}
		if err := validateWriterGrant(storedGrant, slot); err != nil || !writerGrantIdentityEqual(storedGrant, grant) {
			return false, errors.New("fenced writer grant identity changed")
		}
		if storedGrant.State == sandboxstore.RootFSWriterGrantStateIssued ||
			storedGrant.State == sandboxstore.RootFSWriterGrantStateConsumed {
			return false, errors.New("writer controller did not persist a renewal fence")
		}
	}

	cleanupProof, err := r.node.Cleanup(ctx, NodeCleanupRequest{
		OperationID: ids.cleanup, SlotID: slot.ID, ClusterID: slot.ClusterID,
		AllocationID: slot.AllocationID, NodeID: slot.NodeID, NodeUID: slot.NodeUID,
		NodeBootID: slot.NodeBootID, NetNSIdentity: slot.NetNSIdentity,
		RunscContainerID: slot.RunscContainerID, WriterGrantID: slot.WriterGrantID,
		WriterFenceDigest: append([]byte(nil), writerFence.ProofDigest...),
	})
	if err != nil {
		return false, fmt.Errorf("clean node runtime: %w", err)
	}
	if err := validateNodeCleanupProof(cleanupProof, slot, ids.cleanup); err != nil {
		return false, err
	}

	var writerProof WriterFinalizeProof
	if slot.WriterGrantID != "" {
		writerProof, err = r.writer.Complete(ctx, WriterCompleteRequest{
			OperationID: ids.writer, GrantID: grant.ID, SlotID: slot.ID,
			WriterEpoch: grant.WriterEpoch, WriterFenceDigest: append([]byte(nil), writerFence.ProofDigest...),
			NodeCleanupDigest: append([]byte(nil), cleanupProof.ProofDigest...),
		})
		if err != nil {
			return false, fmt.Errorf("finalize writer grant: %w", err)
		}
		if err := validateWriterFinalizeProof(writerProof, grant, ids.writer); err != nil {
			return false, err
		}
		storedGrant, err := r.store.GetRootFSWriterGrant(ctx, slot.WriterGrantID)
		if err != nil {
			return false, fmt.Errorf("reload terminal writer grant: %w", err)
		}
		if err := validateWriterGrant(storedGrant, slot); err != nil || !writerGrantIdentityEqual(storedGrant, grant) {
			return false, errors.New("terminal writer grant identity changed")
		}
		if storedGrant.State != writerProof.State ||
			(storedGrant.State != sandboxstore.RootFSWriterGrantStateRetired && storedGrant.State != sandboxstore.RootFSWriterGrantStateCanceled) {
			return false, errors.New("writer finalizer did not persist a terminal grant")
		}
	}

	if allocationObservation.PhysicalPresent {
		if err := r.allocation.Purge(ctx, AllocationPurgeRequest{OperationID: ids.purge, Target: target}); err != nil {
			return false, fmt.Errorf("purge Nomad allocation: %w", err)
		}
		allocationObservation, err = r.allocation.Observe(ctx, target)
		if err != nil {
			return false, fmt.Errorf("confirm Nomad allocation purge: %w", err)
		}
		if err := validateAllocationObservation(allocationObservation, target); err != nil {
			return false, err
		}
	}
	if allocationObservation.PhysicalPresent {
		return false, ErrAllocationStillPresent
	}

	orphanProof := allocationObservation.ProofDigest
	if slot.State != sandboxstore.RuntimeSlotStateOrphaned {
		slot, err = r.store.MarkRuntimeSlotAllocationMissing(ctx, &sandboxstore.MarkRuntimeSlotAllocationMissingRequest{
			SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID,
			NodeBootID: slot.NodeBootID, ObservationDigest: append([]byte(nil), orphanProof...),
		})
		if err != nil {
			return false, fmt.Errorf("persist missing allocation: %w", err)
		}
		if slot.State == sandboxstore.RuntimeSlotStateTerminal {
			return true, nil
		}
	} else {
		orphanProof = slot.OrphanObservationDigest
		if err := validateProof("stored orphan observation", orphanProof); err != nil {
			return false, err
		}
	}

	reason := "reconciled_orphan"
	if slot.WriterGrantID == "" {
		reason = "prelaunch_abort"
	}
	terminalProof, err := terminalProofDigest(slot, cleanupProof, writerFence, writerProof, orphanProof)
	if err != nil {
		return false, err
	}
	terminal, err := r.store.FinalizeRuntimeSlot(ctx, &sandboxstore.FinalizeRuntimeSlotRequest{
		SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
		Reason: reason, ProofDigest: terminalProof,
	})
	if err != nil {
		return false, fmt.Errorf("finalize runtime slot: %w", err)
	}
	if terminal.State != sandboxstore.RuntimeSlotStateTerminal || terminal.TerminalReason != reason {
		return false, errors.New("runtime slot authority did not persist terminal state")
	}
	return true, nil
}

func runtimeSlotDue(slot *sandboxstore.RuntimeSlot) bool {
	if slot == nil {
		return false
	}
	if slot.State == sandboxstore.RuntimeSlotStateOrphaned {
		return true
	}
	if !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
		return true
	}
	return slot.State == sandboxstore.RuntimeSlotStateClaiming &&
		!slot.ClaimLeaseExpiresAt.After(slot.AuthorityObservedAt)
}

func validateSlotIdentity(slot *sandboxstore.RuntimeSlot) error {
	if slot == nil || slot.ID == "" || slot.ClusterID == "" || slot.AllocationID == "" ||
		slot.AllocationNamespace == "" || slot.NodeID == "" || slot.NodeUID == "" ||
		slot.NodeBootID == "" || slot.NetNSIdentity == "" {
		return errors.New("runtime slot has incomplete physical identity")
	}
	if slot.ClaimID != "" && (slot.ClaimOperationID == "" || slot.SandboxID == "" || slot.FilesystemID == "") {
		return errors.New("runtime slot has incomplete claim identity")
	}
	return nil
}

func allocationTarget(slot *sandboxstore.RuntimeSlot) AllocationTarget {
	return AllocationTarget{
		ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
		AllocationNamespace: slot.AllocationNamespace, NodeID: slot.NodeID,
	}
}

func validateAllocationObservation(observation AllocationObservation, target AllocationTarget) error {
	if observation.Target != target {
		return errors.New("Nomad allocation observation does not match runtime slot")
	}
	return validateProof("Nomad allocation observation", observation.ProofDigest)
}

func validateNodeCleanupProof(proof NodeCleanupProof, slot *sandboxstore.RuntimeSlot, operationID string) error {
	if proof.OperationID != operationID || proof.SlotID != slot.ID || proof.AllocationID != slot.AllocationID ||
		proof.NodeUID != slot.NodeUID || proof.NodeBootID != slot.NodeBootID {
		return errors.New("node cleanup proof does not match runtime slot")
	}
	return validateProof("node cleanup", proof.ProofDigest)
}

func validateWriterGrant(grant *sandboxstore.RootFSWriterGrant, slot *sandboxstore.RuntimeSlot) error {
	if grant == nil || grant.ID != slot.WriterGrantID || grant.SlotID != slot.ID ||
		grant.SandboxID != slot.SandboxID || grant.ClaimID != slot.ClaimID ||
		grant.FilesystemID != slot.FilesystemID || grant.NodeUID != slot.NodeUID ||
		grant.NodeBootID != slot.NodeBootID || grant.WriterEpoch <= 0 ||
		grant.IssueOperationID == "" || grant.BindingVersion != sandboxstore.RootFSWriterBindingVersion ||
		len(grant.BindingDigest) != sha256.Size || grant.InitialGenerationID == "" ||
		grant.InitialGenerationID != slot.SourceGenerationID {
		return errors.New("writer grant does not match runtime slot")
	}
	switch grant.State {
	case sandboxstore.RootFSWriterGrantStateIssued,
		sandboxstore.RootFSWriterGrantStateConsumed,
		sandboxstore.RootFSWriterGrantStateRetiring,
		sandboxstore.RootFSWriterGrantStateRetired,
		sandboxstore.RootFSWriterGrantStateCanceled:
	default:
		return errors.New("writer grant has an invalid state")
	}
	return nil
}

func writerGrantIdentityEqual(left, right *sandboxstore.RootFSWriterGrant) bool {
	return left != nil && right != nil &&
		left.ID == right.ID && left.FilesystemID == right.FilesystemID &&
		left.SandboxID == right.SandboxID && left.ClaimID == right.ClaimID &&
		left.SlotID == right.SlotID && left.IssueOperationID == right.IssueOperationID &&
		left.WriterEpoch == right.WriterEpoch && left.BindingVersion == right.BindingVersion &&
		bytes.Equal(left.BindingDigest, right.BindingDigest) && left.NodeUID == right.NodeUID &&
		left.NodeBootID == right.NodeBootID && left.InitialGenerationID == right.InitialGenerationID
}

func validateWriterFinalizeProof(proof WriterFinalizeProof, grant *sandboxstore.RootFSWriterGrant, operationID string) error {
	if proof.OperationID != operationID || proof.GrantID != grant.ID ||
		(proof.State != sandboxstore.RootFSWriterGrantStateRetired && proof.State != sandboxstore.RootFSWriterGrantStateCanceled) {
		return errors.New("writer finalization proof does not match runtime slot grant")
	}
	return validateProof("writer finalization", proof.ProofDigest)
}

func validateWriterFenceProof(proof WriterFenceProof, grant *sandboxstore.RootFSWriterGrant, operationID string) error {
	if proof.OperationID != operationID || proof.GrantID != grant.ID {
		return errors.New("writer fence proof does not match runtime slot grant")
	}
	return validateProof("writer fence", proof.ProofDigest)
}

func validateProof(name string, proof []byte) error {
	if len(proof) != sha256.Size {
		return fmt.Errorf("%s proof must contain exactly %d bytes", name, sha256.Size)
	}
	return nil
}

type derivedOperations struct {
	cleanup string
	writer  string
	purge   string
}

func operationIDs(slot *sandboxstore.RuntimeSlot) derivedOperations {
	base := strings.Join([]string{
		slot.ID, slot.ClusterID, slot.AllocationID, slot.NodeUID, slot.NodeBootID, slot.ClaimOperationID,
	}, "\x00")
	derive := func(phase string) string {
		sum := sha256.Sum256([]byte("sandbox0-runtime-slot-reconcile-v1\x00" + phase + "\x00" + base))
		return "reconcile-" + phase + "-" + hex.EncodeToString(sum[:])
	}
	return derivedOperations{cleanup: derive("cleanup"), writer: derive("writer"), purge: derive("purge")}
}

func terminalProofDigest(
	slot *sandboxstore.RuntimeSlot,
	cleanup NodeCleanupProof,
	fence WriterFenceProof,
	writer WriterFinalizeProof,
	orphanProof []byte,
) ([]byte, error) {
	payload, err := json.Marshal(struct {
		Version           int    `json:"version"`
		SlotID            string `json:"slot_id"`
		OperationID       string `json:"operation_id"`
		ClaimID           string `json:"claim_id"`
		CleanupDigest     string `json:"cleanup_digest"`
		WriterGrantID     string `json:"writer_grant_id,omitempty"`
		WriterFenceDigest string `json:"writer_fence_digest,omitempty"`
		WriterState       string `json:"writer_state,omitempty"`
		WriterDigest      string `json:"writer_digest,omitempty"`
		AllocationDigest  string `json:"allocation_digest"`
	}{
		Version: 1, SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
		CleanupDigest: hex.EncodeToString(cleanup.ProofDigest), WriterGrantID: writer.GrantID,
		WriterFenceDigest: hex.EncodeToString(fence.ProofDigest),
		WriterState:       writer.State, WriterDigest: hex.EncodeToString(writer.ProofDigest),
		AllocationDigest: hex.EncodeToString(orphanProof),
	})
	if err != nil {
		return nil, fmt.Errorf("encode runtime slot terminal proof: %w", err)
	}
	digest := sha256.Sum256(payload)
	return digest[:], nil
}
