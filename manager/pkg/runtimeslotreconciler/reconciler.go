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
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const defaultLimit = 100

// ErrAllocationStillPresent means purge was accepted but physical client state
// has not disappeared yet. A later pass must retry observation.
var ErrAllocationStillPresent = errors.New("runtime slot allocation remains physically present")

// Store is the durable region authority used by the terminal reconciler.
type Store interface {
	ListRuntimeSlotsForReconcile(context.Context, int) ([]sandboxstore.RuntimeSlot, error)
	GetRuntimeSlot(context.Context, string) (*sandboxstore.RuntimeSlot, error)
	GetActiveLifecycleTxn(context.Context, string) (*sandboxstore.SandboxLifecycleTxn, error)
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
	OperationID           string
	WriterOperationID     string
	WriterRetireKind      string
	SlotID                string
	ClusterID             string
	AllocationID          string
	NodeID                string
	NodeUID               string
	NodeBootID            string
	NetNSIdentity         string
	RunscContainerID      string
	WriterGrantID         string
	WriterAuthorityDigest []byte
	Resources             protocol.RuntimeResourceLease
	ResourceLeaseDigest   []byte
}

// NodeCleanupProof is stable evidence that no node-local runtime, mount, or
// network ownership remains for the exact slot incarnation.
type NodeCleanupProof struct {
	OperationID          string
	SlotID               string
	AllocationID         string
	NodeUID              string
	NodeBootID           string
	ResourceLeaseID      string
	ResourceLeaseDigest  []byte
	ResourceCgroupAbsent bool
	ProofDigest          []byte
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
	if slot.ClaimID != "" && slot.State != sandboxstore.RuntimeSlotStateQuiescing &&
		slot.State != sandboxstore.RuntimeSlotStateOrphaned {
		slot, err = r.store.FenceRuntimeSlotForReconcile(ctx, &sandboxstore.FenceRuntimeSlotForReconcileRequest{
			SlotID: slot.ID, ExpectedRevision: slot.Revision,
		})
		if errors.Is(err, sandboxstore.ErrRuntimeSlotNotDue) || errors.Is(err, sandboxstore.ErrRuntimeSlotConflict) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("fence runtime slot claim: %w", err)
		}
	} else if !runtimeSlotDue(slot) {
		return false, nil
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
	var writerProof WriterFinalizeProof
	writerRetireKind := ""
	writerAlreadyTerminal := false
	if slot.WriterGrantID != "" {
		grant, err = r.store.GetRootFSWriterGrant(ctx, slot.WriterGrantID)
		if err != nil {
			return false, fmt.Errorf("load writer grant: %w", err)
		}
		if err := validateWriterGrant(grant, slot); err != nil {
			return false, err
		}
		if terminalWriterNeedsDirectCleanup(grant, ids.writer) {
			writerFence, writerProof, writerRetireKind, err = terminalWriterProofs(grant)
			if err != nil {
				return false, err
			}
			writerAlreadyTerminal = true
		} else {
			plannedPending, pendingErr := r.plannedPausePending(ctx, slot, grant)
			if pendingErr != nil {
				return false, pendingErr
			}
			if plannedPending {
				return false, nil
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
			switch storedGrant.State {
			case sandboxstore.RootFSWriterGrantStateCanceled:
				writerRetireKind = protocol.WriterRetireKindCanceled
			case sandboxstore.RootFSWriterGrantStateRetiring:
				writerRetireKind = protocol.WriterRetireKindCrashAbandon
			case sandboxstore.RootFSWriterGrantStateRetired:
				if storedGrant.RetireKind != sandboxstore.RootFSWriterRetireKindCrashAbandon ||
					storedGrant.RetireOperationID != ids.writer || len(storedGrant.RetireProofDigest) != sha256.Size {
					return false, errors.New("writer controller returned another terminal retirement")
				}
				writerRetireKind = protocol.WriterRetireKindCrashAbandon
			default:
				return false, errors.New("writer controller did not persist a renewal fence")
			}
		}
	}

	writerOperationID := ""
	if grant != nil {
		writerOperationID = writerFence.OperationID
	}
	runscContainerID := slot.RunscContainerID
	if runscContainerID == "" {
		runscContainerID = protocol.NomadRunscContainerID(slot.ID)
	}
	cleanupProof, err := r.node.Cleanup(ctx, NodeCleanupRequest{
		OperationID: ids.cleanup, WriterOperationID: writerOperationID,
		WriterRetireKind: writerRetireKind,
		SlotID:           slot.ID, ClusterID: slot.ClusterID,
		AllocationID: slot.AllocationID, NodeID: slot.NodeID, NodeUID: slot.NodeUID,
		NodeBootID: slot.NodeBootID, NetNSIdentity: slot.NetNSIdentity,
		RunscContainerID: runscContainerID, WriterGrantID: slot.WriterGrantID,
		WriterAuthorityDigest: append([]byte(nil), writerFence.ProofDigest...),
		Resources:             slot.ResourceLease,
		ResourceLeaseDigest:   append([]byte(nil), slot.ResourceLeaseDigest...),
	})
	if err != nil {
		return false, fmt.Errorf("clean node runtime: %w", err)
	}
	if err := validateNodeCleanupProof(cleanupProof, slot, ids.cleanup); err != nil {
		return false, err
	}

	if slot.WriterGrantID != "" && !writerAlreadyTerminal {
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
	} else if writerAlreadyTerminal {
		storedGrant, err := r.store.GetRootFSWriterGrant(ctx, slot.WriterGrantID)
		if err != nil {
			return false, fmt.Errorf("reload already-terminal writer grant: %w", err)
		}
		storedFence, storedProof, storedKind, proofErr := terminalWriterProofs(storedGrant)
		if proofErr != nil || !writerGrantIdentityEqual(storedGrant, grant) ||
			storedKind != writerRetireKind || storedFence.OperationID != writerFence.OperationID ||
			storedProof.State != writerProof.State || !bytes.Equal(storedFence.ProofDigest, writerFence.ProofDigest) ||
			!bytes.Equal(storedProof.ProofDigest, writerProof.ProofDigest) {
			return false, errors.New("already-terminal writer authority changed during node cleanup")
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
		ResourceLeaseID:      cleanupProof.ResourceLeaseID,
		ResourceLeaseDigest:  append([]byte(nil), cleanupProof.ResourceLeaseDigest...),
		ResourceCgroupAbsent: cleanupProof.ResourceCgroupAbsent,
	})
	if err != nil {
		return false, fmt.Errorf("finalize runtime slot: %w", err)
	}
	if terminal.State != sandboxstore.RuntimeSlotStateTerminal || terminal.TerminalReason != reason {
		return false, errors.New("runtime slot authority did not persist terminal state")
	}
	return true, nil
}

func (r *Reconciler) plannedPausePending(
	ctx context.Context,
	slot *sandboxstore.RuntimeSlot,
	grant *sandboxstore.RootFSWriterGrant,
) (bool, error) {
	if slot == nil || grant == nil || grant.GateParent == "" {
		return false, nil
	}
	operationID := rootfshandoff.PlannedRetireOperationID(grant.GateParent, grant.ID, grant.WriterEpoch)
	switch grant.State {
	case sandboxstore.RootFSWriterGrantStateConsumed:
		if grant.RetireOperationID != "" || grant.RetireKind != "" || len(grant.RetireProofDigest) != 0 {
			return false, nil
		}
	case sandboxstore.RootFSWriterGrantStateRetiring:
		if grant.RetireOperationID != operationID ||
			grant.RetireKind != sandboxstore.RootFSWriterRetireKindPlannedPublish {
			return false, nil
		}
	default:
		return false, nil
	}
	lifecycle, err := r.store.GetActiveLifecycleTxn(ctx, slot.SandboxID)
	if err != nil {
		return false, fmt.Errorf("load active planned-pause lifecycle: %w", err)
	}
	runtimeGeneration, parseErr := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if parseErr != nil || runtimeGeneration <= 0 {
		return false, nil
	}
	if lifecycle == nil || lifecycle.ID != operationID || lifecycle.SandboxID != slot.SandboxID ||
		lifecycle.Kind != sandboxstore.SandboxLifecycleKindPause ||
		(lifecycle.Source != sandboxstore.SandboxLifecycleSourceManual &&
			lifecycle.Source != sandboxstore.SandboxLifecycleSourceAuto) ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		(lifecycle.Phase != sandboxstore.SandboxLifecyclePhasePreparing &&
			lifecycle.Phase != sandboxstore.SandboxLifecyclePhaseBarriered &&
			lifecycle.Phase != sandboxstore.SandboxLifecyclePhasePublishing &&
			lifecycle.Phase != sandboxstore.SandboxLifecyclePhaseCommitting) ||
		lifecycle.FromGeneration != runtimeGeneration ||
		lifecycle.FromRuntimeNamespace != grant.RuntimeNamespace ||
		lifecycle.FromRuntimeID != grant.RuntimeIncarnationID ||
		lifecycle.ExpectedGenerationID != grant.InitialGenerationID || lifecycle.PreparedGenerationID != "" {
		return false, nil
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
	if slot.State == sandboxstore.RuntimeSlotStateQuiescing {
		return true
	}
	if !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
		return true
	}
	return (slot.State == sandboxstore.RuntimeSlotStateClaiming ||
		slot.State == sandboxstore.RuntimeSlotStateStarting) &&
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
	if slot.ResourceLease.IsZero() {
		if len(slot.ResourceLeaseDigest) != 0 || slot.ResourceLeaseState != "" || !slot.ResourceLeaseReleasedAt.IsZero() {
			return errors.New("runtime slot has partial resource lease identity")
		}
		return nil
	}
	if err := slot.ResourceLease.Validate(); err != nil {
		return fmt.Errorf("runtime slot resource lease: %w", err)
	}
	if slot.ResourceLease.SlotID != slot.ID || slot.ResourceLease.ClusterID != slot.ClusterID ||
		slot.ResourceLease.NodeID != slot.NodeID || slot.ResourceLease.NodeUID != slot.NodeUID ||
		slot.ResourceLease.NodeBootID != slot.NodeBootID ||
		slot.ResourceLease.OperationID != slot.ClaimOperationID || slot.ResourceLease.ClaimID != slot.ClaimID ||
		slot.ResourceLeaseState != sandboxstore.RuntimeResourceLeaseActive || !slot.ResourceLeaseReleasedAt.IsZero() {
		return errors.New("runtime slot resource lease does not match its active claim")
	}
	digest, err := slot.ResourceLease.Digest()
	if err != nil {
		return fmt.Errorf("digest runtime slot resource lease: %w", err)
	}
	decoded, err := protocol.DecodeProof("resource_lease_digest", strings.TrimPrefix(digest, "sha256:"))
	if err != nil || !bytes.Equal(decoded, slot.ResourceLeaseDigest) {
		return errors.New("runtime slot resource lease digest changed")
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
		return errors.New("nomad allocation observation does not match runtime slot")
	}
	return validateProof("Nomad allocation observation", observation.ProofDigest)
}

func validateNodeCleanupProof(proof NodeCleanupProof, slot *sandboxstore.RuntimeSlot, operationID string) error {
	if proof.OperationID != operationID || proof.SlotID != slot.ID || proof.AllocationID != slot.AllocationID ||
		proof.NodeUID != slot.NodeUID || proof.NodeBootID != slot.NodeBootID {
		return errors.New("node cleanup proof does not match runtime slot")
	}
	if slot.ResourceLease.IsZero() {
		if proof.ResourceLeaseID != "" || len(proof.ResourceLeaseDigest) != 0 || proof.ResourceCgroupAbsent {
			return errors.New("legacy node cleanup proof contains resource lease facts")
		}
	} else if proof.ResourceLeaseID != slot.ResourceLease.LeaseID ||
		!bytes.Equal(proof.ResourceLeaseDigest, slot.ResourceLeaseDigest) || !proof.ResourceCgroupAbsent {
		return errors.New("node cleanup proof does not establish exact resource cgroup absence")
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

func terminalWriterProofs(
	grant *sandboxstore.RootFSWriterGrant,
) (WriterFenceProof, WriterFinalizeProof, string, error) {
	if grant == nil || grant.State != sandboxstore.RootFSWriterGrantStateRetired ||
		grant.RetireOperationID == "" || strings.TrimSpace(grant.RetireOperationID) != grant.RetireOperationID ||
		len(grant.RetireOperationID) > 512 || len(grant.RetireProofDigest) != sha256.Size {
		return WriterFenceProof{}, WriterFinalizeProof{}, "", errors.New("retired writer grant lacks canonical terminal authority proof")
	}
	var kind string
	switch grant.RetireKind {
	case sandboxstore.RootFSWriterRetireKindCrashAbandon:
		kind = protocol.WriterRetireKindCrashAbandon
	case sandboxstore.RootFSWriterRetireKindPlannedPublish:
		kind = protocol.WriterRetireKindPlannedPublish
	case sandboxstore.RootFSWriterRetireKindPrelaunchAbort:
		kind = protocol.WriterRetireKindPrelaunchAbort
	default:
		return WriterFenceProof{}, WriterFinalizeProof{}, "", errors.New("retired writer grant has an unsupported retirement kind")
	}
	fence := WriterFenceProof{
		OperationID: grant.RetireOperationID, GrantID: grant.ID,
		ProofDigest: append([]byte(nil), grant.RetireProofDigest...),
	}
	final := WriterFinalizeProof{
		OperationID: grant.RetireOperationID, GrantID: grant.ID,
		State: grant.State, ProofDigest: append([]byte(nil), grant.RetireProofDigest...),
	}
	return fence, final, kind, nil
}

func terminalWriterNeedsDirectCleanup(grant *sandboxstore.RootFSWriterGrant, reconcileOperationID string) bool {
	return grant != nil && grant.State == sandboxstore.RootFSWriterGrantStateRetired &&
		(grant.RetireKind != sandboxstore.RootFSWriterRetireKindCrashAbandon ||
			grant.RetireOperationID != reconcileOperationID)
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
		Version              int    `json:"version"`
		SlotID               string `json:"slot_id"`
		OperationID          string `json:"operation_id"`
		ClaimID              string `json:"claim_id"`
		CleanupDigest        string `json:"cleanup_digest"`
		WriterGrantID        string `json:"writer_grant_id,omitempty"`
		WriterFenceDigest    string `json:"writer_fence_digest,omitempty"`
		WriterState          string `json:"writer_state,omitempty"`
		WriterDigest         string `json:"writer_digest,omitempty"`
		AllocationDigest     string `json:"allocation_digest"`
		ResourceLeaseID      string `json:"resource_lease_id,omitempty"`
		ResourceLeaseDigest  string `json:"resource_lease_digest,omitempty"`
		ResourceCgroupAbsent bool   `json:"resource_cgroup_absent"`
	}{
		Version: 2, SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
		CleanupDigest: hex.EncodeToString(cleanup.ProofDigest), WriterGrantID: writer.GrantID,
		WriterFenceDigest: hex.EncodeToString(fence.ProofDigest),
		WriterState:       writer.State, WriterDigest: hex.EncodeToString(writer.ProofDigest),
		AllocationDigest:     hex.EncodeToString(orphanProof),
		ResourceLeaseID:      cleanup.ResourceLeaseID,
		ResourceLeaseDigest:  hex.EncodeToString(cleanup.ResourceLeaseDigest),
		ResourceCgroupAbsent: cleanup.ResourceCgroupAbsent,
	})
	if err != nil {
		return nil, fmt.Errorf("encode runtime slot terminal proof: %w", err)
	}
	digest := sha256.Sum256(payload)
	return digest[:], nil
}
