// Package runtimeslotwriter adapts regional RootFS writer authority to the
// plugin-independent runtime slot terminal reconciler.
package runtimeslotwriter

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

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

const maxIdentityBytes = 512

// Store is the regional transactional authority required by Controller.
type Store interface {
	GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error)
	CancelRootFSWriterGrant(context.Context, *sandboxstore.CancelRootFSWriterGrantRequest) (*sandboxstore.RootFSWriterGrant, error)
	BeginRootFSWriterCrashAbandon(context.Context, *sandboxstore.BeginRootFSWriterCrashAbandonRequest) (*sandboxstore.RootFSWriterGrant, error)
	WithSandboxLock(context.Context, string, func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error
}

type sandboxRuntimeClaimReader interface {
	GetSandboxRuntimeClaim(context.Context, string) (*sandboxstore.SandboxRuntimeClaim, error)
}

type sandboxRuntimeSlotReader interface {
	GetRuntimeSlot(context.Context, string) (*sandboxstore.RuntimeSlot, error)
}

type sandboxCrashLifecycleStarter interface {
	BeginOrRestartRootFSWriterCrashLifecycleTxn(context.Context, *sandboxstore.SandboxLifecycleTxn) error
}

// Controller fences renewal and terminally abandons unsealed writers while
// preserving the last durable RootFS generation.
type Controller struct {
	store Store
}

var _ runtimeslotreconciler.WriterController = (*Controller)(nil)

// New constructs a regional runtime slot writer controller.
func New(store Store) (*Controller, error) {
	if store == nil {
		return nil, errors.New("runtime slot writer store is required")
	}
	return &Controller{store: store}, nil
}

// Fence disables renewal before node-local teardown. Issued grants are
// canceled. Consumed grants wait for server-time lease maturity after an
// unexpected loss, while durable explicit termination revokes renewal
// immediately for the exact still-bound runtime.
func (c *Controller) Fence(
	ctx context.Context,
	request runtimeslotreconciler.WriterFenceRequest,
) (runtimeslotreconciler.WriterFenceProof, error) {
	if err := validateFenceRequest(request); err != nil {
		return runtimeslotreconciler.WriterFenceProof{}, err
	}
	grant, err := c.store.GetRootFSWriterGrant(ctx, request.GrantID)
	if err != nil {
		return runtimeslotreconciler.WriterFenceProof{}, fmt.Errorf("load runtime slot writer grant: %w", err)
	}
	if err := validateFenceGrant(grant, request); err != nil {
		return runtimeslotreconciler.WriterFenceProof{}, err
	}

	switch grant.State {
	case sandboxstore.RootFSWriterGrantStateIssued, sandboxstore.RootFSWriterGrantStateCanceled:
		grant, err = c.store.CancelRootFSWriterGrant(ctx, &sandboxstore.CancelRootFSWriterGrantRequest{
			GrantID: grant.ID, WriterEpoch: grant.WriterEpoch, OperationID: grant.IssueOperationID,
			BindingVersion: grant.BindingVersion, BindingDigest: append([]byte(nil), grant.BindingDigest...),
		})
		if err != nil {
			return runtimeslotreconciler.WriterFenceProof{}, fmt.Errorf("cancel issued runtime slot writer: %w", err)
		}
		if err := validateFenceGrant(grant, request); err != nil || grant.State != sandboxstore.RootFSWriterGrantStateCanceled {
			return runtimeslotreconciler.WriterFenceProof{}, errors.New("writer cancellation returned another grant state")
		}
	case sandboxstore.RootFSWriterGrantStateConsumed, sandboxstore.RootFSWriterGrantStateRetiring:
		if grant.State == sandboxstore.RootFSWriterGrantStateRetiring {
			if err := validateCrashRetireGrant(grant, request.OperationID, false); err != nil {
				return runtimeslotreconciler.WriterFenceProof{}, err
			}
		}
		if err := c.ensureCrashLifecycle(ctx, grant, request); err != nil {
			return runtimeslotreconciler.WriterFenceProof{}, err
		}
		grant, err = c.store.BeginRootFSWriterCrashAbandon(ctx, &sandboxstore.BeginRootFSWriterCrashAbandonRequest{
			GrantID: grant.ID, WriterEpoch: grant.WriterEpoch, OperationID: request.OperationID,
			BindingVersion: grant.BindingVersion, BindingDigest: append([]byte(nil), grant.BindingDigest...),
			NodeUID: grant.NodeUID, NodeBootID: grant.NodeBootID,
			ExpectedOldGenerationID: grant.InitialGenerationID,
		})
		if err != nil {
			return runtimeslotreconciler.WriterFenceProof{}, fmt.Errorf("begin runtime slot writer crash abandon: %w", err)
		}
		if err := validateFenceGrant(grant, request); err != nil {
			return runtimeslotreconciler.WriterFenceProof{}, err
		}
		if err := validateCrashRetireGrant(grant, request.OperationID, false); err != nil {
			return runtimeslotreconciler.WriterFenceProof{}, err
		}
	case sandboxstore.RootFSWriterGrantStateRetired:
		if err := validateTerminalRetiredGrant(grant); err != nil {
			return runtimeslotreconciler.WriterFenceProof{}, err
		}
	default:
		return runtimeslotreconciler.WriterFenceProof{}, fmt.Errorf("writer grant is in unsupported state %s", grant.State)
	}

	return runtimeslotreconciler.WriterFenceProof{
		OperationID: request.OperationID,
		GrantID:     request.GrantID,
		ProofDigest: fenceProofDigest(request),
	}, nil
}

// Complete atomically retires a crash-fenced consumed writer after the exact
// node absence proof is available. Canceled issued grants remain terminal.
func (c *Controller) Complete(
	ctx context.Context,
	request runtimeslotreconciler.WriterCompleteRequest,
) (runtimeslotreconciler.WriterFinalizeProof, error) {
	if err := validateCompleteRequest(request); err != nil {
		return runtimeslotreconciler.WriterFinalizeProof{}, err
	}
	grant, err := c.store.GetRootFSWriterGrant(ctx, request.GrantID)
	if err != nil {
		return runtimeslotreconciler.WriterFinalizeProof{}, fmt.Errorf("load fenced runtime slot writer grant: %w", err)
	}
	if err := validateCompleteGrant(grant, request); err != nil {
		return runtimeslotreconciler.WriterFinalizeProof{}, err
	}
	expectedFence := fenceProofDigest(fenceRequestFromGrant(request.OperationID, grant))
	if !bytes.Equal(request.WriterFenceDigest, expectedFence) {
		return runtimeslotreconciler.WriterFinalizeProof{}, errors.New("writer completion does not match the canonical renewal fence")
	}

	switch grant.State {
	case sandboxstore.RootFSWriterGrantStateCanceled:
		if grant.RetireOperationID != "" || grant.RetireKind != "" || len(grant.RetireProofDigest) != 0 {
			return runtimeslotreconciler.WriterFinalizeProof{}, errors.New("canceled writer grant contains retirement state")
		}
	case sandboxstore.RootFSWriterGrantStateRetiring:
		if err := validateCrashRetireGrant(grant, request.OperationID, false); err != nil {
			return runtimeslotreconciler.WriterFinalizeProof{}, err
		}
		grant, err = c.completeCrashAbandon(ctx, request, grant)
		if err != nil {
			return runtimeslotreconciler.WriterFinalizeProof{}, err
		}
	case sandboxstore.RootFSWriterGrantStateRetired:
		if err := validateTerminalRetiredGrant(grant); err != nil {
			return runtimeslotreconciler.WriterFinalizeProof{}, err
		}
		if grant.RetireOperationID == request.OperationID &&
			grant.RetireKind == sandboxstore.RootFSWriterRetireKindCrashAbandon {
			grant, err = c.completeCrashAbandon(ctx, request, grant)
			if err != nil {
				return runtimeslotreconciler.WriterFinalizeProof{}, err
			}
		}
	default:
		return runtimeslotreconciler.WriterFinalizeProof{}, fmt.Errorf("writer grant is not renewal-fenced from state %s", grant.State)
	}

	return runtimeslotreconciler.WriterFinalizeProof{
		OperationID: request.OperationID,
		GrantID:     request.GrantID,
		State:       grant.State,
		ProofDigest: finalizeProofDigest(request, grant),
	}, nil
}

func (c *Controller) completeCrashAbandon(
	ctx context.Context,
	request runtimeslotreconciler.WriterCompleteRequest,
	grant *sandboxstore.RootFSWriterGrant,
) (*sandboxstore.RootFSWriterGrant, error) {
	var completed *sandboxstore.RootFSWriterGrant
	err := c.store.WithSandboxLock(ctx, grant.SandboxID, func(
		lockCtx context.Context,
		tx sandboxstore.SandboxStoreTx,
		_ *sandboxstore.SandboxRecord,
	) error {
		crashTx, ok := tx.(sandboxstore.RootFSWriterCrashAbandonTx)
		if !ok {
			return errors.New("sandbox transaction cannot abandon RootFS writers")
		}
		var completeErr error
		completed, completeErr = crashTx.CompleteRootFSWriterCrashAbandon(lockCtx, &sandboxstore.CompleteRootFSWriterCrashAbandonRequest{
			LifecycleTxnID: request.OperationID, GrantID: grant.ID, WriterEpoch: grant.WriterEpoch,
			OperationID: request.OperationID, BindingVersion: grant.BindingVersion,
			BindingDigest: append([]byte(nil), grant.BindingDigest...),
			ProofVersion:  sandboxstore.RootFSWriterCrashAbandonProofVersion,
			ProofDigest:   append([]byte(nil), request.NodeCleanupDigest...),
			NodeUID:       grant.NodeUID, NodeBootID: grant.NodeBootID,
			ExpectedOldGenerationID: grant.InitialGenerationID,
		})
		return completeErr
	})
	if err != nil {
		return nil, fmt.Errorf("complete runtime slot writer crash abandon: %w", err)
	}
	if err := validateCompleteGrant(completed, request); err != nil {
		return nil, err
	}
	if err := validateCrashRetireGrant(completed, request.OperationID, true); err != nil ||
		!bytes.Equal(completed.RetireProofDigest, request.NodeCleanupDigest) {
		return nil, errors.New("writer crash abandon returned another terminal proof")
	}
	return completed, nil
}

func (c *Controller) ensureCrashLifecycle(
	ctx context.Context,
	grant *sandboxstore.RootFSWriterGrant,
	request runtimeslotreconciler.WriterFenceRequest,
) error {
	runtimeGeneration, err := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if err != nil || runtimeGeneration <= 0 {
		return errors.New("writer grant has an invalid runtime generation")
	}
	err = c.store.WithSandboxLock(ctx, grant.SandboxID, func(
		lockCtx context.Context,
		tx sandboxstore.SandboxStoreTx,
		record *sandboxstore.SandboxRecord,
	) error {
		precommitResume := record != nil && record.DesiredState == sandboxstore.SandboxDesiredStatePaused &&
			record.DeletedAt.IsZero() && record.RuntimeNamespace == "" && record.RuntimeID == "" &&
			record.RuntimeGeneration >= 0 && record.RuntimeGeneration+1 == runtimeGeneration
		active, err := tx.GetActiveLifecycleTxn(lockCtx, grant.SandboxID)
		if err != nil {
			return err
		}
		if active != nil {
			if crashLifecycleMatches(active, grant, request.OperationID, runtimeGeneration) {
				return nil
			}
			if !precommitResume {
				return fmt.Errorf("another sandbox lifecycle transaction %s is active", active.ID)
			}
			slotReader, ok := tx.(sandboxRuntimeSlotReader)
			if !ok {
				return errors.New("sandbox transaction cannot inspect the failed Nomad resume slot")
			}
			slot, err := slotReader.GetRuntimeSlot(lockCtx, request.SlotID)
			if err != nil {
				return err
			}
			if !precommitResumeLifecycleMatches(active, record, grant, slot, runtimeGeneration) {
				return fmt.Errorf("another sandbox lifecycle transaction %s is active", active.ID)
			}
			if err := tx.AbortLifecycleTxn(
				lockCtx, active.ID, "resume runtime did not commit before terminal reconciliation",
			); err != nil {
				return fmt.Errorf("abort failed Nomad resume lifecycle: %w", err)
			}
		}
		regularRuntime := record != nil && record.DeletedAt.IsZero() && record.RuntimeGeneration == runtimeGeneration &&
			(record.DesiredState == sandboxstore.SandboxDesiredStateActive ||
				record.DesiredState == sandboxstore.SandboxDesiredStateTerminating)
		failedInitialClaim := false
		failedInitialClaimCandidate := record != nil && record.DeletedAt.IsZero() &&
			(record.DesiredState == sandboxstore.SandboxDesiredStateActive ||
				record.DesiredState == sandboxstore.SandboxDesiredStateTerminating) &&
			record.RuntimeNamespace == "" && record.RuntimeID == "" &&
			(record.RuntimeGeneration == runtimeGeneration ||
				record.RuntimeGeneration == 0 && runtimeGeneration == 1)
		if failedInitialClaimCandidate {
			claimReader, ok := tx.(sandboxRuntimeClaimReader)
			if !ok {
				return errors.New("sandbox transaction cannot inspect Nomad claim cleanup")
			}
			claim, err := claimReader.GetSandboxRuntimeClaim(lockCtx, grant.SandboxID)
			if err != nil {
				return err
			}
			failedInitialClaim = claim != nil && claim.Phase == sandboxstore.SandboxRuntimeClaimPhaseCleanupPending
		}
		fromRuntimeNamespace, fromRuntimeID := grant.RuntimeNamespace, grant.RuntimeIncarnationID
		switch {
		case regularRuntime && record.RuntimeNamespace == grant.RuntimeNamespace &&
			record.RuntimeID == grant.RuntimeIncarnationID:
			fromRuntimeNamespace = record.RuntimeNamespace
			fromRuntimeID = record.RuntimeID
		case failedInitialClaim, precommitResume:
			// A first claim publishes generation 1 only after command readiness.
			// Cleanup must therefore accept the durable generation-0 identity
			// while fencing the exact generation-1 writer bound to its slot.
		default:
			return errors.New("sandbox runtime does not match the runtime slot writer")
		}
		lifecycle := &sandboxstore.SandboxLifecycleTxn{
			ID: request.OperationID, SandboxID: grant.SandboxID,
			Kind: sandboxstore.SandboxLifecycleKindPause, Phase: sandboxstore.SandboxLifecyclePhasePublishing,
			Source: sandboxstore.SandboxLifecycleSourceCrash, Cancelable: false,
			FromGeneration: runtimeGeneration, FromRuntimeNamespace: fromRuntimeNamespace,
			FromRuntimeID: fromRuntimeID, ExpectedGenerationID: grant.InitialGenerationID,
		}
		if starter, ok := tx.(sandboxCrashLifecycleStarter); ok {
			return starter.BeginOrRestartRootFSWriterCrashLifecycleTxn(lockCtx, lifecycle)
		}
		return tx.BeginLifecycleTxn(lockCtx, lifecycle)
	})
	if err != nil {
		return fmt.Errorf("prepare runtime slot writer crash lifecycle: %w", err)
	}
	return nil
}

func precommitResumeLifecycleMatches(
	active *sandboxstore.SandboxLifecycleTxn,
	record *sandboxstore.SandboxRecord,
	grant *sandboxstore.RootFSWriterGrant,
	slot *sandboxstore.RuntimeSlot,
	runtimeGeneration int64,
) bool {
	return active != nil && record != nil && grant != nil && slot != nil &&
		active.ID == slot.ClaimOperationID && active.SandboxID == record.ID &&
		active.Kind == sandboxstore.SandboxLifecycleKindResume &&
		active.Source == sandboxstore.SandboxLifecycleSourceManual && !active.Cancelable &&
		active.CancelRequestedAt.IsZero() &&
		(active.Phase == sandboxstore.SandboxLifecyclePhasePreparing ||
			active.Phase == sandboxstore.SandboxLifecyclePhaseBarriered ||
			active.Phase == sandboxstore.SandboxLifecyclePhasePublishing ||
			active.Phase == sandboxstore.SandboxLifecyclePhaseCommitting) &&
		active.Epoch > 0 && active.Epoch == record.LifecycleEpoch &&
		active.FromGeneration == record.RuntimeGeneration && active.ToGeneration == runtimeGeneration &&
		active.ToGeneration == active.FromGeneration+1 &&
		active.FromRuntimeNamespace == "" && active.FromRuntimeID == "" &&
		active.ToRuntimeNamespace == "" && active.ToRuntimeID == "" &&
		active.ExpectedGenerationID == grant.InitialGenerationID && active.PreparedGenerationID == "" &&
		active.ID == sandboxstore.NomadSandboxResumeOperationID(
			record.ID, active.FromGeneration, active.ExpectedGenerationID, active.Epoch,
		) &&
		slot.ID == grant.SlotID && slot.SandboxID == record.ID && slot.WriterGrantID == grant.ID &&
		slot.ClaimID == grant.ClaimID && slot.SourceGenerationID == grant.InitialGenerationID &&
		slot.AllocationID == grant.RuntimeIncarnationID &&
		slot.AllocationNamespace == grant.RuntimeNamespace &&
		slot.State != sandboxstore.RuntimeSlotStateTerminal
}

func crashLifecycleMatches(
	active *sandboxstore.SandboxLifecycleTxn,
	grant *sandboxstore.RootFSWriterGrant,
	operationID string,
	runtimeGeneration int64,
) bool {
	return active != nil && active.ID == operationID && active.SandboxID == grant.SandboxID &&
		active.Kind == sandboxstore.SandboxLifecycleKindPause &&
		(active.Phase == sandboxstore.SandboxLifecyclePhasePublishing || active.Phase == sandboxstore.SandboxLifecyclePhaseCommitting) &&
		active.Source == sandboxstore.SandboxLifecycleSourceCrash && !active.Cancelable &&
		active.CancelRequestedAt.IsZero() && active.FromGeneration == runtimeGeneration &&
		active.FromRuntimeNamespace == grant.RuntimeNamespace && active.FromRuntimeID == grant.RuntimeIncarnationID &&
		active.ExpectedGenerationID == grant.InitialGenerationID && active.PreparedGenerationID == ""
}

func validateFenceRequest(request runtimeslotreconciler.WriterFenceRequest) error {
	for name, value := range map[string]string{
		"operation_id": request.OperationID, "issue_operation_id": request.IssueOperationID,
		"slot_id": request.SlotID, "sandbox_id": request.SandboxID, "claim_id": request.ClaimID,
		"grant_id": request.GrantID, "node_uid": request.NodeUID, "node_boot_id": request.NodeBootID,
		"initial_generation_id": request.InitialGenerationID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > maxIdentityBytes {
			return fmt.Errorf("%s is required, canonical, and at most %d bytes", name, maxIdentityBytes)
		}
	}
	if request.WriterEpoch <= 0 || request.BindingVersion != sandboxstore.RootFSWriterBindingVersion ||
		len(request.BindingDigest) != sha256.Size {
		return errors.New("writer fence binding is invalid")
	}
	return nil
}

func validateCompleteRequest(request runtimeslotreconciler.WriterCompleteRequest) error {
	for name, value := range map[string]string{
		"operation_id": request.OperationID, "grant_id": request.GrantID, "slot_id": request.SlotID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > maxIdentityBytes {
			return fmt.Errorf("%s is required, canonical, and at most %d bytes", name, maxIdentityBytes)
		}
	}
	if request.WriterEpoch <= 0 || len(request.WriterFenceDigest) != sha256.Size ||
		len(request.NodeCleanupDigest) != sha256.Size {
		return errors.New("writer completion proofs are invalid")
	}
	return nil
}

func validateFenceGrant(grant *sandboxstore.RootFSWriterGrant, request runtimeslotreconciler.WriterFenceRequest) error {
	if grant == nil || grant.ID != request.GrantID || grant.IssueOperationID != request.IssueOperationID ||
		grant.SlotID != request.SlotID || grant.SandboxID != request.SandboxID || grant.ClaimID != request.ClaimID ||
		grant.WriterEpoch != request.WriterEpoch || grant.BindingVersion != request.BindingVersion ||
		!bytes.Equal(grant.BindingDigest, request.BindingDigest) || grant.NodeUID != request.NodeUID ||
		grant.NodeBootID != request.NodeBootID || grant.InitialGenerationID != request.InitialGenerationID {
		return errors.New("writer grant does not match the runtime slot fence")
	}
	return nil
}

func validateCompleteGrant(grant *sandboxstore.RootFSWriterGrant, request runtimeslotreconciler.WriterCompleteRequest) error {
	if grant == nil || grant.ID != request.GrantID || grant.SlotID != request.SlotID ||
		grant.WriterEpoch != request.WriterEpoch || grant.BindingVersion != sandboxstore.RootFSWriterBindingVersion ||
		len(grant.BindingDigest) != sha256.Size || grant.SandboxID == "" || grant.ClaimID == "" ||
		grant.NodeUID == "" || grant.NodeBootID == "" || grant.InitialGenerationID == "" {
		return errors.New("writer grant does not match the runtime slot completion")
	}
	return nil
}

func validateCrashRetireGrant(grant *sandboxstore.RootFSWriterGrant, operationID string, terminal bool) error {
	if grant == nil || grant.RetireOperationID != operationID ||
		grant.RetireKind != sandboxstore.RootFSWriterRetireKindCrashAbandon {
		return errors.New("writer grant is bound to another retirement operation")
	}
	if terminal {
		if grant.State != sandboxstore.RootFSWriterGrantStateRetired || len(grant.RetireProofDigest) != sha256.Size {
			return errors.New("writer grant has an invalid crash-abandon terminal proof")
		}
	} else if grant.State != sandboxstore.RootFSWriterGrantStateRetiring && grant.State != sandboxstore.RootFSWriterGrantStateRetired {
		return errors.New("writer grant did not persist the renewal fence")
	} else if grant.State == sandboxstore.RootFSWriterGrantStateRetired && len(grant.RetireProofDigest) != sha256.Size {
		return errors.New("writer grant has an invalid crash-abandon terminal proof")
	}
	return nil
}

func validateTerminalRetiredGrant(grant *sandboxstore.RootFSWriterGrant) error {
	if grant == nil || grant.State != sandboxstore.RootFSWriterGrantStateRetired ||
		grant.RetireOperationID == "" || len(grant.RetireProofDigest) != sha256.Size {
		return errors.New("writer grant has an invalid terminal retirement")
	}
	switch grant.RetireKind {
	case sandboxstore.RootFSWriterRetireKindPlannedPublish,
		sandboxstore.RootFSWriterRetireKindPrelaunchAbort,
		sandboxstore.RootFSWriterRetireKindCrashAbandon:
		return nil
	default:
		return errors.New("writer grant has an unknown terminal retirement kind")
	}
}

func fenceRequestFromGrant(operationID string, grant *sandboxstore.RootFSWriterGrant) runtimeslotreconciler.WriterFenceRequest {
	return runtimeslotreconciler.WriterFenceRequest{
		OperationID: operationID, IssueOperationID: grant.IssueOperationID,
		SlotID: grant.SlotID, SandboxID: grant.SandboxID, ClaimID: grant.ClaimID, GrantID: grant.ID,
		WriterEpoch: grant.WriterEpoch, BindingVersion: grant.BindingVersion,
		BindingDigest: append([]byte(nil), grant.BindingDigest...), NodeUID: grant.NodeUID,
		NodeBootID: grant.NodeBootID, InitialGenerationID: grant.InitialGenerationID,
	}
}

func fenceProofDigest(request runtimeslotreconciler.WriterFenceRequest) []byte {
	payload, _ := json.Marshal(struct {
		Version             int    `json:"version"`
		OperationID         string `json:"operation_id"`
		IssueOperationID    string `json:"issue_operation_id"`
		SlotID              string `json:"slot_id"`
		SandboxID           string `json:"sandbox_id"`
		ClaimID             string `json:"claim_id"`
		GrantID             string `json:"grant_id"`
		WriterEpoch         int64  `json:"writer_epoch"`
		BindingVersion      int    `json:"binding_version"`
		BindingDigest       string `json:"binding_digest"`
		NodeUID             string `json:"node_uid"`
		NodeBootID          string `json:"node_boot_id"`
		InitialGenerationID string `json:"initial_generation_id"`
	}{
		Version: 1, OperationID: request.OperationID, IssueOperationID: request.IssueOperationID,
		SlotID: request.SlotID, SandboxID: request.SandboxID, ClaimID: request.ClaimID,
		GrantID: request.GrantID, WriterEpoch: request.WriterEpoch, BindingVersion: request.BindingVersion,
		BindingDigest: hex.EncodeToString(request.BindingDigest), NodeUID: request.NodeUID,
		NodeBootID: request.NodeBootID, InitialGenerationID: request.InitialGenerationID,
	})
	digest := sha256.Sum256(payload)
	return digest[:]
}

func finalizeProofDigest(
	request runtimeslotreconciler.WriterCompleteRequest,
	grant *sandboxstore.RootFSWriterGrant,
) []byte {
	payload, _ := json.Marshal(struct {
		Version             int    `json:"version"`
		OperationID         string `json:"operation_id"`
		GrantID             string `json:"grant_id"`
		SlotID              string `json:"slot_id"`
		WriterEpoch         int64  `json:"writer_epoch"`
		WriterFenceDigest   string `json:"writer_fence_digest"`
		NodeCleanupDigest   string `json:"node_cleanup_digest"`
		State               string `json:"state"`
		RetireOperationID   string `json:"retire_operation_id,omitempty"`
		RetireKind          string `json:"retire_kind,omitempty"`
		RetireProofDigest   string `json:"retire_proof_digest,omitempty"`
		InitialGenerationID string `json:"initial_generation_id"`
	}{
		Version: 1, OperationID: request.OperationID, GrantID: request.GrantID, SlotID: request.SlotID,
		WriterEpoch: request.WriterEpoch, WriterFenceDigest: hex.EncodeToString(request.WriterFenceDigest),
		NodeCleanupDigest: hex.EncodeToString(request.NodeCleanupDigest), State: grant.State,
		RetireOperationID: grant.RetireOperationID, RetireKind: grant.RetireKind,
		RetireProofDigest:   hex.EncodeToString(grant.RetireProofDigest),
		InitialGenerationID: grant.InitialGenerationID,
	})
	digest := sha256.Sum256(payload)
	return digest[:]
}
