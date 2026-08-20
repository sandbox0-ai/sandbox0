package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

var (
	ErrNomadSandboxPauseConflict = errors.New("nomad sandbox pause conflict")
	ErrNomadSandboxPauseNotReady = errors.New("nomad sandbox pause is not ready")
)

// NomadSandboxPauseCandidate is the exact active allocation and writer
// incarnation bound to one durable planned-pause lifecycle transaction.
type NomadSandboxPauseCandidate struct {
	SandboxID           string
	OperationID         string
	Source              string
	LifecyclePhase      string
	AlreadyPaused       bool
	ClaimOperationID    string
	ClaimID             string
	SlotID              string
	SlotState           string
	ClusterID           string
	AllocationID        string
	AllocationNamespace string
	NodeID              string
	WriterGrantID       string
	WriterGrantState    string
}

// RequestNomadSandboxPause persists a deterministic planned-retire intent
// before any external Nomad stop request. Retries recover the same lifecycle
// and exact slot/writer binding.
func (s *PGSandboxStore) RequestNomadSandboxPause(
	ctx context.Context,
	sandboxID string,
	source string,
) (*NomadSandboxPauseCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	originalSandboxID := sandboxID
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || sandboxID != originalSandboxID || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox_id is required, canonical, and at most 512 bytes")
	}
	source = strings.TrimSpace(source)
	if source != SandboxLifecycleSourceManual && source != SandboxLifecycleSourceAuto {
		return nil, fmt.Errorf("pause source must be manual or auto")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad sandbox pause tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	alreadyPaused := false
	switch record.DesiredState {
	case SandboxDesiredStatePaused:
		alreadyPaused = true
	case SandboxDesiredStateTerminating:
		return nil, fmt.Errorf("%w: sandbox termination is in progress", ErrNomadSandboxPauseConflict)
	case SandboxDesiredStateDeleted:
		return nil, fmt.Errorf("%w: sandbox is deleted", ErrSandboxRecordNotFound)
	case SandboxDesiredStateActive:
	default:
		return nil, fmt.Errorf("%w: sandbox desired state is %s", ErrNomadSandboxPauseConflict, record.DesiredState)
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if claim.Phase != SandboxRuntimeClaimPhaseReady || claim.OperationID == "" {
		return nil, fmt.Errorf("%w: sandbox runtime claim is %s", ErrNomadSandboxPauseNotReady, claim.Phase)
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE sandbox_id = $1 AND state <> $2
		FOR UPDATE
	`, sandboxID, RuntimeSlotStateTerminal))
	if errors.Is(err, pgx.ErrNoRows) {
		if alreadyPaused {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit already-paused Nomad sandbox: %w", err)
			}
			return &NomadSandboxPauseCandidate{SandboxID: sandboxID, AlreadyPaused: true}, nil
		}
		return nil, fmt.Errorf("%w: active runtime slot is missing", ErrNomadSandboxPauseNotReady)
	}
	if err != nil {
		return nil, fmt.Errorf("lock Nomad pause runtime slot: %w", err)
	}
	validSlotState := slot.State == RuntimeSlotStateActive || alreadyPaused &&
		(slot.State == RuntimeSlotStateQuiescing || slot.State == RuntimeSlotStateOrphaned)
	activeBindingMatches := alreadyPaused ||
		(slot.AllocationID == record.CurrentPodName && slot.AllocationNamespace == record.CurrentPodNamespace)
	if !validSlotState || slot.ClaimOperationID != claim.OperationID ||
		slot.ClaimID == "" || slot.WriterGrantID == "" || !activeBindingMatches || slot.ClusterID != record.ClusterID {
		return nil, fmt.Errorf("%w: active runtime slot does not match the sandbox claim", ErrNomadSandboxPauseNotReady)
	}
	grantRecord, err := getRootFSWriterGrantForUpdate(ctx, tx, slot.WriterGrantID)
	if err != nil {
		return nil, fmt.Errorf("lock Nomad pause writer grant: %w", err)
	}
	grant := &grantRecord.RootFSWriterGrant
	runtimeGeneration, parseErr := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if parseErr != nil || runtimeGeneration <= 0 || runtimeGeneration != record.RuntimeGeneration ||
		grant.SandboxID != sandboxID || grant.ClaimID != slot.ClaimID || grant.SlotID != slot.ID ||
		grant.FilesystemID != slot.FilesystemID || grant.InitialGenerationID != slot.SourceGenerationID ||
		grant.NodeUID != slot.NodeUID || grant.NodeBootID != slot.NodeBootID ||
		grant.PodNamespace != slot.AllocationNamespace || grant.PodUID != slot.AllocationID ||
		grant.NodeName != slot.NodeID || grant.GateParent == "" {
		return nil, fmt.Errorf("%w: writer grant does not match the active runtime", ErrNomadSandboxPauseNotReady)
	}
	operationID := rootfshandoff.PlannedRetireOperationID(grant.GateParent, grant.ID, grant.WriterEpoch)
	var lifecycle *SandboxLifecycleTxn
	if alreadyPaused {
		if grant.State != RootFSWriterGrantStateRetired || grant.RetireOperationID != operationID ||
			grant.RetireKind != RootFSWriterRetireKindPlannedPublish || len(grant.RetireProofDigest) == 0 {
			return nil, fmt.Errorf("%w: paused sandbox writer lacks planned retirement proof", ErrNomadSandboxPauseConflict)
		}
		lifecycle, err = scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
			WHERE txn_id = $1
			FOR UPDATE
		`, operationID))
		if err != nil || !nomadCommittedPlannedPauseLifecycleMatches(lifecycle, record, slot, grant, operationID) {
			return nil, fmt.Errorf("%w: paused sandbox lifecycle does not match planned retirement", ErrNomadSandboxPauseConflict)
		}
	} else {
		switch grant.State {
		case RootFSWriterGrantStateConsumed:
			if grant.RetireOperationID != "" || grant.RetireKind != "" || len(grant.RetireProofDigest) != 0 {
				return nil, fmt.Errorf("%w: consumed writer already has retirement state", ErrNomadSandboxPauseConflict)
			}
		case RootFSWriterGrantStateRetiring, RootFSWriterGrantStateRetired:
			if grant.RetireOperationID != operationID || grant.RetireKind != RootFSWriterRetireKindPlannedPublish {
				return nil, fmt.Errorf("%w: writer is owned by another retirement", ErrNomadSandboxPauseConflict)
			}
		default:
			return nil, fmt.Errorf("%w: writer grant is %s", ErrNomadSandboxPauseNotReady, grant.State)
		}
		lifecycle, err = getActiveLifecycleTxn(ctx, tx, sandboxID)
		if err != nil {
			return nil, fmt.Errorf("load active Nomad pause lifecycle: %w", err)
		}
		if lifecycle == nil {
			lifecycle = &SandboxLifecycleTxn{
				ID: operationID, SandboxID: sandboxID, Kind: SandboxLifecycleKindPause,
				Phase: SandboxLifecyclePhasePreparing, Source: source, Cancelable: false,
				FromGeneration: record.RuntimeGeneration, FromPodNamespace: record.CurrentPodNamespace,
				FromPodName: record.CurrentPodName, ExpectedHeadLayerID: grant.InitialGenerationID,
			}
			if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, lifecycle); err != nil {
				return nil, fmt.Errorf("begin Nomad planned pause lifecycle: %w", err)
			}
		} else if !nomadPlannedPauseLifecycleMatches(lifecycle, record, grant, operationID) {
			return nil, fmt.Errorf("%w: lifecycle %s owns the sandbox", ErrNomadSandboxPauseConflict, lifecycle.ID)
		}
	}
	candidate := &NomadSandboxPauseCandidate{
		SandboxID: sandboxID, OperationID: operationID, Source: lifecycle.Source,
		LifecyclePhase: lifecycle.Phase, AlreadyPaused: alreadyPaused,
		ClaimOperationID: claim.OperationID, ClaimID: slot.ClaimID,
		SlotID: slot.ID, SlotState: slot.State, ClusterID: slot.ClusterID,
		AllocationID: slot.AllocationID, AllocationNamespace: slot.AllocationNamespace,
		NodeID: slot.NodeID, WriterGrantID: grant.ID, WriterGrantState: grant.State,
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad sandbox pause request: %w", err)
	}
	return candidate, nil
}

func nomadCommittedPlannedPauseLifecycleMatches(
	txn *SandboxLifecycleTxn,
	record *SandboxRecord,
	slot *RuntimeSlot,
	grant *RootFSWriterGrant,
	operationID string,
) bool {
	if txn == nil || record == nil || slot == nil || grant == nil {
		return false
	}
	return txn.ID == operationID && txn.SandboxID == record.ID &&
		txn.Kind == SandboxLifecycleKindPause &&
		(txn.Source == SandboxLifecycleSourceManual || txn.Source == SandboxLifecycleSourceAuto) &&
		!txn.Cancelable && txn.CancelRequestedAt.IsZero() && txn.Phase == SandboxLifecyclePhaseCommitted &&
		txn.FromGeneration == record.RuntimeGeneration && txn.FromPodNamespace == slot.AllocationNamespace &&
		txn.FromPodName == slot.AllocationID && txn.ExpectedHeadLayerID == grant.InitialGenerationID &&
		txn.PreparedHeadLayerID != ""
}

func nomadPlannedPauseLifecycleMatches(
	txn *SandboxLifecycleTxn,
	record *SandboxRecord,
	grant *RootFSWriterGrant,
	operationID string,
) bool {
	if txn == nil || record == nil || grant == nil {
		return false
	}
	return txn.ID == operationID && txn.SandboxID == record.ID &&
		txn.Kind == SandboxLifecycleKindPause &&
		(txn.Source == SandboxLifecycleSourceManual || txn.Source == SandboxLifecycleSourceAuto) &&
		!txn.Cancelable && txn.CancelRequestedAt.IsZero() &&
		(txn.Phase == SandboxLifecyclePhasePreparing || txn.Phase == SandboxLifecyclePhaseBarriered ||
			txn.Phase == SandboxLifecyclePhasePublishing || txn.Phase == SandboxLifecyclePhaseCommitting) &&
		txn.FromGeneration == record.RuntimeGeneration &&
		txn.FromPodNamespace == record.CurrentPodNamespace && txn.FromPodName == record.CurrentPodName &&
		txn.ExpectedHeadLayerID == grant.InitialGenerationID && txn.PreparedHeadLayerID == ""
}
