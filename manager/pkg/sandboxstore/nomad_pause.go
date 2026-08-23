package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

var (
	ErrNomadSandboxPauseConflict  = errors.New("nomad sandbox pause conflict")
	ErrNomadSandboxPauseNotReady  = errors.New("nomad sandbox pause is not ready")
	ErrNomadSandboxTTLNotExpired  = errors.New("nomad sandbox TTL is not expired")
	ErrNomadSandboxHardTTLExpired = errors.New("nomad sandbox hard TTL is expired")
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

// RootFSWriterPressurePauseRequest fences automatic pressure handling to one
// exact authenticated writer incarnation. The regional store must reject a
// sandbox that has already resumed onto a different slot or writer.
type RootFSWriterPressurePauseRequest struct {
	SandboxID      string
	GrantID        string
	WriterEpoch    int64
	BindingVersion int
	BindingDigest  []byte
	NodeUID        string
}

// RequestNomadSandboxPause persists a deterministic planned-retire intent
// before any external Nomad stop request. Retries recover the same lifecycle
// and exact slot/writer binding.
func (s *PGSandboxStore) RequestNomadSandboxPause(
	ctx context.Context,
	sandboxID string,
	source string,
) (*NomadSandboxPauseCandidate, error) {
	return s.requestNomadSandboxPause(ctx, sandboxID, source, nil, false)
}

// RequestNomadSandboxTTLPause starts an automatic pause only if the soft TTL
// is still due while holding the sandbox row lock. This prevents a stale scan
// from pausing a sandbox after a concurrent TTL refresh.
func (s *PGSandboxStore) RequestNomadSandboxTTLPause(
	ctx context.Context,
	sandboxID string,
) (*NomadSandboxPauseCandidate, error) {
	return s.requestNomadSandboxPause(ctx, sandboxID, SandboxLifecycleSourceAuto, nil, true)
}

// RequestNomadSandboxPressurePause persists the same planned pause while
// atomically proving that the reporting writer is still the active runtime.
func (s *PGSandboxStore) RequestNomadSandboxPressurePause(
	ctx context.Context,
	request *RootFSWriterPressurePauseRequest,
) (*NomadSandboxPauseCandidate, error) {
	if request == nil || strings.TrimSpace(request.SandboxID) == "" ||
		strings.TrimSpace(request.GrantID) == "" || request.WriterEpoch <= 0 ||
		request.BindingVersion != RootFSWriterBindingVersion || len(request.BindingDigest) != 32 ||
		strings.TrimSpace(request.NodeUID) == "" {
		return nil, fmt.Errorf("exact RootFS writer pressure binding is required")
	}
	copy := *request
	copy.BindingDigest = append([]byte(nil), request.BindingDigest...)
	return s.requestNomadSandboxPause(ctx, copy.SandboxID, SandboxLifecycleSourceAuto, &copy, false)
}

func (s *PGSandboxStore) requestNomadSandboxPause(
	ctx context.Context,
	sandboxID string,
	source string,
	pressure *RootFSWriterPressurePauseRequest,
	requireExpiredTTL bool,
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
	if requireExpiredTTL && !alreadyPaused {
		var authorityNow time.Time
		if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
			return nil, fmt.Errorf("read authority time for Nomad sandbox TTL pause: %w", err)
		}
		if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
			return nil, ErrNomadSandboxHardTTLExpired
		}
		if record.ExpiresAt.IsZero() || record.ExpiresAt.After(authorityNow) {
			return nil, ErrNomadSandboxTTLNotExpired
		}
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
		FOR UPDATE OF runtime_slots
	`, sandboxID, RuntimeSlotStateTerminal))
	if errors.Is(err, pgx.ErrNoRows) {
		if alreadyPaused {
			if pressure != nil {
				grantRecord, grantErr := getRootFSWriterGrantForUpdate(ctx, tx, pressure.GrantID)
				if grantErr != nil || !rootFSWriterPressureMatches(&grantRecord.RootFSWriterGrant, pressure) {
					return nil, fmt.Errorf("%w: pressured writer is not the paused runtime", ErrNomadSandboxPauseConflict)
				}
				grant := &grantRecord.RootFSWriterGrant
				operationID := rootfshandoff.PlannedRetireOperationID(grant.GateParent, grant.ID, grant.WriterEpoch)
				if grant.State != RootFSWriterGrantStateRetired || grant.RetireOperationID != operationID ||
					grant.RetireKind != RootFSWriterRetireKindPlannedPublish {
					return nil, fmt.Errorf("%w: pressured writer lacks planned retirement", ErrNomadSandboxPauseConflict)
				}
				if err := tx.Commit(ctx); err != nil {
					return nil, fmt.Errorf("commit already-paused pressured Nomad sandbox: %w", err)
				}
				return &NomadSandboxPauseCandidate{
					SandboxID: sandboxID, OperationID: operationID, AlreadyPaused: true,
					WriterGrantID: grant.ID, WriterGrantState: grant.State,
				}, nil
			}
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
		(slot.AllocationID == record.RuntimeID && slot.AllocationNamespace == record.RuntimeNamespace)
	if !validSlotState || slot.ClaimOperationID != claim.OperationID ||
		slot.ClaimID == "" || slot.WriterGrantID == "" || !activeBindingMatches || slot.ClusterID != record.ClusterID {
		return nil, fmt.Errorf("%w: active runtime slot does not match the sandbox claim", ErrNomadSandboxPauseNotReady)
	}
	grantRecord, err := getRootFSWriterGrantForUpdate(ctx, tx, slot.WriterGrantID)
	if err != nil {
		return nil, fmt.Errorf("lock Nomad pause writer grant: %w", err)
	}
	grant := &grantRecord.RootFSWriterGrant
	if pressure != nil && !rootFSWriterPressureMatches(grant, pressure) {
		return nil, fmt.Errorf("%w: pressured writer is no longer the active runtime", ErrNomadSandboxPauseConflict)
	}
	runtimeGeneration, parseErr := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if parseErr != nil || runtimeGeneration <= 0 || runtimeGeneration != record.RuntimeGeneration ||
		grant.SandboxID != sandboxID || grant.ClaimID != slot.ClaimID || grant.SlotID != slot.ID ||
		grant.FilesystemID != slot.FilesystemID || grant.InitialGenerationID != slot.SourceGenerationID ||
		grant.NodeUID != slot.NodeUID || grant.NodeBootID != slot.NodeBootID ||
		grant.RuntimeNamespace != slot.AllocationNamespace || grant.RuntimeIncarnationID != slot.AllocationID ||
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
				FromGeneration: record.RuntimeGeneration, FromRuntimeNamespace: record.RuntimeNamespace,
				FromRuntimeID: record.RuntimeID, ExpectedHeadLayerID: grant.InitialGenerationID,
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

func rootFSWriterPressureMatches(grant *RootFSWriterGrant, request *RootFSWriterPressurePauseRequest) bool {
	return grant != nil && request != nil && grant.SandboxID == request.SandboxID && grant.ID == request.GrantID &&
		grant.WriterEpoch == request.WriterEpoch && grant.BindingVersion == request.BindingVersion &&
		bytes.Equal(grant.BindingDigest, request.BindingDigest) && grant.NodeUID == request.NodeUID
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
		txn.FromGeneration == record.RuntimeGeneration && txn.FromRuntimeNamespace == slot.AllocationNamespace &&
		txn.FromRuntimeID == slot.AllocationID && txn.ExpectedHeadLayerID == grant.InitialGenerationID &&
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
		txn.FromRuntimeNamespace == record.RuntimeNamespace && txn.FromRuntimeID == record.RuntimeID &&
		txn.ExpectedHeadLayerID == grant.InitialGenerationID && txn.PreparedHeadLayerID == ""
}
