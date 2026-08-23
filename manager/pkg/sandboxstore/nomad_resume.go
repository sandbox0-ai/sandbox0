package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrNomadSandboxResumeConflict = errors.New("nomad sandbox resume conflict")
	ErrNomadSandboxResumeNotReady = errors.New("nomad sandbox resume is not ready")
)

// RequestNomadSandboxResumeRequest contains the quota decision that must be
// serialized with a paused-to-resuming transition. A nil limit is unlimited.
type RequestNomadSandboxResumeRequest struct {
	SandboxID          string
	ExpectedTeamID     string
	ActiveSandboxLimit *int64
}

// RetryNomadSandboxResumeRequest identifies a resume that may already be
// durable. It cannot create a lifecycle or reserve quota.
type RetryNomadSandboxResumeRequest struct {
	SandboxID      string
	ExpectedTeamID string
}

// NomadSandboxResumeCandidate is the immutable logical input for one exact
// paused-head to fresh-runtime transition.
type NomadSandboxResumeCandidate struct {
	SandboxID          string
	OperationID        string
	LifecyclePhase     string
	AlreadyActive      bool
	RuntimeGeneration  int64
	FilesystemID       string
	SourceGenerationID string
	Record             *SandboxRecord
}

// CompleteNomadSandboxResumeRequest commits only an exact command-ready slot
// previously acquired by the durable resume operation.
type CompleteNomadSandboxResumeRequest struct {
	SandboxID           string
	OperationID         string
	SlotID              string
	AllocationID        string
	AllocationNamespace string
	ResourceLeaseID     string
	ResourceLeaseDigest []byte
}

// NomadSandboxResumeOperationID derives the stable lifecycle and slot-claim
// identity for one paused runtime generation and RootFS head.
func NomadSandboxResumeOperationID(sandboxID string, fromGeneration int64, sourceGenerationID string) string {
	payload := fmt.Sprintf("%s\x00%d\x00%s", sandboxID, fromGeneration, sourceGenerationID)
	digest := sha256.Sum256([]byte(payload))
	return "nomad-resume-" + hex.EncodeToString(digest[:16])
}

// RetryNomadSandboxResume returns an already-active runtime or an existing
// exact resume lifecycle without consulting mutable quota policy. A false
// result means the caller must load quota and use RequestNomadSandboxResume.
func (s *PGSandboxStore) RetryNomadSandboxResume(
	ctx context.Context,
	request *RetryNomadSandboxResumeRequest,
) (*NomadSandboxResumeCandidate, bool, error) {
	normalized, err := normalizeRetryNomadSandboxResumeRequest(request)
	if err != nil {
		return nil, false, err
	}
	if s == nil || s.pool == nil {
		return nil, false, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("begin retry Nomad sandbox resume tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SandboxID)
	if err != nil {
		return nil, false, err
	}
	if record.TeamID != normalized.ExpectedTeamID {
		return nil, false, fmt.Errorf("%w: sandbox team identity changed", ErrNomadSandboxResumeConflict)
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, record.ID)
	if err != nil {
		return nil, false, err
	}
	if claim.Phase != SandboxRuntimeClaimPhaseReady || claim.OperationID == "" {
		return nil, false, fmt.Errorf("%w: sandbox runtime claim is %s", ErrNomadSandboxResumeNotReady, claim.Phase)
	}
	activeLifecycle, err := getActiveLifecycleTxn(ctx, tx, record.ID)
	if err != nil {
		return nil, false, fmt.Errorf("load active Nomad resume lifecycle: %w", err)
	}
	if record.DesiredState == SandboxDesiredStateActive {
		if activeLifecycle != nil {
			return nil, false, fmt.Errorf("%w: lifecycle %s owns the active sandbox", ErrNomadSandboxResumeConflict, activeLifecycle.ID)
		}
		if err := validateAlreadyActiveNomadSandbox(ctx, tx, record); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit already-active Nomad sandbox resume retry: %w", err)
		}
		return &NomadSandboxResumeCandidate{
			SandboxID: record.ID, AlreadyActive: true,
			RuntimeGeneration: record.RuntimeGeneration, Record: record,
		}, true, nil
	}
	if record.DesiredState == SandboxDesiredStateTerminating {
		return nil, false, fmt.Errorf("%w: sandbox termination is in progress", ErrNomadSandboxResumeConflict)
	}
	if record.DesiredState == SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return nil, false, fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, record.ID)
	}
	if record.DesiredState != SandboxDesiredStatePaused || record.CurrentPodName != "" ||
		record.CurrentPodNamespace != "" || record.RuntimeGeneration < 0 || record.RuntimeGeneration == math.MaxInt64 {
		return nil, false, fmt.Errorf("%w: sandbox is not a canonical paused runtime", ErrNomadSandboxResumeConflict)
	}
	if activeLifecycle == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit absent Nomad sandbox resume retry: %w", err)
		}
		return nil, false, nil
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, false, fmt.Errorf("read Nomad resume retry authority time: %w", err)
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return nil, false, fmt.Errorf("%w: sandbox hard TTL has expired", ErrNomadSandboxResumeConflict)
	}
	filesystemID, sourceGenerationID, err := lockNomadSandboxResumeHead(ctx, tx, record.ID)
	if err != nil {
		return nil, false, err
	}
	operationID := NomadSandboxResumeOperationID(record.ID, record.RuntimeGeneration, sourceGenerationID)
	if !nomadResumeLifecycleMatches(activeLifecycle, record, operationID, sourceGenerationID, false) {
		return nil, false, fmt.Errorf("%w: lifecycle %s owns the paused sandbox", ErrNomadSandboxResumeConflict, activeLifecycle.ID)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit Nomad sandbox resume retry: %w", err)
	}
	return nomadSandboxResumeCandidate(record, activeLifecycle, filesystemID, sourceGenerationID), true, nil
}

// RequestNomadSandboxResume reserves active-sandbox quota and creates the
// resume lifecycle before a warm slot or writer grant can be acquired.
func (s *PGSandboxStore) RequestNomadSandboxResume(
	ctx context.Context,
	request *RequestNomadSandboxResumeRequest,
) (*NomadSandboxResumeCandidate, error) {
	normalized, err := normalizeNomadSandboxResumeRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad sandbox resume tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockActiveSandboxQuotaTeam(ctx, tx, normalized.ExpectedTeamID); err != nil {
		return nil, fmt.Errorf("lock team sandbox resumes: %w", err)
	}

	record, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SandboxID)
	if err != nil {
		return nil, err
	}
	if record.TeamID != normalized.ExpectedTeamID {
		return nil, fmt.Errorf("%w: sandbox team identity changed", ErrNomadSandboxResumeConflict)
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if claim.Phase != SandboxRuntimeClaimPhaseReady || claim.OperationID == "" {
		return nil, fmt.Errorf("%w: sandbox runtime claim is %s", ErrNomadSandboxResumeNotReady, claim.Phase)
	}
	activeLifecycle, err := getActiveLifecycleTxn(ctx, tx, record.ID)
	if err != nil {
		return nil, fmt.Errorf("load active Nomad resume lifecycle: %w", err)
	}
	if record.DesiredState == SandboxDesiredStateActive {
		if activeLifecycle != nil {
			return nil, fmt.Errorf("%w: lifecycle %s owns the active sandbox", ErrNomadSandboxResumeConflict, activeLifecycle.ID)
		}
		if err := validateAlreadyActiveNomadSandbox(ctx, tx, record); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit already-active Nomad sandbox resume: %w", err)
		}
		return &NomadSandboxResumeCandidate{
			SandboxID: record.ID, AlreadyActive: true,
			RuntimeGeneration: record.RuntimeGeneration, Record: record,
		}, nil
	}
	if record.DesiredState == SandboxDesiredStateTerminating {
		return nil, fmt.Errorf("%w: sandbox termination is in progress", ErrNomadSandboxResumeConflict)
	}
	if record.DesiredState == SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return nil, fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, record.ID)
	}
	if record.DesiredState != SandboxDesiredStatePaused || record.CurrentPodName != "" || record.CurrentPodNamespace != "" ||
		record.RuntimeGeneration < 0 || record.RuntimeGeneration == math.MaxInt64 {
		return nil, fmt.Errorf("%w: sandbox is not a canonical paused runtime", ErrNomadSandboxResumeConflict)
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, fmt.Errorf("read Nomad resume authority time: %w", err)
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: sandbox hard TTL has expired", ErrNomadSandboxResumeConflict)
	}

	filesystemID, sourceGenerationID, err := lockNomadSandboxResumeHead(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	operationID := NomadSandboxResumeOperationID(record.ID, record.RuntimeGeneration, sourceGenerationID)
	if activeLifecycle != nil {
		if !nomadResumeLifecycleMatches(activeLifecycle, record, operationID, sourceGenerationID, false) {
			return nil, fmt.Errorf("%w: lifecycle %s owns the paused sandbox", ErrNomadSandboxResumeConflict, activeLifecycle.ID)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit Nomad sandbox resume retry: %w", err)
		}
		return nomadSandboxResumeCandidate(record, activeLifecycle, filesystemID, sourceGenerationID), nil
	}
	if err := ensureNomadResumePhysicalStateTerminal(ctx, tx, record.ID); err != nil {
		return nil, err
	}
	if err := reserveNomadResumeQuota(ctx, tx, record.TeamID, normalized.ActiveSandboxLimit); err != nil {
		return nil, err
	}
	lifecycle := &SandboxLifecycleTxn{
		ID: operationID, SandboxID: record.ID, Kind: SandboxLifecycleKindResume,
		Phase: SandboxLifecyclePhasePreparing, Source: SandboxLifecycleSourceManual, Cancelable: false,
		FromGeneration: record.RuntimeGeneration, ToGeneration: record.RuntimeGeneration + 1,
		ExpectedHeadLayerID: sourceGenerationID,
	}
	if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, lifecycle); err != nil {
		return nil, fmt.Errorf("begin Nomad resume lifecycle: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad sandbox resume request: %w", err)
	}
	return nomadSandboxResumeCandidate(record, lifecycle, filesystemID, sourceGenerationID), nil
}

// CompleteNomadSandboxResume atomically makes an exact command-ready slot the
// current runtime and commits its resume lifecycle.
func (s *PGSandboxStore) CompleteNomadSandboxResume(
	ctx context.Context,
	request *CompleteNomadSandboxResumeRequest,
) (*SandboxRecord, error) {
	normalized, err := normalizeCompleteNomadSandboxResumeRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin complete Nomad sandbox resume tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SandboxID)
	if err != nil {
		return nil, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2
		FOR UPDATE
	`, normalized.OperationID, normalized.SandboxID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: resume lifecycle is missing", ErrNomadSandboxResumeConflict)
	}
	if err != nil {
		return nil, fmt.Errorf("lock Nomad resume lifecycle: %w", err)
	}
	if lifecycle.Phase == SandboxLifecyclePhaseCommitted {
		if !nomadResumeLifecycleMatches(lifecycle, record, normalized.OperationID, lifecycle.ExpectedHeadLayerID, true) ||
			record.DesiredState != SandboxDesiredStateActive || record.CurrentPodName != normalized.AllocationID ||
			record.CurrentPodNamespace != normalized.AllocationNamespace {
			return nil, fmt.Errorf("%w: committed resume binding changed", ErrNomadSandboxResumeConflict)
		}
		slot, slotErr := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
			WHERE slot_id = $1 AND claim_operation_id = $2
			FOR SHARE OF runtime_slots
		`, normalized.SlotID, normalized.OperationID))
		if slotErr != nil || slot.State != RuntimeSlotStateActive || slot.SandboxID != record.ID ||
			slot.AllocationID != normalized.AllocationID || slot.AllocationNamespace != normalized.AllocationNamespace {
			return nil, fmt.Errorf("%w: committed resume slot changed", ErrNomadSandboxResumeConflict)
		}
		millicpu, memoryMiB, leaseErr := validateCompletedRuntimeResourceLease(
			slot, normalized.OperationID, normalized.ResourceLeaseID, normalized.ResourceLeaseDigest,
		)
		if leaseErr != nil || record.ResourceMillicpu != millicpu || record.ResourceMemoryMiB != memoryMiB {
			return nil, fmt.Errorf("%w: committed resume resource lease changed", ErrNomadSandboxResumeConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit completed Nomad resume retry: %w", err)
		}
		return record, nil
	}
	if !nomadResumeLifecycleMatches(lifecycle, record, normalized.OperationID, lifecycle.ExpectedHeadLayerID, false) {
		return nil, fmt.Errorf("%w: active resume lifecycle changed", ErrNomadSandboxResumeConflict)
	}
	filesystemID, sourceGenerationID, err := lockNomadSandboxResumeHead(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if sourceGenerationID != lifecycle.ExpectedHeadLayerID {
		return nil, fmt.Errorf("%w: paused RootFS head changed during resume", ErrNomadSandboxResumeConflict)
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE slot_id = $1 AND claim_operation_id = $2
		FOR UPDATE OF runtime_slots
	`, normalized.SlotID, normalized.OperationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: resumed runtime slot is missing", ErrNomadSandboxResumeNotReady)
	}
	if err != nil {
		return nil, fmt.Errorf("lock resumed runtime slot: %w", err)
	}
	if slot.State != RuntimeSlotStateActive || slot.SandboxID != record.ID || slot.FilesystemID != filesystemID ||
		slot.SourceGenerationID != sourceGenerationID || slot.AllocationID != normalized.AllocationID ||
		slot.AllocationNamespace != normalized.AllocationNamespace || slot.WriterGrantID == "" {
		return nil, fmt.Errorf("%w: runtime slot is not the exact command-ready resume", ErrNomadSandboxResumeNotReady)
	}
	resourceMillicpu, resourceMemoryMiB, err := validateCompletedRuntimeResourceLease(
		slot, normalized.OperationID, normalized.ResourceLeaseID, normalized.ResourceLeaseDigest,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNomadSandboxResumeNotReady, err)
	}
	grantRecord, err := getRootFSWriterGrantForUpdate(ctx, tx, slot.WriterGrantID)
	if err != nil {
		return nil, fmt.Errorf("lock resumed writer grant: %w", err)
	}
	grant := &grantRecord.RootFSWriterGrant
	runtimeGeneration, parseErr := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if parseErr != nil || runtimeGeneration != lifecycle.ToGeneration ||
		grant.State != RootFSWriterGrantStateConsumed || grant.SandboxID != record.ID ||
		grant.SlotID != slot.ID || grant.FilesystemID != filesystemID || grant.InitialGenerationID != sourceGenerationID ||
		grant.ClaimID != slot.ClaimID || grant.NodeUID != slot.NodeUID || grant.NodeBootID != slot.NodeBootID {
		return nil, fmt.Errorf("%w: writer grant is not the exact resumed runtime", ErrNomadSandboxResumeNotReady)
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, fmt.Errorf("read Nomad resume commit time: %w", err)
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: sandbox hard TTL expired during resume", ErrNomadSandboxResumeConflict)
	}
	expiresAt := time.Time{}
	if record.Config.TTL != nil && *record.Config.TTL > 0 {
		expiresAt = authorityNow.Add(time.Duration(*record.Config.TTL) * time.Second)
		if !record.HardExpiresAt.IsZero() && expiresAt.After(record.HardExpiresAt) {
			expiresAt = record.HardExpiresAt
		}
	}
	locked := sandboxStoreTx{tx: tx}
	if err := locked.SaveRuntime(ctx, record.ID, slot.AllocationNamespace, slot.AllocationID,
		lifecycle.ToGeneration, expiresAt, record.HardExpiresAt, SandboxRuntimeMetadata{}); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET resource_millicpu = $2, resource_memory_mib = $3, updated_at = NOW()
		WHERE sandbox_id = $1
	`, record.ID, resourceMillicpu, resourceMemoryMiB); err != nil {
		return nil, fmt.Errorf("persist resumed runtime lease metering: %w", err)
	}
	if err := locked.SetLifecycleTxnRuntime(ctx, lifecycle.ID, slot.AllocationNamespace, slot.AllocationID); err != nil {
		return nil, err
	}
	if err := locked.CommitLifecycleTxn(ctx, lifecycle.ID, ""); err != nil {
		return nil, err
	}
	completed, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1
	`, record.ID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit completed Nomad sandbox resume: %w", err)
	}
	return completed, nil
}

func normalizeNomadSandboxResumeRequest(request *RequestNomadSandboxResumeRequest) (*RequestNomadSandboxResumeRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("Nomad sandbox resume request is required")
	}
	normalized := *request
	normalized.SandboxID = strings.TrimSpace(request.SandboxID)
	normalized.ExpectedTeamID = strings.TrimSpace(request.ExpectedTeamID)
	if normalized.SandboxID == "" || normalized.SandboxID != request.SandboxID || len(normalized.SandboxID) > 512 ||
		normalized.ExpectedTeamID == "" || normalized.ExpectedTeamID != request.ExpectedTeamID || len(normalized.ExpectedTeamID) > 512 {
		return nil, fmt.Errorf("sandbox_id and expected_team_id must be canonical and at most 512 bytes")
	}
	if request.ActiveSandboxLimit != nil {
		if *request.ActiveSandboxLimit < 0 {
			return nil, fmt.Errorf("active sandbox limit must be non-negative")
		}
		limit := *request.ActiveSandboxLimit
		normalized.ActiveSandboxLimit = &limit
	}
	return &normalized, nil
}

func normalizeRetryNomadSandboxResumeRequest(request *RetryNomadSandboxResumeRequest) (*RetryNomadSandboxResumeRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("retry Nomad sandbox resume request is required")
	}
	normalized := *request
	normalized.SandboxID = strings.TrimSpace(request.SandboxID)
	normalized.ExpectedTeamID = strings.TrimSpace(request.ExpectedTeamID)
	if normalized.SandboxID == "" || normalized.SandboxID != request.SandboxID || len(normalized.SandboxID) > 512 ||
		normalized.ExpectedTeamID == "" || normalized.ExpectedTeamID != request.ExpectedTeamID || len(normalized.ExpectedTeamID) > 512 {
		return nil, fmt.Errorf("sandbox_id and expected_team_id must be canonical and at most 512 bytes")
	}
	return &normalized, nil
}

func normalizeCompleteNomadSandboxResumeRequest(request *CompleteNomadSandboxResumeRequest) (*CompleteNomadSandboxResumeRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("complete Nomad sandbox resume request is required")
	}
	normalized := *request
	for name, value := range map[string]string{
		"sandbox_id": request.SandboxID, "operation_id": request.OperationID, "slot_id": request.SlotID,
		"allocation_id": request.AllocationID, "allocation_namespace": request.AllocationNamespace,
		"resource_lease_id": request.ResourceLeaseID,
	} {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || trimmed != value || len(trimmed) > 512 {
			return nil, fmt.Errorf("%s must be canonical and at most 512 bytes", name)
		}
	}
	if len(request.ResourceLeaseDigest) != sha256.Size {
		return nil, fmt.Errorf("resource_lease_digest must contain exactly %d bytes", sha256.Size)
	}
	normalized.ResourceLeaseDigest = append([]byte(nil), request.ResourceLeaseDigest...)
	return &normalized, nil
}

func validateCompletedRuntimeResourceLease(
	slot *RuntimeSlot,
	operationID, leaseID string,
	leaseDigest []byte,
) (int64, int64, error) {
	if slot == nil || slot.ResourceLease.IsZero() || slot.ResourceLease.LeaseID != leaseID ||
		slot.ResourceLease.OperationID != operationID || slot.ResourceLease.SlotID != slot.ID ||
		slot.ResourceLeaseState != RuntimeResourceLeaseActive || !slot.ResourceLeaseReleasedAt.IsZero() ||
		!bytes.Equal(slot.ResourceLeaseDigest, leaseDigest) {
		return 0, 0, fmt.Errorf("runtime resource lease is not the exact active binding")
	}
	memoryMiB := (slot.ResourceLease.MemoryBytes + (1 << 20) - 1) / (1 << 20)
	if slot.ResourceLease.CPUMillicores <= 0 || memoryMiB <= 0 {
		return 0, 0, fmt.Errorf("runtime resource lease has invalid metering values")
	}
	return slot.ResourceLease.CPUMillicores, memoryMiB, nil
}

func lockNomadSandboxResumeHead(ctx context.Context, tx pgx.Tx, sandboxID string) (string, string, error) {
	var filesystemID, generationID string
	err := tx.QueryRow(ctx, `
		SELECT filesystem.filesystem_id, filesystem.head_generation_id
		FROM manager.sandbox_rootfs_bindings AS binding
		JOIN manager.rootfs_filesystems AS filesystem
			ON filesystem.filesystem_id = binding.filesystem_id
		JOIN manager.rootfs_generations AS generation
			ON generation.generation_id = filesystem.head_generation_id
			AND generation.filesystem_id = filesystem.filesystem_id
		WHERE binding.sandbox_id = $1
		FOR SHARE OF binding, filesystem, generation
	`, sandboxID).Scan(&filesystemID, &generationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("%w: paused sandbox RootFS head is missing", ErrNomadSandboxResumeNotReady)
	}
	if err != nil {
		return "", "", fmt.Errorf("lock Nomad resume RootFS head: %w", err)
	}
	if filesystemID == "" || generationID == "" {
		return "", "", fmt.Errorf("%w: paused sandbox RootFS head is invalid", ErrNomadSandboxResumeNotReady)
	}
	return filesystemID, generationID, nil
}

func ensureNomadResumePhysicalStateTerminal(ctx context.Context, tx pgx.Tx, sandboxID string) error {
	var nonterminalSlots, liveWriters int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.runtime_slots
		WHERE sandbox_id = $1 AND state <> $2
	`, sandboxID, RuntimeSlotStateTerminal).Scan(&nonterminalSlots); err != nil {
		return fmt.Errorf("count nonterminal Nomad runtime slots: %w", err)
	}
	if nonterminalSlots != 0 {
		return fmt.Errorf("%w: previous runtime slot is not terminal", ErrNomadSandboxResumeNotReady)
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_writer_grants
		WHERE sandbox_id = $1 AND state IN ($2, $3, $4)
	`, sandboxID, RootFSWriterGrantStateIssued, RootFSWriterGrantStateConsumed,
		RootFSWriterGrantStateRetiring).Scan(&liveWriters); err != nil {
		return fmt.Errorf("count live Nomad RootFS writers: %w", err)
	}
	if liveWriters != 0 {
		return fmt.Errorf("%w: previous RootFS writer is not terminal", ErrNomadSandboxResumeNotReady)
	}
	return nil
}

func reserveNomadResumeQuota(ctx context.Context, tx pgx.Tx, teamID string, limit *int64) error {
	if limit == nil {
		return nil
	}
	current, err := countActiveSandboxQuotaReservations(ctx, tx, teamID)
	if err != nil {
		return fmt.Errorf("count active and resuming sandboxes: %w", err)
	}
	if current >= *limit {
		return &ActiveSandboxQuotaExceededError{TeamID: teamID, Current: current, Limit: *limit}
	}
	return nil
}

func validateAlreadyActiveNomadSandbox(ctx context.Context, tx pgx.Tx, record *SandboxRecord) error {
	if record == nil || record.CurrentPodName == "" || record.CurrentPodNamespace == "" || record.RuntimeGeneration <= 0 {
		return fmt.Errorf("%w: active sandbox lacks its runtime binding", ErrNomadSandboxResumeNotReady)
	}
	var count int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.runtime_slots
		WHERE sandbox_id = $1 AND allocation_id = $2 AND allocation_namespace = $3
			AND state = $4 AND heartbeat_expires_at > NOW()
	`, record.ID, record.CurrentPodName, record.CurrentPodNamespace, RuntimeSlotStateActive).Scan(&count); err != nil {
		return fmt.Errorf("verify active Nomad resume runtime: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("%w: active sandbox has no exact command-ready slot", ErrNomadSandboxResumeNotReady)
	}
	return nil
}

func nomadSandboxResumeCandidate(
	record *SandboxRecord,
	lifecycle *SandboxLifecycleTxn,
	filesystemID string,
	sourceGenerationID string,
) *NomadSandboxResumeCandidate {
	return &NomadSandboxResumeCandidate{
		SandboxID: record.ID, OperationID: lifecycle.ID, LifecyclePhase: lifecycle.Phase,
		RuntimeGeneration: lifecycle.ToGeneration, FilesystemID: filesystemID,
		SourceGenerationID: sourceGenerationID, Record: record,
	}
}

func nomadResumeLifecycleMatches(
	lifecycle *SandboxLifecycleTxn,
	record *SandboxRecord,
	operationID string,
	sourceGenerationID string,
	committed bool,
) bool {
	if lifecycle == nil || record == nil || lifecycle.ID != operationID || lifecycle.SandboxID != record.ID ||
		lifecycle.Kind != SandboxLifecycleKindResume || lifecycle.Source != SandboxLifecycleSourceManual ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() || lifecycle.ExpectedHeadLayerID != sourceGenerationID ||
		lifecycle.PreparedHeadLayerID != "" || lifecycle.ToGeneration != lifecycle.FromGeneration+1 {
		return false
	}
	if committed {
		return lifecycle.Phase == SandboxLifecyclePhaseCommitted && record.RuntimeGeneration == lifecycle.ToGeneration &&
			lifecycle.ToPodNamespace != "" && lifecycle.ToPodName != "" &&
			record.CurrentPodNamespace == lifecycle.ToPodNamespace && record.CurrentPodName == lifecycle.ToPodName
	}
	return record.DesiredState == SandboxDesiredStatePaused && record.RuntimeGeneration == lifecycle.FromGeneration &&
		record.CurrentPodNamespace == "" && record.CurrentPodName == "" &&
		(lifecycle.Phase == SandboxLifecyclePhasePreparing || lifecycle.Phase == SandboxLifecyclePhaseBarriered ||
			lifecycle.Phase == SandboxLifecyclePhasePublishing || lifecycle.Phase == SandboxLifecyclePhaseCommitting)
}
