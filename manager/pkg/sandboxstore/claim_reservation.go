package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

const (
	SandboxRuntimeClaimPhaseClaiming       = "claiming"
	SandboxRuntimeClaimPhaseReady          = "ready"
	SandboxRuntimeClaimPhaseCleanupPending = "cleanup_pending"
	SandboxRuntimeClaimPhaseCleaned        = "cleaned"

	MaxSandboxRuntimeClaimCleanupLimit = 1_000
)

var ErrActiveSandboxQuotaExceeded = errors.New("active sandbox quota exceeded")
var ErrSandboxClaimReservationConflict = errors.New("sandbox claim reservation conflict")
var ErrSandboxClaimCleanupPending = errors.New("sandbox claim cleanup is pending")

// ActiveSandboxQuotaExceededError describes the serialized region-wide quota
// decision made while reserving a new logical sandbox identity.
type ActiveSandboxQuotaExceededError struct {
	TeamID  string
	Current int64
	Limit   int64
}

func (e *ActiveSandboxQuotaExceededError) Error() string {
	if e == nil {
		return ErrActiveSandboxQuotaExceeded.Error()
	}
	return fmt.Sprintf("%s for team %s: current %d + requested 1 exceeds limit %d",
		ErrActiveSandboxQuotaExceeded, e.TeamID, e.Current, e.Limit)
}

func (e *ActiveSandboxQuotaExceededError) Unwrap() error {
	return ErrActiveSandboxQuotaExceeded
}

// SandboxRuntimeClaim is the durable admission workflow for one Nomad-backed
// logical sandbox. It is separate from desired lifecycle state: a ready claim
// can subsequently be paused, resumed, or deleted.
type SandboxRuntimeClaim struct {
	SandboxID        string
	OperationID      string
	Phase            string
	LeaseExpiresAt   time.Time
	LastError        string
	CompletedAt      time.Time
	CleanupStartedAt time.Time
	CleanedAt        time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	AuthorityNow     time.Time
}

// ReserveSandboxClaimRequest is the complete admission input for a new
// logical sandbox. A nil limit means unlimited admission.
type ReserveSandboxClaimRequest struct {
	Record             *SandboxRecord
	OperationID        string
	LeaseTTL           time.Duration
	ActiveSandboxLimit *int64
}

// RetrySandboxClaimRequest renews an exact in-progress operation without
// consulting quota policy again.
type RetrySandboxClaimRequest struct {
	Record      *SandboxRecord
	OperationID string
	LeaseTTL    time.Duration
}

// CompleteSandboxClaimRequest publishes the exact active runtime binding only
// after the slot registry has durable command-ready proof.
type CompleteSandboxClaimRequest struct {
	SandboxID           string
	OperationID         string
	SlotID              string
	AllocationID        string
	AllocationNamespace string
}

// SandboxClaimCleanupCandidate joins one due logical cleanup workflow to its
// optional physical runtime-slot incarnation.
type SandboxClaimCleanupCandidate struct {
	SandboxID             string
	OperationID           string
	SlotID                string
	SlotState             string
	PhysicalStateRequired bool
}

// RetrySandboxClaim locks and renews an exact claim if its deterministic
// sandbox identity already exists. Cleanup fencing wins atomically over late
// retries.
func (s *PGSandboxStore) RetrySandboxClaim(ctx context.Context, request *RetrySandboxClaimRequest) (*SandboxRecord, bool, error) {
	if s == nil || s.pool == nil {
		return nil, false, fmt.Errorf("sandbox store is not configured")
	}
	record, operationID, leaseTTL, err := normalizeSandboxClaimInput(request.Record, request.OperationID, request.LeaseTTL)
	if err != nil {
		return nil, false, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("begin sandbox claim retry tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	existing, found, err := retrySandboxClaimTx(ctx, tx, record, operationID, leaseTTL)
	if err != nil || !found {
		return nil, found, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit sandbox claim retry: %w", err)
	}
	return existing, true, nil
}

// ReserveSandboxClaim serializes claims per team and creates the logical
// sandbox and its retry lease in the same transaction as active-sandbox quota
// admission. A simultaneous exact retry renews the winner's existing lease.
func (s *PGSandboxStore) ReserveSandboxClaim(ctx context.Context, request *ReserveSandboxClaimRequest) (*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if request == nil {
		return nil, fmt.Errorf("sandbox claim reservation request is required")
	}
	record, operationID, leaseTTL, err := normalizeSandboxClaimInput(request.Record, request.OperationID, request.LeaseTTL)
	if err != nil {
		return nil, err
	}
	if request.ActiveSandboxLimit != nil && *request.ActiveSandboxLimit < 0 {
		return nil, fmt.Errorf("active sandbox limit must be non-negative")
	}
	args, err := sandboxRecordInsertArgs(record)
	if err != nil {
		return nil, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin sandbox claim reservation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockActiveSandboxQuotaTeam(ctx, tx, record.TeamID); err != nil {
		return nil, fmt.Errorf("lock team sandbox claims: %w", err)
	}
	if existing, found, err := retrySandboxClaimTx(ctx, tx, record, operationID, leaseTTL); err != nil {
		return nil, err
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit simultaneous sandbox claim retry: %w", err)
		}
		return existing, nil
	}

	if request.ActiveSandboxLimit != nil {
		current, err := countActiveSandboxQuotaReservations(ctx, tx, record.TeamID)
		if err != nil {
			return nil, fmt.Errorf("count active sandboxes for claim reservation: %w", err)
		}
		if current >= *request.ActiveSandboxLimit {
			return nil, &ActiveSandboxQuotaExceededError{
				TeamID: record.TeamID, Current: current, Limit: *request.ActiveSandboxLimit,
			}
		}
	}

	tag, err := tx.Exec(ctx, sandboxRecordInsertSQL+` ON CONFLICT (sandbox_id) DO NOTHING`, args...)
	if err != nil {
		return nil, fmt.Errorf("insert sandbox claim reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: sandbox ID %s was concurrently reserved", ErrSandboxClaimReservationConflict, record.ID)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.sandbox_runtime_claims (
			sandbox_id, operation_id, phase, lease_expires_at
		) VALUES ($1, $2, $3, NOW() + ($4 * INTERVAL '1 millisecond'))
	`, record.ID, operationID, SandboxRuntimeClaimPhaseClaiming, leaseTTL.Milliseconds()); err != nil {
		return nil, mapSandboxClaimConflict("insert sandbox runtime claim", err)
	}
	reserved, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1
		FOR UPDATE
	`, record.ID))
	if err != nil {
		return nil, fmt.Errorf("load inserted sandbox claim reservation: %w", err)
	}
	if reserved == nil {
		return nil, fmt.Errorf("inserted sandbox claim reservation disappeared")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sandbox claim reservation: %w", err)
	}
	return reserved, nil
}

// lockActiveSandboxQuotaTeam establishes one lock order for initial claims and
// paused resumes. Hash collisions only serialize unrelated teams.
func lockActiveSandboxQuotaTeam(ctx context.Context, tx pgx.Tx, teamID string) error {
	_, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, "sandbox-claim/team/"+teamID)
	return err
}

// countActiveSandboxQuotaReservations treats an in-progress durable resume as
// active capacity before its command-ready slot is committed.
func countActiveSandboxQuotaReservations(ctx context.Context, tx pgx.Tx, teamID string) (int64, error) {
	var current int64
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.sandboxes AS sandbox
		WHERE sandbox.team_id = $1
			AND sandbox.deleted_at IS NULL
			AND (
				sandbox.desired_state = $2
				OR EXISTS (
					SELECT 1 FROM manager.sandbox_lifecycle_txns AS lifecycle
					WHERE lifecycle.sandbox_id = sandbox.sandbox_id
						AND lifecycle.kind = $3
						AND lifecycle.phase IN ($4, $5, $6, $7)
				)
			)
	`, teamID, SandboxDesiredStateActive, SandboxLifecycleKindResume,
		SandboxLifecyclePhasePreparing, SandboxLifecyclePhaseBarriered,
		SandboxLifecyclePhasePublishing, SandboxLifecyclePhaseCommitting).Scan(&current)
	return current, err
}

// CompleteSandboxClaim atomically binds the exact active slot to the logical
// sandbox and makes the claim immune to abandoned-claim cleanup.
func (s *PGSandboxStore) CompleteSandboxClaim(ctx context.Context, request *CompleteSandboxClaimRequest) (*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if err := validateCompleteSandboxClaimRequest(request); err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin complete sandbox claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, request.SandboxID)
	if err != nil {
		return nil, err
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, request.SandboxID)
	if err != nil {
		return nil, err
	}
	if claim.OperationID != request.OperationID {
		return nil, fmt.Errorf("%w: operation identity changed", ErrSandboxClaimReservationConflict)
	}
	if claim.Phase == SandboxRuntimeClaimPhaseReady {
		if record.CurrentPodName != request.AllocationID || record.CurrentPodNamespace != request.AllocationNamespace {
			return nil, fmt.Errorf("%w: ready allocation binding changed", ErrSandboxClaimReservationConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit completed sandbox claim retry: %w", err)
		}
		return record, nil
	}
	if claim.Phase != SandboxRuntimeClaimPhaseClaiming {
		return nil, fmt.Errorf("%w: claim is %s", ErrSandboxClaimCleanupPending, claim.Phase)
	}
	var slotState, slotSandboxID, allocationID, allocationNamespace string
	if err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(sandbox_id, ''), allocation_id, allocation_namespace
		FROM manager.runtime_slots
		WHERE slot_id = $1 AND claim_operation_id = $2
		FOR SHARE
	`, request.SlotID, request.OperationID).Scan(
		&slotState, &slotSandboxID, &allocationID, &allocationNamespace,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: command-ready runtime slot is missing", ErrSandboxClaimReservationConflict)
		}
		return nil, fmt.Errorf("verify command-ready runtime slot: %w", err)
	}
	if slotState != RuntimeSlotStateActive || slotSandboxID != request.SandboxID ||
		allocationID != request.AllocationID || allocationNamespace != request.AllocationNamespace {
		return nil, fmt.Errorf("%w: runtime slot is not the exact active claim binding", ErrSandboxClaimReservationConflict)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET current_pod_name = $2, current_pod_namespace = $3, updated_at = NOW()
		WHERE sandbox_id = $1
	`, request.SandboxID, request.AllocationID, request.AllocationNamespace); err != nil {
		return nil, fmt.Errorf("bind completed sandbox runtime: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET phase = $2, lease_expires_at = NULL, completed_at = NOW(), last_error = ''
		WHERE sandbox_id = $1
	`, request.SandboxID, SandboxRuntimeClaimPhaseReady); err != nil {
		return nil, fmt.Errorf("complete sandbox runtime claim: %w", err)
	}
	completed, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+` WHERE sandbox_id = $1`, request.SandboxID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit completed sandbox claim: %w", err)
	}
	return completed, nil
}

// ListSandboxRuntimeClaimsForCleanup returns a bounded oldest-first batch of
// expired or already fenced logical claims.
func (s *PGSandboxStore) ListSandboxRuntimeClaimsForCleanup(ctx context.Context, limit int) ([]SandboxRuntimeClaim, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if limit < 1 || limit > MaxSandboxRuntimeClaimCleanupLimit {
		return nil, fmt.Errorf("sandbox claim cleanup limit must be between 1 and %d", MaxSandboxRuntimeClaimCleanupLimit)
	}
	rows, err := s.pool.Query(ctx, sandboxRuntimeClaimSelectSQL()+`
		WHERE phase = $1 OR (phase = $2 AND lease_expires_at <= NOW())
		ORDER BY CASE WHEN phase = $1 THEN 0 ELSE 1 END,
			lease_expires_at NULLS FIRST, updated_at, sandbox_id
		LIMIT $3
	`, SandboxRuntimeClaimPhaseCleanupPending, SandboxRuntimeClaimPhaseClaiming, limit)
	if err != nil {
		return nil, fmt.Errorf("list sandbox runtime claims for cleanup: %w", err)
	}
	defer rows.Close()
	claims := make([]SandboxRuntimeClaim, 0, limit)
	for rows.Next() {
		claim, err := scanSandboxRuntimeClaim(rows)
		if err != nil {
			return nil, err
		}
		claims = append(claims, *claim)
	}
	return claims, rows.Err()
}

// RequestSandboxRuntimeClaimCleanup atomically makes an explicit delete win
// over a concurrent claim completion and makes the exact physical slot due for
// plugin-independent terminal reconciliation.
func (s *PGSandboxStore) RequestSandboxRuntimeClaimCleanup(
	ctx context.Context,
	sandboxID, reason string,
) (*SandboxClaimCleanupCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	originalSandboxID := sandboxID
	sandboxID = strings.TrimSpace(sandboxID)
	reason = strings.TrimSpace(reason)
	if sandboxID == "" || sandboxID != originalSandboxID || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox ID is required, canonical, and at most 512 bytes")
	}
	if len(reason) > 2_048 {
		return nil, fmt.Errorf("sandbox claim cleanup reason exceeds 2048 bytes")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin sandbox claim cleanup request tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var operationID string
	if err := tx.QueryRow(ctx, `
		SELECT operation_id
		FROM manager.sandbox_runtime_claims
		WHERE sandbox_id = $1
	`, sandboxID).Scan(&operationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: sandbox runtime claim is missing", ErrSandboxClaimReservationConflict)
		}
		return nil, fmt.Errorf("load sandbox cleanup operation identity: %w", err)
	}
	if err := lockRuntimeSlotClaimOperation(ctx, tx, operationID); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if claim.OperationID != operationID {
		return nil, fmt.Errorf("%w: cleanup operation identity changed", ErrSandboxClaimReservationConflict)
	}

	if claim.Phase == SandboxRuntimeClaimPhaseCleaned {
		if record.DesiredState != SandboxDesiredStateDeleted || record.DeletedAt.IsZero() {
			return nil, fmt.Errorf("%w: cleaned claim has a live sandbox record", ErrSandboxClaimReservationConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit cleaned sandbox deletion retry: %w", err)
		}
		return nil, nil
	}
	firstCleanupRequest := claim.Phase != SandboxRuntimeClaimPhaseCleanupPending
	if firstCleanupRequest {
		if err := abortConflictingSandboxLifecycleForClaimCleanup(ctx, tx, record); err != nil {
			return nil, err
		}
	}
	if record.DesiredState == SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		if claim.Phase != SandboxRuntimeClaimPhaseCleanupPending ||
			record.DesiredState != SandboxDesiredStateDeleted || record.DeletedAt.IsZero() {
			return nil, fmt.Errorf("%w: deleted sandbox has an incomplete claim state", ErrSandboxClaimReservationConflict)
		}
	} else {
		switch record.DesiredState {
		case SandboxDesiredStateActive, SandboxDesiredStatePaused, SandboxDesiredStateTerminating:
		default:
			return nil, fmt.Errorf("%w: sandbox desired state is %s", ErrSandboxClaimReservationConflict, record.DesiredState)
		}
		switch claim.Phase {
		case SandboxRuntimeClaimPhaseClaiming, SandboxRuntimeClaimPhaseReady, SandboxRuntimeClaimPhaseCleanupPending:
		default:
			return nil, fmt.Errorf("%w: claim is %s", ErrSandboxClaimReservationConflict, claim.Phase)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE manager.sandboxes
			SET desired_state = $2, updated_at = NOW()
			WHERE sandbox_id = $1 AND deleted_at IS NULL
		`, record.ID, SandboxDesiredStateTerminating); err != nil {
			return nil, fmt.Errorf("persist Nomad sandbox deletion intent: %w", err)
		}
		record.DesiredState = SandboxDesiredStateTerminating
	}
	if firstCleanupRequest {
		if _, err := tx.Exec(ctx, `
			UPDATE manager.sandbox_runtime_claims
			SET phase = $2, lease_expires_at = NULL,
				cleanup_started_at = COALESCE(cleanup_started_at, NOW()), last_error = $3
			WHERE sandbox_id = $1
		`, record.ID, SandboxRuntimeClaimPhaseCleanupPending, reason); err != nil {
			return nil, fmt.Errorf("request Nomad sandbox claim cleanup: %w", err)
		}
		claim.Phase = SandboxRuntimeClaimPhaseCleanupPending
	}
	candidate, err := fenceSandboxClaimRuntimeSlotForCleanup(ctx, tx, record, claim)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sandbox claim cleanup request: %w", err)
	}
	return candidate, nil
}

// FenceSandboxRuntimeClaimForCleanup makes a due cleanup claim non-retryable
// and forces any exact physical slot into the terminal reconciler's due set.
func (s *PGSandboxStore) FenceSandboxRuntimeClaimForCleanup(
	ctx context.Context,
	sandboxID, operationID, reason string,
) (*SandboxClaimCleanupCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	operationID = strings.TrimSpace(operationID)
	reason = strings.TrimSpace(reason)
	if sandboxID == "" || operationID == "" || len(operationID) > 512 {
		return nil, fmt.Errorf("sandbox and operation identities are required")
	}
	if len(reason) > 2_048 {
		return nil, fmt.Errorf("sandbox claim cleanup reason exceeds 2048 bytes")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin fence sandbox claim cleanup tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRuntimeSlotClaimOperation(ctx, tx, operationID); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if claim.OperationID != operationID {
		return nil, fmt.Errorf("%w: cleanup operation identity changed", ErrSandboxClaimReservationConflict)
	}
	if claim.Phase == SandboxRuntimeClaimPhaseReady || claim.Phase == SandboxRuntimeClaimPhaseCleaned {
		return nil, nil
	}
	if claim.Phase == SandboxRuntimeClaimPhaseClaiming && claim.LeaseExpiresAt.After(claim.AuthorityNow) {
		return nil, nil
	}
	if claim.Phase == SandboxRuntimeClaimPhaseClaiming {
		if _, err := tx.Exec(ctx, `
			UPDATE manager.sandbox_runtime_claims
			SET phase = $2, lease_expires_at = NULL, cleanup_started_at = NOW(), last_error = $3
			WHERE sandbox_id = $1
		`, sandboxID, SandboxRuntimeClaimPhaseCleanupPending, reason); err != nil {
			return nil, fmt.Errorf("fence abandoned sandbox claim: %w", err)
		}
	}
	candidate, err := fenceSandboxClaimRuntimeSlotForCleanup(ctx, tx, record, claim)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sandbox claim cleanup fence: %w", err)
	}
	return candidate, nil
}

func abortConflictingSandboxLifecycleForClaimCleanup(
	ctx context.Context,
	tx pgx.Tx,
	record *SandboxRecord,
) error {
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE sandbox_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
		ORDER BY updated_at DESC
		LIMIT 1
		FOR UPDATE
	`, record.ID))
	if err != nil {
		return fmt.Errorf("lock sandbox lifecycle before claim cleanup: %w", err)
	}
	if lifecycle == nil {
		return nil
	}
	preserveTerminalWriter := lifecycle.Kind == SandboxLifecycleKindPause &&
		(lifecycle.Source == SandboxLifecycleSourceCrash ||
			lifecycle.Source == SandboxLifecycleSourceHealth ||
			lifecycle.Source == SandboxLifecycleSourceLost) &&
		!lifecycle.Cancelable && lifecycle.CancelRequestedAt.IsZero() &&
		(lifecycle.Phase == SandboxLifecyclePhasePublishing || lifecycle.Phase == SandboxLifecyclePhaseCommitting) &&
		lifecycle.PreparedHeadLayerID == "" &&
		lifecycle.FromGeneration == record.RuntimeGeneration &&
		lifecycle.FromPodNamespace == record.CurrentPodNamespace &&
		lifecycle.FromPodName == record.CurrentPodName
	if preserveTerminalWriter {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2, error = $3, aborted_at = NOW(), updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`, lifecycle.ID, SandboxLifecyclePhaseAborted, "sandbox termination requested"); err != nil {
		return fmt.Errorf("abort conflicting sandbox lifecycle for claim cleanup: %w", err)
	}
	return nil
}

func fenceSandboxClaimRuntimeSlotForCleanup(
	ctx context.Context,
	tx pgx.Tx,
	record *SandboxRecord,
	claim *SandboxRuntimeClaim,
) (*SandboxClaimCleanupCandidate, error) {
	candidate := &SandboxClaimCleanupCandidate{
		SandboxID: record.ID, OperationID: claim.OperationID,
		PhysicalStateRequired: !claim.CompletedAt.IsZero() ||
			record.CurrentPodName != "" || record.CurrentPodNamespace != "",
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE claim_operation_id = $1
		FOR UPDATE
	`, claim.OperationID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("lock sandbox cleanup runtime slot: %w", err)
	}
	if err == nil {
		if slot.SandboxID != record.ID ||
			(record.ClusterID != "" && slot.ClusterID != record.ClusterID) {
			return nil, fmt.Errorf("%w: cleanup slot is not bound to the sandbox record", ErrSandboxClaimReservationConflict)
		}
		if candidate.PhysicalStateRequired &&
			(record.CurrentPodName == "" || record.CurrentPodNamespace == "" ||
				slot.AllocationID != record.CurrentPodName ||
				slot.AllocationNamespace != record.CurrentPodNamespace) {
			return nil, fmt.Errorf("%w: cleanup allocation binding changed", ErrSandboxClaimReservationConflict)
		}
		candidate.SlotID = slot.ID
		candidate.SlotState = slot.State
		switch slot.State {
		case RuntimeSlotStateClaiming, RuntimeSlotStateStarting, RuntimeSlotStateActive:
			if _, err := tx.Exec(ctx, `
				UPDATE manager.runtime_slots
				SET state = $2, revision = revision + 1,
					heartbeat_expires_at = LEAST(heartbeat_expires_at, NOW()),
					quiescing_at = COALESCE(quiescing_at, NOW()), updated_at = NOW()
				WHERE slot_id = $1
			`, slot.ID, RuntimeSlotStateQuiescing); err != nil {
				return nil, fmt.Errorf("fence sandbox cleanup runtime slot: %w", err)
			}
			candidate.SlotState = RuntimeSlotStateQuiescing
		case RuntimeSlotStateQuiescing, RuntimeSlotStateOrphaned, RuntimeSlotStateTerminal:
		default:
			return nil, fmt.Errorf("%w: sandbox cleanup slot is %s", ErrRuntimeSlotInvalid, slot.State)
		}
	}
	return candidate, nil
}

// MarkSandboxRuntimeClaimCleaned closes the durable cleanup workflow only
// after the logical sandbox has been deleted.
func (s *PGSandboxStore) MarkSandboxRuntimeClaimCleaned(ctx context.Context, sandboxID, operationID string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin complete sandbox claim cleanup tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, strings.TrimSpace(sandboxID))
	if err != nil {
		return err
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, record.ID)
	if err != nil {
		return err
	}
	if claim.OperationID != strings.TrimSpace(operationID) {
		return fmt.Errorf("%w: cleanup operation identity changed", ErrSandboxClaimReservationConflict)
	}
	if claim.Phase == SandboxRuntimeClaimPhaseCleaned {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit cleaned sandbox claim retry: %w", err)
		}
		return nil
	}
	if claim.Phase != SandboxRuntimeClaimPhaseCleanupPending || record.DeletedAt.IsZero() || record.DesiredState != SandboxDesiredStateDeleted {
		return fmt.Errorf("%w: sandbox claim cleanup is not complete", ErrSandboxClaimReservationConflict)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET phase = $2, cleaned_at = NOW()
		WHERE sandbox_id = $1
	`, record.ID, SandboxRuntimeClaimPhaseCleaned); err != nil {
		return fmt.Errorf("mark sandbox runtime claim cleaned: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sandbox claim cleanup completion: %w", err)
	}
	return nil
}

func retrySandboxClaimTx(
	ctx context.Context,
	tx pgx.Tx,
	expected *SandboxRecord,
	operationID string,
	leaseTTL time.Duration,
) (*SandboxRecord, bool, error) {
	existing, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1
		FOR UPDATE
	`, expected.ID))
	if err != nil {
		return nil, false, fmt.Errorf("load retryable sandbox claim: %w", err)
	}
	if existing == nil {
		return nil, false, nil
	}
	if !sandboxClaimRecordMatches(existing, expected) {
		return nil, true, fmt.Errorf("%w: sandbox identity is bound to different claim inputs", ErrSandboxClaimReservationConflict)
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, existing.ID)
	if err != nil {
		return nil, true, err
	}
	if claim.OperationID != operationID {
		return nil, true, fmt.Errorf("%w: operation identity changed", ErrSandboxClaimReservationConflict)
	}
	switch claim.Phase {
	case SandboxRuntimeClaimPhaseReady:
		return existing, true, nil
	case SandboxRuntimeClaimPhaseClaiming:
		if _, err := tx.Exec(ctx, `
			UPDATE manager.sandbox_runtime_claims
			SET lease_expires_at = NOW() + ($2 * INTERVAL '1 millisecond'), last_error = ''
			WHERE sandbox_id = $1
		`, existing.ID, leaseTTL.Milliseconds()); err != nil {
			return nil, true, fmt.Errorf("renew sandbox claim retry lease: %w", err)
		}
		return existing, true, nil
	case SandboxRuntimeClaimPhaseCleanupPending, SandboxRuntimeClaimPhaseCleaned:
		return nil, true, fmt.Errorf("%w: claim is %s", ErrSandboxClaimCleanupPending, claim.Phase)
	default:
		return nil, true, fmt.Errorf("%w: unknown claim phase %s", ErrSandboxClaimReservationConflict, claim.Phase)
	}
}

func normalizeSandboxClaimInput(record *SandboxRecord, operationID string, leaseTTL time.Duration) (*SandboxRecord, string, time.Duration, error) {
	if record == nil {
		return nil, "", 0, fmt.Errorf("sandbox claim reservation record is required")
	}
	if record.ID == "" || record.ID != strings.TrimSpace(record.ID) {
		return nil, "", 0, fmt.Errorf("sandbox_id must be non-empty and canonical")
	}
	if record.TeamID == "" || record.TeamID != strings.TrimSpace(record.TeamID) {
		return nil, "", 0, fmt.Errorf("team_id must be non-empty and canonical")
	}
	if record.RuntimeBackend != SandboxRuntimeBackendNomad {
		return nil, "", 0, fmt.Errorf("sandbox claim reservation requires the Nomad runtime backend")
	}
	if record.DesiredState != SandboxDesiredStateActive || !record.DeletedAt.IsZero() {
		return nil, "", 0, fmt.Errorf("sandbox claim reservation requires a live active record")
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" || len(operationID) > 512 {
		return nil, "", 0, fmt.Errorf("operation_id is required and must not exceed 512 bytes")
	}
	if leaseTTL < time.Second || leaseTTL > time.Minute {
		return nil, "", 0, fmt.Errorf("sandbox claim lease TTL must be between 1s and 1m")
	}
	return record, operationID, time.Duration(leaseTTL.Milliseconds()) * time.Millisecond, nil
}

func sandboxClaimRecordMatches(actual, expected *SandboxRecord) bool {
	return actual != nil && expected != nil && actual.DeletedAt.IsZero() &&
		actual.ID == expected.ID && actual.TeamID == expected.TeamID && actual.UserID == expected.UserID &&
		actual.TemplateID == expected.TemplateID && actual.ClusterID == expected.ClusterID &&
		actual.RuntimeBackend == SandboxRuntimeBackendNomad &&
		actual.DesiredState == SandboxDesiredStateActive && actual.RuntimeGeneration == expected.RuntimeGeneration &&
		apiequality.Semantic.DeepEqual(actual.Config, expected.Config) &&
		apiequality.Semantic.DeepEqual(actual.TemplateSpec, expected.TemplateSpec)
}

func validateCompleteSandboxClaimRequest(request *CompleteSandboxClaimRequest) error {
	if request == nil {
		return fmt.Errorf("complete sandbox claim request is required")
	}
	fields := map[string]string{
		"sandbox_id": request.SandboxID, "operation_id": request.OperationID,
		"slot_id": request.SlotID, "allocation_id": request.AllocationID,
		"allocation_namespace": request.AllocationNamespace,
	}
	for name, value := range fields {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
			return fmt.Errorf("%s is required, canonical, and at most 512 bytes", name)
		}
	}
	return nil
}

func lockNomadSandboxClaimRecord(ctx context.Context, tx pgx.Tx, sandboxID string) (*SandboxRecord, error) {
	record, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1
		FOR UPDATE
	`, sandboxID))
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	if record.RuntimeBackend != SandboxRuntimeBackendNomad {
		return nil, fmt.Errorf("%w: sandbox is not Nomad-backed", ErrSandboxClaimReservationConflict)
	}
	return record, nil
}

func lockSandboxRuntimeClaim(ctx context.Context, tx pgx.Tx, sandboxID string) (*SandboxRuntimeClaim, error) {
	claim, err := scanSandboxRuntimeClaim(tx.QueryRow(ctx, sandboxRuntimeClaimSelectSQL()+`
		WHERE sandbox_id = $1
		FOR UPDATE
	`, sandboxID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: sandbox runtime claim is missing", ErrSandboxClaimReservationConflict)
	}
	return claim, err
}

func sandboxRuntimeClaimSelectSQL() string {
	return `
		SELECT sandbox_id, operation_id, phase, lease_expires_at, last_error,
			completed_at, cleanup_started_at, cleaned_at, created_at, updated_at, NOW()
		FROM manager.sandbox_runtime_claims`
}

type sandboxRuntimeClaimScanner interface {
	Scan(...any) error
}

func scanSandboxRuntimeClaim(scanner sandboxRuntimeClaimScanner) (*SandboxRuntimeClaim, error) {
	var claim SandboxRuntimeClaim
	var lease, completed, cleanupStarted, cleaned pgtype.Timestamptz
	if err := scanner.Scan(
		&claim.SandboxID, &claim.OperationID, &claim.Phase, &lease, &claim.LastError,
		&completed, &cleanupStarted, &cleaned, &claim.CreatedAt, &claim.UpdatedAt, &claim.AuthorityNow,
	); err != nil {
		return nil, err
	}
	if lease.Valid {
		claim.LeaseExpiresAt = lease.Time
	}
	if completed.Valid {
		claim.CompletedAt = completed.Time
	}
	if cleanupStarted.Valid {
		claim.CleanupStartedAt = cleanupStarted.Time
	}
	if cleaned.Valid {
		claim.CleanedAt = cleaned.Time
	}
	return &claim, nil
}

func mapSandboxClaimConflict(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503", "23505", "23514", "40001":
			return fmt.Errorf("%s: %w", operation, ErrSandboxClaimReservationConflict)
		}
	}
	return err
}
