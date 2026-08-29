package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	minRuntimeRecoveryLeaseDuration = time.Second
	maxRuntimeRecoveryLeaseDuration = 10 * time.Minute
	minRuntimeRecoveryRetryDelay    = 100 * time.Millisecond
	maxRuntimeRecoveryRetryDelay    = time.Hour
	maxRuntimeRecoveryErrorBytes    = 4096
)

var ErrSandboxRuntimeRecoveryClaimLost = errors.New("sandbox runtime recovery claim lost")

// SandboxRuntimeRecoveryClaim is an exclusive, renewable scheduling lease on
// the lifecycle transaction that created one durable recovery obligation.
type SandboxRuntimeRecoveryClaim struct {
	SandboxID      string
	LifecycleTxnID string
	WorkerID       string
	Token          string
	AttemptCount   int
	ClaimedUntil   time.Time
}

// ClaimSandboxRuntimeRecovery serializes automatic reconstruction across all
// manager replicas without creating a second source of recovery truth.
func (s *PGSandboxStore) ClaimSandboxRuntimeRecovery(
	ctx context.Context,
	sandboxID, workerID string,
	leaseDuration time.Duration,
) (*SandboxRuntimeRecoveryClaim, error) {
	sandboxID, workerID, err := normalizeRuntimeRecoveryIdentity(sandboxID, workerID)
	if err != nil {
		return nil, err
	}
	if err := validateRuntimeRecoveryDuration(
		leaseDuration, minRuntimeRecoveryLeaseDuration, maxRuntimeRecoveryLeaseDuration, "lease duration",
	); err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin sandbox runtime recovery claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var desiredState string
	var lifecycleEpoch int64
	err = tx.QueryRow(ctx, `
		SELECT desired_state, lifecycle_epoch
		FROM manager.sandboxes
		WHERE sandbox_id = $1 AND deleted_at IS NULL
		FOR UPDATE
	`, sandboxID).Scan(&desiredState, &lifecycleEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit absent sandbox runtime recovery claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock sandbox runtime recovery identity: %w", err)
	}

	authority, err := lockSandboxRuntimeRecoveryAuthority(ctx, tx, sandboxID, lifecycleEpoch)
	if err != nil {
		return nil, err
	}
	if authority == nil || !authority.authorizes(desiredState) ||
		authority.nextAttemptAt.After(authority.now) ||
		(authority.claimedUntil != nil && authority.claimedUntil.After(authority.now)) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit unavailable sandbox runtime recovery claim: %w", err)
		}
		return nil, nil
	}
	if authority.attempts >= 1000000 {
		return nil, fmt.Errorf("sandbox runtime recovery attempt limit reached for %s", sandboxID)
	}

	token := uuid.NewString()
	claim := &SandboxRuntimeRecoveryClaim{
		SandboxID: sandboxID, LifecycleTxnID: authority.txnID,
		WorkerID: workerID, Token: token,
	}
	err = tx.QueryRow(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET recovery_attempts = recovery_attempts + 1,
			recovery_claimed_by = $2,
			recovery_claim_token = $3,
			recovery_claimed_until = NOW() + ($4::double precision * INTERVAL '1 millisecond'),
			recovery_last_error = ''
		WHERE txn_id = $1
			AND recovery_next_attempt_at <= NOW()
			AND (recovery_claimed_until IS NULL OR recovery_claimed_until <= NOW())
		RETURNING recovery_attempts, recovery_claimed_until
	`, authority.txnID, workerID, token, leaseDuration.Milliseconds()).Scan(
		&claim.AttemptCount, &claim.ClaimedUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit raced sandbox runtime recovery claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim sandbox runtime recovery: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit sandbox runtime recovery claim: %w", err)
	}
	return claim, nil
}

// RenewSandboxRuntimeRecoveryClaim keeps a live reconstruction exclusive.
func (s *PGSandboxStore) RenewSandboxRuntimeRecoveryClaim(
	ctx context.Context,
	claim *SandboxRuntimeRecoveryClaim,
	leaseDuration time.Duration,
) error {
	if err := validateSandboxRuntimeRecoveryClaim(claim); err != nil {
		return err
	}
	if err := validateRuntimeRecoveryDuration(
		leaseDuration, minRuntimeRecoveryLeaseDuration, maxRuntimeRecoveryLeaseDuration, "lease duration",
	); err != nil {
		return err
	}
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET recovery_claimed_until = NOW() + ($4::double precision * INTERVAL '1 millisecond')
		WHERE txn_id = $1 AND sandbox_id = $2
			AND recovery_claimed_by = $3 AND recovery_claim_token = $5
			AND recovery_claimed_until > NOW()
	`, claim.LifecycleTxnID, claim.SandboxID, claim.WorkerID, leaseDuration.Milliseconds(), claim.Token)
	if err != nil {
		return fmt.Errorf("renew sandbox runtime recovery claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrSandboxRuntimeRecoveryClaimLost
	}
	return nil
}

// FailSandboxRuntimeRecoveryClaim releases a failed attempt with a durable
// not-before time shared by every manager replica.
func (s *PGSandboxStore) FailSandboxRuntimeRecoveryClaim(
	ctx context.Context,
	claim *SandboxRuntimeRecoveryClaim,
	retryDelay time.Duration,
	reason string,
) error {
	if err := validateSandboxRuntimeRecoveryClaim(claim); err != nil {
		return err
	}
	if err := validateRuntimeRecoveryDuration(
		retryDelay, minRuntimeRecoveryRetryDelay, maxRuntimeRecoveryRetryDelay, "retry delay",
	); err != nil {
		return err
	}
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	reason = boundRuntimeRecoveryError(reason)
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET recovery_next_attempt_at = NOW() + ($5::double precision * INTERVAL '1 millisecond'),
			recovery_claimed_by = '', recovery_claim_token = '', recovery_claimed_until = NULL,
			recovery_last_error = $6
		WHERE txn_id = $1 AND sandbox_id = $2
			AND recovery_claimed_by = $3 AND recovery_claim_token = $4
			AND recovery_claimed_until > NOW()
	`, claim.LifecycleTxnID, claim.SandboxID, claim.WorkerID, claim.Token,
		retryDelay.Milliseconds(), reason)
	if err != nil {
		return fmt.Errorf("fail sandbox runtime recovery claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrSandboxRuntimeRecoveryClaimLost
	}
	return nil
}

// CompleteSandboxRuntimeRecoveryClaim releases scheduling ownership after the
// obligation was reconstructed or durably superseded.
func (s *PGSandboxStore) CompleteSandboxRuntimeRecoveryClaim(
	ctx context.Context,
	claim *SandboxRuntimeRecoveryClaim,
) error {
	if err := validateSandboxRuntimeRecoveryClaim(claim); err != nil {
		return err
	}
	if s == nil || s.pool == nil {
		return fmt.Errorf("sandbox store is not configured")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET recovery_next_attempt_at = NOW(),
			recovery_claimed_by = '', recovery_claim_token = '', recovery_claimed_until = NULL,
			recovery_last_error = ''
		WHERE txn_id = $1 AND sandbox_id = $2
			AND recovery_claimed_by = $3 AND recovery_claim_token = $4
			AND recovery_claimed_until > NOW()
	`, claim.LifecycleTxnID, claim.SandboxID, claim.WorkerID, claim.Token)
	if err != nil {
		return fmt.Errorf("complete sandbox runtime recovery claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrSandboxRuntimeRecoveryClaimLost
	}
	return nil
}

type sandboxRuntimeRecoveryAuthority struct {
	txnID           string
	kind            string
	phase           string
	source          string
	attempts        int
	nextAttemptAt   time.Time
	claimedUntil    *time.Time
	now             time.Time
	activeAuthority bool
}

func (a *sandboxRuntimeRecoveryAuthority) authorizes(desiredState string) bool {
	if a == nil || a.kind != SandboxLifecycleKindPause || !sandboxLifecycleSourceRequiresRecovery(a.source) {
		return false
	}
	if a.activeAuthority {
		return desiredState == SandboxDesiredStateActive || desiredState == SandboxDesiredStatePaused
	}
	return desiredState == SandboxDesiredStatePaused &&
		(a.phase == SandboxLifecyclePhaseCommitted ||
			(a.phase == SandboxLifecyclePhaseAborted))
}

func lockSandboxRuntimeRecoveryAuthority(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID string,
	lifecycleEpoch int64,
) (*sandboxRuntimeRecoveryAuthority, error) {
	var authority sandboxRuntimeRecoveryAuthority
	err := tx.QueryRow(ctx, `
		SELECT txn_id, kind, phase, source, recovery_attempts,
			recovery_next_attempt_at, recovery_claimed_until, NOW(),
			(epoch = $2 AND kind = $3 AND source IN ($4, $5, $6)
				AND phase IN ($7, $8, $9, $10)) AS active_authority
		FROM manager.sandbox_lifecycle_txns
		WHERE sandbox_id = $1
			AND (
				(epoch = $2 AND kind = $3 AND source IN ($4, $5, $6)
					AND phase IN ($7, $8, $9, $10))
				OR phase = $11
				OR (phase = $12 AND kind = $3 AND source IN ($4, $5, $6) AND error = $13)
			)
		ORDER BY
			(epoch = $2 AND kind = $3 AND source IN ($4, $5, $6)
				AND phase IN ($7, $8, $9, $10)) DESC,
			epoch DESC
		LIMIT 1
		FOR UPDATE
	`, sandboxID, lifecycleEpoch, SandboxLifecycleKindPause,
		SandboxLifecycleSourceCrash, SandboxLifecycleSourceHealth, SandboxLifecycleSourceLost,
		SandboxLifecyclePhasePreparing, SandboxLifecyclePhaseBarriered,
		SandboxLifecyclePhasePublishing, SandboxLifecyclePhaseCommitting,
		SandboxLifecyclePhaseCommitted, SandboxLifecyclePhaseAborted,
		RootFSWriterCrashAbandonReason,
	).Scan(
		&authority.txnID, &authority.kind, &authority.phase, &authority.source, &authority.attempts,
		&authority.nextAttemptAt, &authority.claimedUntil, &authority.now, &authority.activeAuthority,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock sandbox runtime recovery authority: %w", err)
	}
	return &authority, nil
}

func sandboxLifecycleSourceRequiresRecovery(source string) bool {
	return source == SandboxLifecycleSourceCrash ||
		source == SandboxLifecycleSourceHealth ||
		source == SandboxLifecycleSourceLost
}

func normalizeRuntimeRecoveryIdentity(sandboxID, workerID string) (string, string, error) {
	normalizedSandboxID := strings.TrimSpace(sandboxID)
	normalizedWorkerID := strings.TrimSpace(workerID)
	if normalizedSandboxID == "" || normalizedSandboxID != sandboxID || len(normalizedSandboxID) > 512 {
		return "", "", fmt.Errorf("sandbox ID must be canonical and at most 512 bytes")
	}
	if normalizedWorkerID == "" || normalizedWorkerID != workerID || len(normalizedWorkerID) > 256 {
		return "", "", fmt.Errorf("runtime recovery worker ID must be canonical and at most 256 bytes")
	}
	return normalizedSandboxID, normalizedWorkerID, nil
}

func validateSandboxRuntimeRecoveryClaim(claim *SandboxRuntimeRecoveryClaim) error {
	if claim == nil {
		return fmt.Errorf("sandbox runtime recovery claim is required")
	}
	if _, _, err := normalizeRuntimeRecoveryIdentity(claim.SandboxID, claim.WorkerID); err != nil {
		return err
	}
	if strings.TrimSpace(claim.LifecycleTxnID) == "" || claim.LifecycleTxnID != strings.TrimSpace(claim.LifecycleTxnID) ||
		len(claim.LifecycleTxnID) > 512 {
		return fmt.Errorf("runtime recovery lifecycle txn ID must be canonical and at most 512 bytes")
	}
	parsed, err := uuid.Parse(claim.Token)
	if err != nil || parsed.String() != claim.Token {
		return fmt.Errorf("runtime recovery claim token must be a canonical UUID")
	}
	return nil
}

func validateRuntimeRecoveryDuration(value, minimum, maximum time.Duration, name string) error {
	if value < minimum || value > maximum || value.Milliseconds() <= 0 {
		return fmt.Errorf("runtime recovery %s must be between %s and %s", name, minimum, maximum)
	}
	return nil
}

func boundRuntimeRecoveryError(message string) string {
	if len(message) <= maxRuntimeRecoveryErrorBytes {
		return message
	}
	bounded := []byte(message)[:maxRuntimeRecoveryErrorBytes]
	for len(bounded) > 0 && !utf8.Valid(bounded) {
		bounded = bounded[:len(bounded)-1]
	}
	return string(bounded)
}
