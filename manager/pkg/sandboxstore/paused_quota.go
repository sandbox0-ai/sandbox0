package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

var ErrPausedSandboxQuotaExceeded = errors.New("paused sandbox quota exceeded")

type PausedSandboxQuotaExceededError struct {
	TeamID  string
	Current int64
	Limit   int64
}

func (e *PausedSandboxQuotaExceededError) Error() string {
	return fmt.Sprintf("%s for team %s: current %d, limit %d; resume or delete paused sandboxes, or request a free quota increase",
		ErrPausedSandboxQuotaExceeded, e.TeamID, e.Current, e.Limit)
}

func (e *PausedSandboxQuotaExceededError) Unwrap() error { return ErrPausedSandboxQuotaExceeded }

// Pending running-memory forks reserve their child before capture starts. A
// retry or capture handoff must not be rejected by a later policy change.
const pausedSandboxUsageSQL = `
	SELECT (
		SELECT COUNT(*) FROM manager.sandboxes
		WHERE team_id=$1 AND deleted_at IS NULL AND desired_state='paused'
	) + (
		SELECT COUNT(*) FROM manager.sandbox_runtime_running_memory_forks f
		JOIN manager.sandboxes s ON s.sandbox_id=f.source_sandbox_id
		JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=f.capture_operation_id
		WHERE s.team_id=$1 AND s.deleted_at IS NULL
		AND s.desired_state IN ('active','paused')
		AND l.phase IN ('preparing','barriered','publishing','committing','committed')
		AND NOT EXISTS (SELECT 1 FROM manager.sandboxes child WHERE child.sandbox_id=f.target_sandbox_id)
	)`

// CountPausedSandboxes uses the same regional PostgreSQL authority as admission,
// including accepted memory-fork reservations, rather than delayed metering.
func (s *PGSandboxStore) CountPausedSandboxes(ctx context.Context, teamID string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("sandbox store is not configured")
	}
	var current int64
	err := s.pool.QueryRow(ctx, pausedSandboxUsageSQL, strings.TrimSpace(teamID)).Scan(&current)
	return current, err
}

// Caller holds lockActiveSandboxQuotaTeam before any sandbox row lock. This
// guard applies only to new identities; pause/resume/delete remain available.
func checkPausedSandboxAdmissionTx(ctx context.Context, tx pgx.Tx, teamID string) error {
	limit, err := quota.NewRepository(tx).GetLimit(ctx, teamID, quota.DimensionPausedSandboxes)
	if err != nil {
		return fmt.Errorf("load paused sandbox quota: %w", err)
	}
	if limit == nil {
		return nil
	}
	var current int64
	if err := tx.QueryRow(ctx, pausedSandboxUsageSQL, teamID).Scan(&current); err != nil {
		return fmt.Errorf("count paused sandbox quota: %w", err)
	}
	if current >= limit.LimitValue {
		return &PausedSandboxQuotaExceededError{TeamID: teamID, Current: current, Limit: limit.LimitValue}
	}
	return nil
}
