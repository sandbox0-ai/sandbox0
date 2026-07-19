package teamquota

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// TeamAdmissionDisabledError reports an active durable team tombstone. Resource
// admission must fail closed when this error is present.
type TeamAdmissionDisabledError struct {
	TeamID string
}

func (e *TeamAdmissionDisabledError) Error() string {
	if e == nil || strings.TrimSpace(e.TeamID) == "" {
		return "team quota admission is disabled"
	}
	return fmt.Sprintf("team quota admission is disabled for team %s", e.TeamID)
}

// IsTeamAdmissionDisabled reports whether err contains a durable admission
// tombstone.
func IsTeamAdmissionDisabled(err error) bool {
	var disabled *TeamAdmissionDisabledError
	return errors.As(err, &disabled)
}

// TeamDeletionConflictError reports quota state that must be released before
// admission can be disabled.
type TeamDeletionConflictError struct {
	TeamID            string
	LiveAllocations   int64
	PreparedTransfers int64
	NonzeroUsageRows  int64
}

func (e *TeamDeletionConflictError) Error() string {
	if e == nil {
		return "team quota deletion conflict"
	}
	return fmt.Sprintf(
		"team %s still has quota state: live_allocations=%d prepared_transfers=%d nonzero_usage_rows=%d",
		e.TeamID,
		e.LiveAllocations,
		e.PreparedTransfers,
		e.NonzeroUsageRows,
	)
}

// IsDeletionConflict reports whether quota state still blocks team deletion.
func IsDeletionConflict(err error) bool {
	var conflict *TeamDeletionConflictError
	return errors.As(err, &conflict)
}

// TeamAdmissionDisabled returns the durable admission state for a team.
// Missing state means the team has not consumed quota yet and is not disabled.
func (r *Repository) TeamAdmissionDisabled(ctx context.Context, teamID string) (bool, error) {
	if r == nil || r.pool == nil {
		return false, &UnavailableError{
			Operation: "get team admission state",
			Err:       fmt.Errorf("database pool is not configured"),
		}
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return false, fmt.Errorf("team_id is required")
	}
	return teamAdmissionDisabled(ctx, r.pool, teamID)
}

// DisableTeamAdmission atomically locks the team, proves no quota allocation
// or prepared transfer is live, and creates a durable admission tombstone.
func (r *Repository) DisableTeamAdmission(ctx context.Context, teamID string) error {
	return r.DisableTeamAdmissionWithFinalCheck(ctx, teamID, nil)
}

// DisableTeamAdmissionWithFinalCheck drains transactions holding a shared team
// mutation fence, runs finalCheck while the exclusive fence is held, and then
// creates the durable admission tombstone. Returning an error from
// finalCheck rolls the tombstone transaction back.
func (r *Repository) DisableTeamAdmissionWithFinalCheck(
	ctx context.Context,
	teamID string,
	finalCheck func(context.Context) error,
) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	return r.inTx(ctx, "disable team admission", func(tx pgx.Tx) error {
		state, err := lockTeamLifecycle(ctx, tx, teamID)
		if err != nil {
			return err
		}
		if state.admissionDisabled || state.deleted {
			return nil
		}
		if err := ensureTeamQuotaDeletionReady(ctx, tx, teamID); err != nil {
			return err
		}
		if finalCheck != nil {
			if err := finalCheck(ctx); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE quota.team_states
			SET admission_disabled = TRUE,
				revision = revision + 1,
				updated_at = NOW()
			WHERE team_id = $1
		`, teamID); err != nil {
			return &UnavailableError{Operation: "disable team admission", Err: err}
		}
		return nil
	})
}

// FinalizeTeamDeletion removes mutable quota state while retaining the durable
// team_states tombstone. A separate identity-aware retention worker may prune
// it after all previously issued access tokens have expired. It is safe to
// retry after identity deletion fails.
func (r *Repository) FinalizeTeamDeletion(ctx context.Context, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	return r.inTx(ctx, "finalize team deletion", func(tx pgx.Tx) error {
		state, err := lockTeamLifecycle(ctx, tx, teamID)
		if err != nil {
			return err
		}
		if !state.admissionDisabled && !state.deleted {
			return &UnavailableError{
				Operation: "finalize team deletion",
				Err:       fmt.Errorf("team admission has not been disabled"),
			}
		}
		if err := ensureTeamQuotaDeletionReady(ctx, tx, teamID); err != nil {
			return err
		}
		for _, statement := range []string{
			`DELETE FROM quota.transfer_operations WHERE team_id = $1`,
			`DELETE FROM quota.allocations WHERE team_id = $1`,
			`DELETE FROM quota.team_usage WHERE team_id = $1`,
			`DELETE FROM quota.team_policies WHERE team_id = $1`,
		} {
			if _, err := tx.Exec(ctx, statement, teamID); err != nil {
				return &UnavailableError{Operation: "finalize team deletion", Err: err}
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE quota.team_states
			SET admission_disabled = TRUE,
				deleted_at = COALESCE(deleted_at, NOW()),
				revision = revision + CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END,
				updated_at = NOW()
			WHERE team_id = $1
		`, teamID); err != nil {
			return &UnavailableError{Operation: "finalize team deletion", Err: err}
		}
		return nil
	})
}

type teamLifecycleState struct {
	admissionDisabled bool
	deleted           bool
}

func lockTeamLifecycle(ctx context.Context, tx pgx.Tx, teamID string) (teamLifecycleState, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO quota.team_states (team_id)
		VALUES ($1)
		ON CONFLICT (team_id) DO NOTHING
	`, teamID); err != nil {
		return teamLifecycleState{}, &UnavailableError{Operation: "lock team deletion", Err: err}
	}
	var state teamLifecycleState
	if err := tx.QueryRow(ctx, `
		SELECT admission_disabled, deleted_at IS NOT NULL
		FROM quota.team_states
		WHERE team_id = $1
		FOR UPDATE
	`, teamID).Scan(&state.admissionDisabled, &state.deleted); err != nil {
		return teamLifecycleState{}, &UnavailableError{Operation: "lock team deletion", Err: err}
	}
	return state, nil
}

func ensureTeamQuotaDeletionReady(ctx context.Context, tx pgx.Tx, teamID string) error {
	var conflict TeamDeletionConflictError
	conflict.TeamID = teamID
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM quota.allocations a
		WHERE a.team_id = $1
			AND (
				a.state <> 'released'
				OR a.operation_id IS NOT NULL
				OR EXISTS (
					SELECT 1
					FROM quota.allocation_items i
					WHERE i.allocation_id = a.allocation_id
						AND (
							i.committed_value > 0
							OR COALESCE(i.pending_value, 0) > 0
						)
				)
			)
	`, teamID).Scan(&conflict.LiveAllocations); err != nil {
		return &UnavailableError{Operation: "verify team deletion allocations", Err: err}
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM quota.transfer_operations
		WHERE team_id = $1 AND state = 'prepared'
	`, teamID).Scan(&conflict.PreparedTransfers); err != nil {
		return &UnavailableError{Operation: "verify team deletion transfers", Err: err}
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM quota.team_usage
		WHERE team_id = $1
			AND (committed_value > 0 OR reserved_value > 0)
	`, teamID).Scan(&conflict.NonzeroUsageRows); err != nil {
		return &UnavailableError{Operation: "verify team deletion usage", Err: err}
	}
	if conflict.LiveAllocations > 0 ||
		conflict.PreparedTransfers > 0 ||
		conflict.NonzeroUsageRows > 0 {
		return &conflict
	}
	return nil
}

func teamAdmissionDisabled(ctx context.Context, query rowQuerier, teamID string) (bool, error) {
	var disabled bool
	err := query.QueryRow(ctx, `
		SELECT admission_disabled OR deleted_at IS NOT NULL
		FROM quota.team_states
		WHERE team_id = $1
	`, teamID).Scan(&disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, &UnavailableError{Operation: "get team admission state", Err: err}
	}
	return disabled, nil
}
