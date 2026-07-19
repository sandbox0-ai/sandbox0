package teamquota

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// AdmitTeamMutationTx holds a shared team admission fence for the lifetime of
// tx. Team-owned business rows that are not represented by a capacity
// allocation must call this before creating or replacing state.
func AdmitTeamMutationTx(ctx context.Context, tx pgx.Tx, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if tx == nil {
		return &UnavailableError{
			Operation: "admit team mutation",
			Err:       fmt.Errorf("database transaction is not configured"),
		}
	}
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO quota.team_states (team_id)
		VALUES ($1)
		ON CONFLICT (team_id) DO NOTHING
	`, teamID); err != nil {
		return &UnavailableError{Operation: "admit team mutation", Err: err}
	}
	var disabled bool
	if err := tx.QueryRow(ctx, `
		SELECT admission_disabled OR deleted_at IS NOT NULL
		FROM quota.team_states
		WHERE team_id = $1
		FOR SHARE
	`, teamID).Scan(&disabled); err != nil {
		return &UnavailableError{Operation: "admit team mutation", Err: err}
	}
	if disabled {
		return &UnavailableError{
			Operation: "admit team mutation",
			Err:       &TeamAdmissionDisabledError{TeamID: teamID},
		}
	}
	return nil
}

// LockTeamMutationTx exclusively serializes a team-owned business lifecycle
// transition with quota reservations. Use it when a durable cleanup fence must
// stay at the current target rather than immediately changing capacity.
func LockTeamMutationTx(ctx context.Context, tx pgx.Tx, teamID string) error {
	if tx == nil {
		return &UnavailableError{
			Operation: "lock team mutation",
			Err:       fmt.Errorf("database transaction is not configured"),
		}
	}
	if err := lockTeam(ctx, tx, teamID); err != nil {
		return err
	}
	return nil
}
