package teamquota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const maxDeletedTeamTombstoneBatchSize = 1000

// DeletedTeamTombstone is a stable pagination cursor for retained deletion
// state. DeletedAt is immutable after finalization.
type DeletedTeamTombstone struct {
	TeamID    string
	DeletedAt time.Time
}

// ListDeletedTeamTombstones returns admission tombstones old enough for an
// identity-aware caller to inspect. The caller must independently prove that
// the team identity no longer exists before requesting deletion.
func (r *Repository) ListDeletedTeamTombstones(
	ctx context.Context,
	deletedBefore time.Time,
	after *DeletedTeamTombstone,
	limit int,
) ([]DeletedTeamTombstone, error) {
	if r == nil || r.pool == nil {
		return nil, &UnavailableError{
			Operation: "list deleted team quota tombstones",
			Err:       fmt.Errorf("database pool is not configured"),
		}
	}
	if deletedBefore.IsZero() {
		return nil, fmt.Errorf("deleted-before cutoff is required")
	}
	if limit <= 0 || limit > maxDeletedTeamTombstoneBatchSize {
		return nil, fmt.Errorf(
			"deleted team tombstone batch size must be between 1 and %d",
			maxDeletedTeamTombstoneBatchSize,
		)
	}

	var afterDeletedAt any
	afterTeamID := ""
	if after != nil {
		if after.DeletedAt.IsZero() || strings.TrimSpace(after.TeamID) == "" {
			return nil, fmt.Errorf("deleted team tombstone cursor is incomplete")
		}
		afterDeletedAt = after.DeletedAt.UTC()
		afterTeamID = strings.TrimSpace(after.TeamID)
	}
	rows, err := r.pool.Query(ctx, `
		SELECT team_id
			, deleted_at
		FROM quota.team_states
		WHERE admission_disabled
			AND deleted_at IS NOT NULL
			AND deleted_at < $1
			AND (
				$2::timestamptz IS NULL
				OR (deleted_at, team_id) > ($2::timestamptz, $3)
			)
		ORDER BY deleted_at, team_id
		LIMIT $4
	`, deletedBefore.UTC(), afterDeletedAt, afterTeamID, limit)
	if err != nil {
		return nil, &UnavailableError{
			Operation: "list deleted team quota tombstones",
			Err:       err,
		}
	}
	defer rows.Close()

	tombstones := make([]DeletedTeamTombstone, 0, limit)
	for rows.Next() {
		var tombstone DeletedTeamTombstone
		if err := rows.Scan(&tombstone.TeamID, &tombstone.DeletedAt); err != nil {
			return nil, &UnavailableError{
				Operation: "scan deleted team quota tombstone",
				Err:       err,
			}
		}
		tombstone.DeletedAt = tombstone.DeletedAt.UTC()
		tombstones = append(tombstones, tombstone)
	}
	if err := rows.Err(); err != nil {
		return nil, &UnavailableError{
			Operation: "iterate deleted team quota tombstones",
			Err:       err,
		}
	}
	return tombstones, nil
}

// PruneDeletedTeamTombstone removes one old tombstone after its caller has
// proved that the identity directory no longer contains the team. It rechecks
// the age and every quota reference under the team row lock. A false result
// means the tombstone was absent, too new, or no longer eligible.
func (r *Repository) PruneDeletedTeamTombstone(
	ctx context.Context,
	teamID string,
	deletedBefore time.Time,
) (bool, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return false, fmt.Errorf("team_id is required")
	}
	if deletedBefore.IsZero() {
		return false, fmt.Errorf("deleted-before cutoff is required")
	}

	pruned := false
	err := r.inTx(ctx, "prune deleted team quota tombstone", func(tx pgx.Tx) error {
		var eligible bool
		err := tx.QueryRow(ctx, `
			SELECT admission_disabled
				AND deleted_at IS NOT NULL
				AND deleted_at < $2
			FROM quota.team_states
			WHERE team_id = $1
			FOR UPDATE
		`, teamID, deletedBefore.UTC()).Scan(&eligible)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return &UnavailableError{
				Operation: "lock deleted team quota tombstone",
				Err:       err,
			}
		}
		if !eligible {
			return nil
		}

		var hasReferences bool
		if err := tx.QueryRow(ctx, `
			SELECT
				EXISTS (
					SELECT 1 FROM quota.team_policies
					WHERE team_id = $1
				)
				OR EXISTS (
					SELECT 1 FROM quota.team_usage
					WHERE team_id = $1
				)
				OR EXISTS (
					SELECT 1 FROM quota.allocations
					WHERE team_id = $1
				)
				OR EXISTS (
					SELECT 1 FROM quota.transfer_operations
					WHERE team_id = $1
				)
		`, teamID).Scan(&hasReferences); err != nil {
			return &UnavailableError{
				Operation: "verify deleted team quota tombstone references",
				Err:       err,
			}
		}
		if hasReferences {
			return &UnavailableError{
				Operation: "prune deleted team quota tombstone",
				Err:       fmt.Errorf("team quota references still exist"),
			}
		}

		result, err := tx.Exec(ctx, `
			DELETE FROM quota.team_states
			WHERE team_id = $1
				AND admission_disabled
				AND deleted_at IS NOT NULL
				AND deleted_at < $2
		`, teamID, deletedBefore.UTC())
		if err != nil {
			return &UnavailableError{
				Operation: "delete team quota tombstone",
				Err:       err,
			}
		}
		pruned = result.RowsAffected() == 1
		return nil
	})
	return pruned, err
}
