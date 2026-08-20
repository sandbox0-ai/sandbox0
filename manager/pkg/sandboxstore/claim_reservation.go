package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

var ErrActiveSandboxQuotaExceeded = errors.New("active sandbox quota exceeded")
var ErrSandboxClaimReservationConflict = errors.New("sandbox claim reservation conflict")

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

// ReserveSandboxClaimRequest is the complete admission input for a new
// logical sandbox. A nil limit means unlimited admission.
type ReserveSandboxClaimRequest struct {
	Record             *SandboxRecord
	ActiveSandboxLimit *int64
}

// ReserveSandboxClaim serializes claims per team and creates the logical
// sandbox in the same transaction as the active-sandbox quota decision. An
// existing deterministic sandbox ID is returned before quota evaluation so an
// exact retry never consumes or requires another quota slot.
func (s *PGSandboxStore) ReserveSandboxClaim(ctx context.Context, request *ReserveSandboxClaimRequest) (*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if request == nil || request.Record == nil {
		return nil, fmt.Errorf("sandbox claim reservation record is required")
	}
	record := request.Record
	if record.ID == "" || record.ID != strings.TrimSpace(record.ID) {
		return nil, fmt.Errorf("sandbox_id must be non-empty and canonical")
	}
	if record.TeamID == "" || record.TeamID != strings.TrimSpace(record.TeamID) {
		return nil, fmt.Errorf("team_id must be non-empty and canonical")
	}
	if record.DesiredState != SandboxDesiredStateActive || !record.DeletedAt.IsZero() {
		return nil, fmt.Errorf("sandbox claim reservation requires a live active record")
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

	// The prefix gives team quota admission its own advisory-lock namespace.
	// Hash collisions only serialize unrelated teams and cannot weaken safety.
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, "sandbox-claim/team/"+record.TeamID); err != nil {
		return nil, fmt.Errorf("lock team sandbox claims: %w", err)
	}
	existing, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1
		FOR UPDATE
	`, record.ID))
	if err != nil {
		return nil, fmt.Errorf("load reserved sandbox claim: %w", err)
	}
	if existing != nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit sandbox claim retry: %w", err)
		}
		return existing, nil
	}

	if request.ActiveSandboxLimit != nil {
		var current int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM manager.sandboxes
			WHERE team_id = $1
				AND deleted_at IS NULL
				AND desired_state = $2
		`, record.TeamID, SandboxDesiredStateActive).Scan(&current); err != nil {
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
