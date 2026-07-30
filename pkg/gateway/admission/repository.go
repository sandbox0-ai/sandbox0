package admission

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Reader interface {
	Get(context.Context, string) (Record, bool, error)
}

type Store interface {
	Reader
	Put(context.Context, string, Update) (PutResult, error)
}

type Repository struct {
	db rowQuerier
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	if pool == nil {
		return &Repository{}
	}
	return &Repository{db: pool}
}

func (r *Repository) Get(ctx context.Context, teamID string) (Record, bool, error) {
	if r == nil || r.db == nil {
		return Record{}, false, errors.New("admission repository is unavailable")
	}

	record, err := scanRecord(r.db.QueryRow(ctx, `
		SELECT team_id::text, version, state, source, reason, updated_at
		FROM team_admission_states
		WHERE team_id = $1
	`, teamID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("get team admission state: %w", err)
	}
	return record, true, nil
}

func (r *Repository) Put(ctx context.Context, teamID string, update Update) (PutResult, error) {
	if r == nil || r.db == nil {
		return PutResult{}, errors.New("admission repository is unavailable")
	}

	normalized, err := update.Validate()
	if err != nil {
		return PutResult{}, err
	}

	record, err := scanRecord(r.db.QueryRow(ctx, `
		INSERT INTO team_admission_states (
			team_id,
			version,
			state,
			source,
			reason
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (team_id) DO UPDATE
		SET
			version = EXCLUDED.version,
			state = EXCLUDED.state,
			source = EXCLUDED.source,
			reason = EXCLUDED.reason,
			updated_at = NOW()
		WHERE team_admission_states.version < EXCLUDED.version
		RETURNING team_id::text, version, state, source, reason, updated_at
	`, teamID, normalized.Version, normalized.State, normalized.Source, normalized.Reason))
	if err == nil {
		return PutResult{Record: record, Applied: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return PutResult{}, fmt.Errorf("put team admission state: %w", err)
	}

	current, found, err := r.Get(ctx, teamID)
	if err != nil {
		return PutResult{}, err
	}
	if !found {
		return PutResult{}, errors.New("team admission state disappeared after a version race")
	}
	if current.Version == normalized.Version && !current.Matches(normalized) {
		return PutResult{}, fmt.Errorf(
			"%w: version %d already has different content",
			ErrVersionConflict,
			normalized.Version,
		)
	}
	return PutResult{Record: current, Applied: false}, nil
}

func scanRecord(row pgx.Row) (Record, error) {
	var record Record
	err := row.Scan(
		&record.TeamID,
		&record.Version,
		&record.State,
		&record.Source,
		&record.Reason,
		&record.UpdatedAt,
	)
	return record, err
}
