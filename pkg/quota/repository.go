package quota

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Repository struct {
	db DB
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	if pool == nil {
		return nil
	}
	return &Repository{db: pool}
}

func NewRepositoryWithDB(db DB) *Repository {
	if db == nil {
		return nil
	}
	return &Repository{db: db}
}

func (r *Repository) GetLimit(ctx context.Context, teamID string, dimension Dimension) (*Limit, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if dimension == "" {
		return nil, fmt.Errorf("dimension is required")
	}

	var limit Limit
	err := r.db.QueryRow(ctx, `
		SELECT team_id, dimension, limit_value
		FROM quota.team_quota_limits
		WHERE team_id = $1 AND dimension = $2
	`, teamID, string(dimension)).Scan(&limit.TeamID, &limit.Dimension, &limit.LimitValue)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query team quota limit: %w", err)
	}
	return &limit, nil
}

func (r *Repository) CurrentUsage(ctx context.Context, teamID string, dimension Dimension) (int64, error) {
	if r == nil || r.db == nil {
		return 0, nil
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return 0, fmt.Errorf("team_id is required")
	}
	switch dimension {
	case DimensionActiveSandboxes:
		var current int64
		if err := r.db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM metering.manager_sandbox_projection_state
			WHERE team_id = $1
				AND claimed_at IS NOT NULL
				AND terminated_at IS NULL
		`, teamID).Scan(&current); err != nil {
			return 0, fmt.Errorf("query active sandbox usage: %w", err)
		}
		return current, nil
	default:
		return 0, fmt.Errorf("unsupported quota usage dimension %q", dimension)
	}
}

func (r *Repository) PutLimit(ctx context.Context, limit *Limit) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("quota repository is not configured")
	}
	if limit == nil {
		return fmt.Errorf("limit is nil")
	}
	limit.TeamID = strings.TrimSpace(limit.TeamID)
	if limit.TeamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if limit.Dimension == "" {
		return fmt.Errorf("dimension is required")
	}
	if limit.LimitValue < 0 {
		return fmt.Errorf("limit_value must be non-negative")
	}

	_, err := r.db.Exec(ctx, `
		INSERT INTO quota.team_quota_limits (team_id, dimension, limit_value)
		VALUES ($1, $2, $3)
		ON CONFLICT (team_id, dimension) DO UPDATE
		SET limit_value = EXCLUDED.limit_value,
			updated_at = NOW()
	`, limit.TeamID, string(limit.Dimension), limit.LimitValue)
	if err != nil {
		return fmt.Errorf("upsert team quota limit: %w", err)
	}
	return nil
}

func (r *Repository) DeleteLimit(ctx context.Context, teamID string, dimension Dimension) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("quota repository is not configured")
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if dimension == "" {
		return fmt.Errorf("dimension is required")
	}

	_, err := r.db.Exec(ctx, `
		DELETE FROM quota.team_quota_limits
		WHERE team_id = $1 AND dimension = $2
	`, teamID, string(dimension))
	if err != nil {
		return fmt.Errorf("delete team quota limit: %w", err)
	}
	return nil
}
