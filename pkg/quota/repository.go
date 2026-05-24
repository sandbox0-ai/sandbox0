package quota

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
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
	case DimensionCPU:
		var current int64
		if err := r.db.QueryRow(ctx, `
			SELECT COALESCE(SUM(resource_millicpu), 0)
			FROM metering.manager_sandbox_projection_state
			WHERE team_id = $1
				AND claimed_at IS NOT NULL
				AND terminated_at IS NULL
		`, teamID).Scan(&current); err != nil {
			return 0, fmt.Errorf("query cpu quota usage: %w", err)
		}
		return current, nil
	case DimensionMemory:
		var current int64
		if err := r.db.QueryRow(ctx, `
			SELECT COALESCE(SUM(resource_memory_mib), 0)
			FROM metering.manager_sandbox_projection_state
			WHERE team_id = $1
				AND claimed_at IS NOT NULL
				AND terminated_at IS NULL
		`, teamID).Scan(&current); err != nil {
			return 0, fmt.Errorf("query memory quota usage: %w", err)
		}
		return current, nil
	case DimensionVolumeStorageGB:
		current, err := r.currentStorageUsageBytes(ctx, teamID, metering.SubjectTypeVolume)
		if err != nil {
			return 0, err
		}
		return BytesToGBRoundUp(current), nil
	case DimensionSnapshotGB:
		current, err := r.currentStorageUsageBytes(ctx, teamID, metering.SubjectTypeSnapshot)
		if err != nil {
			return 0, err
		}
		return BytesToGBRoundUp(current), nil
	case DimensionEgress:
		return r.currentNetworkUsage(ctx, teamID, metering.WindowTypeSandboxEgressBytes, metering.WindowTypeFunctionEgressBytes)
	default:
		return 0, fmt.Errorf("unsupported quota usage dimension %q", dimension)
	}
}

func (r *Repository) CheckProjectedStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (Decision, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return Decision{}, fmt.Errorf("team_id is required")
	}
	limit, err := r.GetLimit(ctx, teamID, dimension)
	if err != nil {
		return Decision{}, err
	}
	if limit == nil {
		return Check(teamID, dimension, 0, 0, nil), nil
	}
	projected, err := r.ProjectedStorageUsageGB(ctx, teamID, dimension, subjectType, subjectID, sizeBytes)
	if err != nil {
		return Decision{}, err
	}
	decision := Check(teamID, dimension, projected, 0, limit)
	return decision, decision.Err()
}

func (r *Repository) ProjectedStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error) {
	if r == nil || r.db == nil {
		return 0, nil
	}
	teamID = strings.TrimSpace(teamID)
	subjectID = strings.TrimSpace(subjectID)
	if teamID == "" {
		return 0, fmt.Errorf("team_id is required")
	}
	if subjectID == "" {
		return 0, fmt.Errorf("subject_id is required")
	}
	if sizeBytes < 0 {
		return 0, fmt.Errorf("size_bytes must be non-negative")
	}
	if !storageDimensionMatchesSubjectType(dimension, subjectType) {
		return 0, fmt.Errorf("quota dimension %q does not match storage subject_type %q", dimension, subjectType)
	}

	var otherBytes int64
	if err := r.db.QueryRow(ctx, `
		SELECT COALESCE(SUM(size_bytes), 0)
		FROM metering.storage_projection_state
		WHERE team_id = $1
			AND subject_type = $2
			AND subject_id <> $3
	`, teamID, subjectType, subjectID).Scan(&otherBytes); err != nil {
		return 0, fmt.Errorf("query projected storage quota usage: %w", err)
	}
	return BytesToGBRoundUp(otherBytes + sizeBytes), nil
}

func (r *Repository) currentStorageUsageBytes(ctx context.Context, teamID, subjectType string) (int64, error) {
	var current int64
	if err := r.db.QueryRow(ctx, `
		SELECT COALESCE(SUM(size_bytes), 0)
		FROM metering.storage_projection_state
		WHERE team_id = $1
			AND subject_type = $2
	`, teamID, subjectType).Scan(&current); err != nil {
		return 0, fmt.Errorf("query storage quota usage: %w", err)
	}
	return current, nil
}

func (r *Repository) currentNetworkUsage(ctx context.Context, teamID string, windowTypes ...string) (int64, error) {
	if len(windowTypes) == 0 {
		return 0, fmt.Errorf("window type is required")
	}
	args := make([]any, 0, len(windowTypes)+1)
	args = append(args, teamID)
	placeholders := make([]string, 0, len(windowTypes))
	for i, windowType := range windowTypes {
		args = append(args, windowType)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+2))
	}
	var current int64
	if err := r.db.QueryRow(ctx, fmt.Sprintf(`
		SELECT COALESCE(SUM(value), 0)
		FROM metering.usage_windows
		WHERE team_id = $1
			AND window_type IN (%s)
	`, strings.Join(placeholders, ", ")), args...).Scan(&current); err != nil {
		return 0, fmt.Errorf("query network quota usage: %w", err)
	}
	return current, nil
}

func storageDimensionMatchesSubjectType(dimension Dimension, subjectType string) bool {
	switch dimension {
	case DimensionVolumeStorageGB:
		return subjectType == metering.SubjectTypeVolume
	case DimensionSnapshotGB:
		return subjectType == metering.SubjectTypeSnapshot
	default:
		return false
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
