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
	db            DB
	defaultLimits map[Dimension]int64
	usageStore    UsageStore
}

var ErrUsageStoreNotConfigured = errors.New("quota usage store is not configured")

type UsageStore interface {
	CurrentUsage(ctx context.Context, teamID string, dimension Dimension) (int64, error)
	ProjectedStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error)
	AdditionalStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType string, additionalBytes int64) (int64, error)
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

func NewRepositoryWithDefaults(pool *pgxpool.Pool, defaults []DefaultLimit) (*Repository, error) {
	if pool == nil {
		return nil, nil
	}
	return NewRepositoryWithDBDefaults(pool, defaults)
}

func NewRepositoryWithDBDefaults(db DB, defaults []DefaultLimit) (*Repository, error) {
	if db == nil {
		return nil, nil
	}
	defaultLimits, err := buildDefaultLimitMap(defaults)
	if err != nil {
		return nil, err
	}
	return &Repository{db: db, defaultLimits: defaultLimits}, nil
}

func (r *Repository) SetUsageStore(store UsageStore) {
	if r == nil {
		return
	}
	r.usageStore = store
}

func buildDefaultLimitMap(defaults []DefaultLimit) (map[Dimension]int64, error) {
	if len(defaults) == 0 {
		return nil, nil
	}
	limits := make(map[Dimension]int64, len(defaults))
	for _, limit := range defaults {
		if !KnownDimension(limit.Dimension) {
			return nil, fmt.Errorf("unknown quota dimension %q", limit.Dimension)
		}
		if limit.LimitValue < 0 {
			return nil, fmt.Errorf("default quota %s limit_value must be non-negative", limit.Dimension)
		}
		if _, exists := limits[limit.Dimension]; exists {
			return nil, fmt.Errorf("duplicate default quota dimension %q", limit.Dimension)
		}
		limits[limit.Dimension] = limit.LimitValue
	}
	return limits, nil
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
			return r.defaultLimit(teamID, dimension), nil
		}
		return nil, fmt.Errorf("query team quota limit: %w", err)
	}
	return &limit, nil
}

func (r *Repository) defaultLimit(teamID string, dimension Dimension) *Limit {
	if r == nil || len(r.defaultLimits) == 0 {
		return nil
	}
	limitValue, ok := r.defaultLimits[dimension]
	if !ok {
		return nil
	}
	return &Limit{TeamID: teamID, Dimension: dimension, LimitValue: limitValue}
}

func (r *Repository) CurrentUsage(ctx context.Context, teamID string, dimension Dimension) (int64, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return 0, fmt.Errorf("team_id is required")
	}
	if dimension == "" {
		return 0, fmt.Errorf("dimension is required")
	}
	if r != nil && r.usageStore != nil {
		return r.usageStore.CurrentUsage(ctx, teamID, dimension)
	}
	return 0, ErrUsageStoreNotConfigured
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

func (r *Repository) CheckAdditionalStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType string, additionalBytes int64) (Decision, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return Decision{}, fmt.Errorf("team_id is required")
	}
	if additionalBytes <= 0 {
		return Check(teamID, dimension, 0, 0, nil), nil
	}
	limit, err := r.GetLimit(ctx, teamID, dimension)
	if err != nil {
		return Decision{}, err
	}
	if limit == nil {
		return Check(teamID, dimension, 0, 0, nil), nil
	}
	projected, err := r.AdditionalStorageUsageGB(ctx, teamID, dimension, subjectType, additionalBytes)
	if err != nil {
		return Decision{}, err
	}
	decision := Check(teamID, dimension, projected, 0, limit)
	return decision, decision.Err()
}

func (r *Repository) ProjectedStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error) {
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
	if r != nil && r.usageStore != nil {
		return r.usageStore.ProjectedStorageUsageGB(ctx, teamID, dimension, subjectType, subjectID, sizeBytes)
	}
	return 0, ErrUsageStoreNotConfigured
}

func (r *Repository) AdditionalStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType string, additionalBytes int64) (int64, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return 0, fmt.Errorf("team_id is required")
	}
	if additionalBytes < 0 {
		return 0, fmt.Errorf("additional_bytes must be non-negative")
	}
	if !storageDimensionMatchesSubjectType(dimension, subjectType) {
		return 0, fmt.Errorf("quota dimension %q does not match storage subject_type %q", dimension, subjectType)
	}
	if r != nil && r.usageStore != nil {
		return r.usageStore.AdditionalStorageUsageGB(ctx, teamID, dimension, subjectType, additionalBytes)
	}
	return 0, ErrUsageStoreNotConfigured
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
