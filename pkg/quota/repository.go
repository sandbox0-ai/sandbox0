package quota

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	db               DB
	usageStore       UsageStore
	limitPolicyStore PolicyStore
}

var ErrUsageStoreNotConfigured = errors.New("quota usage store is not configured")

type PolicyStore interface {
	GetPolicy(ctx context.Context, teamID string, dimension Dimension) (*Policy, error)
}

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

func (r *Repository) SetUsageStore(store UsageStore) {
	if r == nil {
		return
	}
	if !usageStoreConfigured(store) {
		r.usageStore = nil
		return
	}
	r.usageStore = store
}

// SetLimitPolicyStore configures the resolved policy source used by GetLimit.
func (r *Repository) SetLimitPolicyStore(store PolicyStore) {
	if r == nil {
		return
	}
	r.limitPolicyStore = store
}

func usageStoreConfigured(store UsageStore) bool {
	if store == nil {
		return false
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !value.IsNil()
	default:
		return true
	}
}

func (r *Repository) configuredUsageStore() (UsageStore, bool) {
	if r == nil || !usageStoreConfigured(r.usageStore) {
		return nil, false
	}
	return r.usageStore, true
}

func (r *Repository) GetLimit(ctx context.Context, teamID string, dimension Dimension) (*Limit, error) {
	var (
		policy *Policy
		err    error
	)
	if r != nil && r.limitPolicyStore != nil {
		policy, err = r.limitPolicyStore.GetPolicy(ctx, teamID, dimension)
	} else {
		policy, err = r.GetPolicy(ctx, teamID, dimension)
	}
	if err != nil || policy == nil {
		return nil, err
	}
	return &Limit{
		TeamID:     policy.TeamID,
		Dimension:  policy.Dimension,
		LimitValue: policy.LimitValue,
	}, nil
}

// GetPolicy resolves a team override before the region default.
func (r *Repository) GetPolicy(ctx context.Context, teamID string, dimension Dimension) (*Policy, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	if !KnownDimension(dimension) {
		return nil, fmt.Errorf("unknown quota dimension %q", dimension)
	}

	var (
		policy     Policy
		source     string
		intervalMS int64
		burstValue int64
	)
	err := r.db.QueryRow(ctx, `
		SELECT team_id, dimension, limit_value, interval_ms, burst_value, source
		FROM (
			SELECT team_id, dimension, limit_value, interval_ms, burst_value,
				'team_override'::TEXT AS source, 0 AS priority
			FROM quota.team_quota_limits
			WHERE team_id = $1 AND dimension = $2
			UNION ALL
			SELECT $1::TEXT AS team_id, dimension, limit_value, interval_ms, burst_value,
				'region_default'::TEXT AS source, 1 AS priority
			FROM quota.region_quota_limits
			WHERE dimension = $2
		) AS candidates
		ORDER BY priority
		LIMIT 1
	`, teamID, string(dimension)).Scan(
		&policy.TeamID,
		&policy.Dimension,
		&policy.LimitValue,
		&intervalMS,
		&burstValue,
		&source,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query team quota policy: %w", err)
	}
	policy.Kind = KindForDimension(policy.Dimension)
	policy.IntervalMS = intervalMS
	policy.BurstValue = burstValue
	policy.Source = Source(source)
	return &policy, nil
}

func (r *Repository) CurrentUsage(ctx context.Context, teamID string, dimension Dimension) (int64, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return 0, fmt.Errorf("team_id is required")
	}
	if dimension == "" {
		return 0, fmt.Errorf("dimension is required")
	}
	if store, ok := r.configuredUsageStore(); ok {
		return store.CurrentUsage(ctx, teamID, dimension)
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
	requested := BytesToGBRoundUp(sizeBytes)
	if decision := Check(teamID, dimension, 0, requested, limit); !decision.Allowed {
		return decision, decision.Err()
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
	requested := BytesToGBRoundUp(additionalBytes)
	if decision := Check(teamID, dimension, 0, requested, limit); !decision.Allowed {
		return decision, decision.Err()
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
	if store, ok := r.configuredUsageStore(); ok {
		return store.ProjectedStorageUsageGB(ctx, teamID, dimension, subjectType, subjectID, sizeBytes)
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
	if store, ok := r.configuredUsageStore(); ok {
		return store.AdditionalStorageUsageGB(ctx, teamID, dimension, subjectType, additionalBytes)
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
	if limit == nil {
		return fmt.Errorf("limit is nil")
	}
	return r.PutPolicy(ctx, &Policy{
		TeamID:     limit.TeamID,
		Dimension:  limit.Dimension,
		Kind:       KindForDimension(limit.Dimension),
		LimitValue: limit.LimitValue,
		Source:     SourceTeamOverride,
	})
}

// PutPolicy creates or replaces a team-specific quota policy.
func (r *Repository) PutPolicy(ctx context.Context, policy *Policy) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("quota repository is not configured")
	}
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}
	policy.TeamID = strings.TrimSpace(policy.TeamID)
	if policy.TeamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if err := ValidatePolicyValues(policy.Dimension, policy.LimitValue, policy.IntervalMS, policy.BurstValue); err != nil {
		return err
	}
	_, err := r.db.Exec(ctx, `
		INSERT INTO quota.team_quota_limits (
			team_id, dimension, limit_value, interval_ms, burst_value
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (team_id, dimension) DO UPDATE
		SET limit_value = EXCLUDED.limit_value,
			interval_ms = EXCLUDED.interval_ms,
			burst_value = EXCLUDED.burst_value,
			updated_at = NOW()
	`, policy.TeamID, string(policy.Dimension), policy.LimitValue, policy.IntervalMS, policy.BurstValue)
	if err != nil {
		return fmt.Errorf("upsert team quota policy: %w", err)
	}
	policy.Kind = KindForDimension(policy.Dimension)
	policy.Source = SourceTeamOverride
	return nil
}

func (r *Repository) DeleteLimit(ctx context.Context, teamID string, dimension Dimension) error {
	return r.DeletePolicy(ctx, teamID, dimension)
}

// DeletePolicy removes a team override so the region default applies again.
func (r *Repository) DeletePolicy(ctx context.Context, teamID string, dimension Dimension) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("quota repository is not configured")
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if !KnownDimension(dimension) {
		return fmt.Errorf("unknown quota dimension %q", dimension)
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

// EnsureDefaultPolicies bootstraps region defaults once without overwriting
// policies already reconciled by another owner.
func (r *Repository) EnsureDefaultPolicies(ctx context.Context, owner string, defaults []DefaultLimit) error {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return fmt.Errorf("default quota owner is required")
	}
	seen := make(map[Dimension]struct{}, len(defaults))
	for _, item := range defaults {
		if _, ok := seen[item.Dimension]; ok {
			return fmt.Errorf("duplicate default quota dimension %q", item.Dimension)
		}
		seen[item.Dimension] = struct{}{}
		policy := &Policy{
			Dimension:  item.Dimension,
			Kind:       KindForDimension(item.Dimension),
			LimitValue: item.LimitValue,
			IntervalMS: item.IntervalMS,
			BurstValue: item.BurstValue,
			Source:     SourceRegionDefault,
		}
		if err := r.ensureDefaultPolicy(ctx, owner, policy); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) ensureDefaultPolicy(ctx context.Context, owner string, policy *Policy) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("quota repository is not configured")
	}
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}
	if err := ValidatePolicyValues(policy.Dimension, policy.LimitValue, policy.IntervalMS, policy.BurstValue); err != nil {
		return err
	}
	_, err := r.db.Exec(ctx, `
		WITH claimed AS (
			INSERT INTO quota.region_quota_bootstrap (dimension)
			VALUES ($1)
			ON CONFLICT (dimension) DO NOTHING
			RETURNING dimension
		)
		INSERT INTO quota.region_quota_limits (
			dimension, limit_value, interval_ms, burst_value, managed_by
		)
		SELECT dimension, $2, $3, $4, $5
		FROM claimed
		ON CONFLICT (dimension) DO NOTHING
	`, string(policy.Dimension), policy.LimitValue, policy.IntervalMS, policy.BurstValue, owner)
	if err != nil {
		return fmt.Errorf("ensure region quota policy: %w", err)
	}
	return nil
}

// SyncDefaultPolicies reconciles the complete set of region defaults owned by
// one declarative configuration source.
func (r *Repository) SyncDefaultPolicies(ctx context.Context, owner string, defaults []DefaultLimit) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("quota repository is not configured")
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return fmt.Errorf("default quota owner is required")
	}
	seen := make(map[Dimension]struct{}, len(defaults))
	dimensions := make([]string, 0, len(defaults))
	for _, item := range defaults {
		if _, ok := seen[item.Dimension]; ok {
			return fmt.Errorf("duplicate default quota dimension %q", item.Dimension)
		}
		seen[item.Dimension] = struct{}{}
		if err := ValidatePolicyValues(item.Dimension, item.LimitValue, item.IntervalMS, item.BurstValue); err != nil {
			return err
		}
		dimensions = append(dimensions, string(item.Dimension))
		_, err := r.db.Exec(ctx, `
			WITH marked AS (
				INSERT INTO quota.region_quota_bootstrap (dimension)
				VALUES ($1)
				ON CONFLICT (dimension) DO NOTHING
			)
			INSERT INTO quota.region_quota_limits (
				dimension, limit_value, interval_ms, burst_value, managed_by
			)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (dimension) DO UPDATE
			SET limit_value = EXCLUDED.limit_value,
				interval_ms = EXCLUDED.interval_ms,
				burst_value = EXCLUDED.burst_value,
				managed_by = EXCLUDED.managed_by,
				updated_at = NOW()
		`, string(item.Dimension), item.LimitValue, item.IntervalMS, item.BurstValue, owner)
		if err != nil {
			return fmt.Errorf("sync region quota policy %s: %w", item.Dimension, err)
		}
	}
	_, err := r.db.Exec(ctx, `
		DELETE FROM quota.region_quota_limits
		WHERE managed_by = $1
			AND NOT (dimension = ANY($2::TEXT[]))
	`, owner, dimensions)
	if err != nil {
		return fmt.Errorf("delete stale region quota policies: %w", err)
	}
	return nil
}
