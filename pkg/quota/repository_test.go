package quota

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type fakeDB struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
}

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if f.execFn != nil {
		return f.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if f.queryRowFn != nil {
		return f.queryRowFn(ctx, sql, args...)
	}
	return fakeRow{}
}

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		switch typed := dest[i].(type) {
		case *int64:
			*typed = r.values[i].(int64)
		case *string:
			*typed = r.values[i].(string)
		case *Dimension:
			switch value := r.values[i].(type) {
			case Dimension:
				*typed = value
			case string:
				*typed = Dimension(value)
			default:
				return errors.New("unsupported dimension value")
			}
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}

func TestGetPolicyReturnsRegionDefault(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{
				"team-1",
				DimensionActiveSandboxes,
				int64(3),
				int64(0),
				int64(0),
				string(SourceRegionDefault),
			}}
		},
	})

	policy, err := repo.GetPolicy(context.Background(), "team-1", DimensionActiveSandboxes)
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if policy == nil || policy.TeamID != "team-1" || policy.Dimension != DimensionActiveSandboxes || policy.LimitValue != 3 {
		t.Fatalf("policy = %+v, want default active sandbox limit 3", policy)
	}
	if policy.Kind != KindCapacity || policy.Source != SourceRegionDefault {
		t.Fatalf("policy = %+v, want region capacity policy", policy)
	}
}

func TestGetPolicyReturnsTeamOverride(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{
				"team-1",
				DimensionAPIRequests,
				int64(5),
				int64(1000),
				int64(10),
				string(SourceTeamOverride),
			}}
		},
	})

	policy, err := repo.GetPolicy(context.Background(), "team-1", DimensionAPIRequests)
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if policy == nil || policy.LimitValue != 5 || policy.IntervalMS != 1000 || policy.BurstValue != 10 {
		t.Fatalf("policy = %+v, want team rate override", policy)
	}
}

func TestGetLimitUsesConfiguredPolicyStore(t *testing.T) {
	source := &countingPolicyStore{policy: &Policy{
		TeamID:     "team-1",
		Dimension:  DimensionActiveSandboxes,
		Kind:       KindCapacity,
		LimitValue: 120,
		Source:     SourceTeamOverride,
	}}
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			t.Fatal("GetLimit must use the configured policy store")
			return fakeRow{}
		},
	})
	repo.SetLimitPolicyStore(source)

	limit, err := repo.GetLimit(context.Background(), "team-1", DimensionActiveSandboxes)
	if err != nil {
		t.Fatalf("GetLimit: %v", err)
	}
	if limit == nil || limit.LimitValue != 120 {
		t.Fatalf("limit = %+v, want active sandbox limit 120", limit)
	}
	if source.calls.Load() != 1 {
		t.Fatalf("source calls = %d, want 1", source.calls.Load())
	}
}

func TestEnsureDefaultPoliciesRejectsInvalidLimits(t *testing.T) {
	tests := []struct {
		name     string
		defaults []DefaultLimit
	}{
		{
			name:     "unknown dimension",
			defaults: []DefaultLimit{{Dimension: Dimension("unknown"), LimitValue: 1}},
		},
		{
			name:     "negative value",
			defaults: []DefaultLimit{{Dimension: DimensionActiveSandboxes, LimitValue: -1}},
		},
		{
			name: "duplicate dimension",
			defaults: []DefaultLimit{
				{Dimension: DimensionActiveSandboxes, LimitValue: 1},
				{Dimension: DimensionActiveSandboxes, LimitValue: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewRepositoryWithDB(&fakeDB{})
			if err := repo.EnsureDefaultPolicies(context.Background(), "test", tt.defaults); err == nil {
				t.Fatal("EnsureDefaultPolicies error = nil, want error")
			}
		})
	}
}

type fakeUsageStore struct {
	currentFn    func(ctx context.Context, teamID string, dimension Dimension) (int64, error)
	projectedFn  func(ctx context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error)
	additionalFn func(ctx context.Context, teamID string, dimension Dimension, subjectType string, additionalBytes int64) (int64, error)
}

func (f *fakeUsageStore) CurrentUsage(ctx context.Context, teamID string, dimension Dimension) (int64, error) {
	if f.currentFn != nil {
		return f.currentFn(ctx, teamID, dimension)
	}
	return 0, nil
}

func (f *fakeUsageStore) ProjectedStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error) {
	if f.projectedFn != nil {
		return f.projectedFn(ctx, teamID, dimension, subjectType, subjectID, sizeBytes)
	}
	return 0, nil
}

func (f *fakeUsageStore) AdditionalStorageUsageGB(ctx context.Context, teamID string, dimension Dimension, subjectType string, additionalBytes int64) (int64, error) {
	if f.additionalFn != nil {
		return f.additionalFn(ctx, teamID, dimension, subjectType, additionalBytes)
	}
	return 0, nil
}

func TestCurrentUsageDelegatesToUsageStore(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{})
	repo.SetUsageStore(&fakeUsageStore{
		currentFn: func(_ context.Context, teamID string, dimension Dimension) (int64, error) {
			if teamID != "team-1" {
				t.Fatalf("teamID = %q, want trimmed team-1", teamID)
			}
			if dimension != DimensionActiveSandboxes {
				t.Fatalf("dimension = %q, want active sandboxes", dimension)
			}
			return 1024, nil
		},
	})

	got, err := repo.CurrentUsage(context.Background(), " team-1 ", DimensionActiveSandboxes)
	if err != nil {
		t.Fatalf("CurrentUsage: %v", err)
	}
	if got != 1024 {
		t.Fatalf("CurrentUsage = %d, want 1024", got)
	}
}

func TestCurrentUsageWithoutUsageStoreReturnsUnavailable(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			t.Fatal("CurrentUsage must not query PostgreSQL usage tables")
			return fakeRow{}
		},
	})

	_, err := repo.CurrentUsage(context.Background(), "team-1", DimensionActiveSandboxes)
	if err == nil || !strings.Contains(err.Error(), "quota usage store is not configured") {
		t.Fatalf("CurrentUsage error = %v, want usage store unavailable", err)
	}
}

func TestCurrentUsageWithTypedNilUsageStoreReturnsUnavailable(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{})
	var store *fakeUsageStore
	repo.SetUsageStore(store)

	_, err := repo.CurrentUsage(context.Background(), "team-1", DimensionActiveSandboxes)
	if !errors.Is(err, ErrUsageStoreNotConfigured) {
		t.Fatalf("CurrentUsage error = %v, want ErrUsageStoreNotConfigured", err)
	}
}

func TestProjectedStorageUsageGBDelegatesToUsageStore(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{})
	repo.SetUsageStore(&fakeUsageStore{
		projectedFn: func(_ context.Context, teamID string, dimension Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error) {
			if teamID != "team-1" || dimension != DimensionVolumeStorageGB || subjectType != metering.SubjectTypeVolume || subjectID != "vol-1" || sizeBytes != 1 {
				t.Fatalf("unexpected args: team=%q dimension=%q subjectType=%q subjectID=%q size=%d", teamID, dimension, subjectType, subjectID, sizeBytes)
			}
			return 2, nil
		},
	})

	got, err := repo.ProjectedStorageUsageGB(context.Background(), " team-1 ", DimensionVolumeStorageGB, metering.SubjectTypeVolume, " vol-1 ", 1)
	if err != nil {
		t.Fatalf("ProjectedStorageUsageGB: %v", err)
	}
	if got != 2 {
		t.Fatalf("ProjectedStorageUsageGB = %d, want 2", got)
	}
}

func TestAdditionalStorageUsageGBDelegatesToUsageStore(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{})
	repo.SetUsageStore(&fakeUsageStore{
		additionalFn: func(_ context.Context, teamID string, dimension Dimension, subjectType string, additionalBytes int64) (int64, error) {
			if teamID != "team-1" || dimension != DimensionSnapshotGB || subjectType != metering.SubjectTypeSnapshot || additionalBytes != 1 {
				t.Fatalf("unexpected args: team=%q dimension=%q subjectType=%q additional=%d", teamID, dimension, subjectType, additionalBytes)
			}
			return 3, nil
		},
	})

	got, err := repo.AdditionalStorageUsageGB(context.Background(), " team-1 ", DimensionSnapshotGB, metering.SubjectTypeSnapshot, 1)
	if err != nil {
		t.Fatalf("AdditionalStorageUsageGB: %v", err)
	}
	if got != 3 {
		t.Fatalf("AdditionalStorageUsageGB = %d, want 3", got)
	}
}

func TestCheckProjectedStorageUsageRejectsRequestBeforeUsageLookup(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{
				"team-1",
				DimensionSnapshotGB,
				int64(0),
				int64(0),
				int64(0),
				string(SourceTeamOverride),
			}}
		},
	})

	decision, err := repo.CheckProjectedStorageUsageGB(
		context.Background(),
		"team-1",
		DimensionSnapshotGB,
		metering.SubjectTypeSnapshot,
		"snapshot-1",
		1,
	)
	if !IsExceeded(err) {
		t.Fatalf("CheckProjectedStorageUsageGB error = %v, want quota exceeded", err)
	}
	if decision.Allowed || decision.Requested != 1 || decision.LimitValue != 0 {
		t.Fatalf("decision = %+v, want one requested GB rejected by zero limit", decision)
	}
}

func TestCheckAdditionalStorageUsageRejectsRequestBeforeUsageLookup(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{
				"team-1",
				DimensionVolumeStorageGB,
				int64(0),
				int64(0),
				int64(0),
				string(SourceTeamOverride),
			}}
		},
	})

	decision, err := repo.CheckAdditionalStorageUsageGB(
		context.Background(),
		"team-1",
		DimensionVolumeStorageGB,
		metering.SubjectTypeVolume,
		1,
	)
	if !IsExceeded(err) {
		t.Fatalf("CheckAdditionalStorageUsageGB error = %v, want quota exceeded", err)
	}
	if decision.Allowed || decision.Requested != 1 || decision.LimitValue != 0 {
		t.Fatalf("decision = %+v, want one requested GB rejected by zero limit", decision)
	}
}
