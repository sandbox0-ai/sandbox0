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

func TestCurrentUsageReturnsStorageGB(t *testing.T) {
	tests := []struct {
		name        string
		dimension   Dimension
		subjectType string
	}{
		{name: "volume", dimension: DimensionVolumeStorageGB, subjectType: metering.SubjectTypeVolume},
		{name: "snapshot", dimension: DimensionSnapshotGB, subjectType: metering.SubjectTypeSnapshot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewRepositoryWithDB(&fakeDB{
				queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
					if !strings.Contains(sql, "storage_projection_state") {
						t.Fatalf("sql = %s, want storage projection query", sql)
					}
					if args[0] != "team-1" || args[1] != tt.subjectType {
						t.Fatalf("args = %#v, want team and storage subject", args)
					}
					return fakeRow{values: []any{BytesPerGB + 1}}
				},
			})

			got, err := repo.CurrentUsage(context.Background(), "team-1", tt.dimension)
			if err != nil {
				t.Fatalf("CurrentUsage: %v", err)
			}
			if got != 2 {
				t.Fatalf("CurrentUsage = %d, want 2", got)
			}
		})
	}
}

func TestProjectedStorageUsageGBExcludesCurrentSubject(t *testing.T) {
	repo := NewRepositoryWithDB(&fakeDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			if !strings.Contains(sql, "subject_id <> $3") {
				t.Fatalf("sql = %s, want current subject exclusion", sql)
			}
			if args[2] != "vol-1" {
				t.Fatalf("subject id arg = %v, want vol-1", args[2])
			}
			return fakeRow{values: []any{BytesPerGB}}
		},
	})

	got, err := repo.ProjectedStorageUsageGB(context.Background(), "team-1", DimensionVolumeStorageGB, metering.SubjectTypeVolume, "vol-1", 1)
	if err != nil {
		t.Fatalf("ProjectedStorageUsageGB: %v", err)
	}
	if got != 2 {
		t.Fatalf("ProjectedStorageUsageGB = %d, want 2", got)
	}
}

func TestCurrentUsageReturnsNetworkBytes(t *testing.T) {
	tests := []struct {
		name        string
		dimension   Dimension
		windowTypes []string
	}{
		{name: "egress", dimension: DimensionEgress, windowTypes: []string{metering.WindowTypeSandboxEgressBytes, metering.WindowTypeFunctionEgressBytes}},
		{name: "ingress", dimension: DimensionIngress, windowTypes: []string{metering.WindowTypeSandboxIngressBytes, metering.WindowTypeFunctionIngressBytes}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewRepositoryWithDB(&fakeDB{
				queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
					if !strings.Contains(sql, "usage_windows") {
						t.Fatalf("sql = %s, want usage windows query", sql)
					}
					if args[0] != "team-1" || args[1] != tt.windowTypes[0] || args[2] != tt.windowTypes[1] {
						t.Fatalf("args = %#v, want team and network window types", args)
					}
					return fakeRow{values: []any{int64(1024)}}
				},
			})

			got, err := repo.CurrentUsage(context.Background(), "team-1", tt.dimension)
			if err != nil {
				t.Fatalf("CurrentUsage: %v", err)
			}
			if got != 1024 {
				t.Fatalf("CurrentUsage = %d, want 1024", got)
			}
		})
	}
}
