package metering

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeDB struct {
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
}

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if f.execFn != nil {
		return f.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if f.queryFn != nil {
		return f.queryFn(ctx, sql, args...)
	}
	return &fakeRows{}, nil
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
		case *int:
			*typed = r.values[i].(int)
		case *string:
			*typed = r.values[i].(string)
		case **time.Time:
			*typed = r.values[i].(*time.Time)
		case *time.Time:
			*typed = r.values[i].(time.Time)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}

type fakeRows struct {
	values [][]any
	index  int
	err    error
}

func (r *fakeRows) Close() {}

func (r *fakeRows) Err() error {
	return r.err
}

func (r *fakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *fakeRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}
	r.index++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.values[r.index-1]
	for i := range dest {
		switch typed := dest[i].(type) {
		case *int64:
			*typed = row[i].(int64)
		case *string:
			*typed = row[i].(string)
		case *time.Time:
			*typed = row[i].(time.Time)
		case *json.RawMessage:
			*typed = row[i].(json.RawMessage)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}

func (r *fakeRows) Values() ([]any, error) {
	return nil, errors.New("not implemented")
}

func (r *fakeRows) RawValues() [][]byte {
	return nil
}

func (r *fakeRows) Conn() *pgx.Conn {
	return nil
}

func TestAppendEventValidation(t *testing.T) {
	repo := &Repository{db: &fakeDB{}}
	err := repo.AppendEvent(context.Background(), &Event{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestAppendEventUsesDefaultPayload(t *testing.T) {
	called := false
	repo := &Repository{
		db: &fakeDB{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				called = true
				payload, ok := args[14].(json.RawMessage)
				if !ok {
					t.Fatalf("payload type = %T, want json.RawMessage", args[14])
				}
				if string(payload) != "{}" {
					t.Fatalf("payload = %s, want {}", string(payload))
				}
				return pgconn.CommandTag{}, nil
			},
		},
	}

	err := repo.AppendEvent(context.Background(), &Event{
		EventID:     "evt-1",
		Producer:    "manager.sandbox_lifecycle",
		EventType:   EventTypeSandboxClaimed,
		SubjectType: SubjectTypeSandbox,
		SubjectID:   "sb-1",
		OccurredAt:  time.Date(2026, 3, 12, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if !called {
		t.Fatal("expected Exec to be called")
	}
}

func TestAppendWindowUsesDefaultPayload(t *testing.T) {
	called := false
	repo := &Repository{
		db: &fakeDB{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				called = true
				payload, ok := args[17].(json.RawMessage)
				if !ok {
					t.Fatalf("payload type = %T, want json.RawMessage", args[17])
				}
				if string(payload) != "{}" {
					t.Fatalf("payload = %s, want {}", string(payload))
				}
				return pgconn.CommandTag{}, nil
			},
		},
	}

	err := repo.AppendWindow(context.Background(), &Window{
		WindowID:    "win-1",
		Producer:    "manager.sandbox_lifecycle",
		WindowType:  WindowTypeSandboxRuntimeMiBMilliseconds,
		SubjectType: SubjectTypeSandbox,
		SubjectID:   "sb-1",
		WindowStart: time.Date(2026, 3, 12, 12, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 3, 12, 12, 5, 0, 0, time.UTC),
		Value:       300_000,
		Unit:        WindowUnitMiBMilliseconds,
	})
	if err != nil {
		t.Fatalf("AppendWindow: %v", err)
	}
	if !called {
		t.Fatal("expected Exec to be called")
	}
}

func TestGetStatusUsesMinProducerWatermark(t *testing.T) {
	earliest := time.Date(2026, 3, 12, 11, 0, 0, 0, time.UTC)
	rowCall := 0
	repo := &Repository{
		db: &fakeDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				rowCall++
				switch rowCall {
				case 1:
					return fakeRow{values: []any{int64(99)}}
				case 2:
					return fakeRow{values: []any{int64(12)}}
				case 3:
					return fakeRow{values: []any{&earliest, 2}}
				case 4:
					return fakeRow{values: []any{"aws-us-east-1"}}
				default:
					return fakeRow{err: errors.New("unexpected query")}
				}
			},
		},
	}

	status, err := repo.GetStatus(context.Background(), "fallback-region")
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if status.LatestEventSequence != 99 {
		t.Fatalf("latest_event_sequence = %d, want 99", status.LatestEventSequence)
	}
	if status.LatestWindowSequence != 12 {
		t.Fatalf("latest_window_sequence = %d, want 12", status.LatestWindowSequence)
	}
	if status.ProducerCount != 2 {
		t.Fatalf("producer_count = %d, want 2", status.ProducerCount)
	}
	if status.RegionID != "aws-us-east-1" {
		t.Fatalf("region_id = %q, want aws-us-east-1", status.RegionID)
	}
	if status.CompleteBefore == nil || !status.CompleteBefore.Equal(earliest) {
		t.Fatalf("complete_before = %v, want %v", status.CompleteBefore, earliest)
	}
}

func TestListEventsAfterReturnsOrderedEvents(t *testing.T) {
	occurredAt := time.Date(2026, 3, 12, 12, 30, 0, 0, time.UTC)
	recordedAt := occurredAt.Add(time.Second)
	repo := &Repository{
		db: &fakeDB{
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &fakeRows{
					values: [][]any{
						{
							int64(10),
							"evt-10",
							"storage-proxy.volume",
							"aws-us-east-1",
							EventTypeVolumeCreated,
							SubjectTypeVolume,
							"vol-1",
							"team-1",
							"user-1",
							"",
							"vol-1",
							"",
							"",
							"cluster-a",
							occurredAt,
							recordedAt,
							json.RawMessage(`{"source":"test"}`),
						},
					},
				}, nil
			},
		},
	}

	events, err := repo.ListEventsAfter(context.Background(), 9, 100)
	if err != nil {
		t.Fatalf("ListEventsAfter: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if events[0].Sequence != 10 || events[0].VolumeID != "vol-1" {
		t.Fatalf("unexpected event: %+v", events[0])
	}
}

func TestListWindowsAfterReturnsOrderedWindows(t *testing.T) {
	windowStart := time.Date(2026, 3, 12, 12, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(5 * time.Minute)
	recordedAt := windowEnd.Add(time.Second)
	repo := &Repository{
		db: &fakeDB{
			queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return &fakeRows{
					values: [][]any{
						{
							int64(7),
							"window-7",
							"manager.sandbox_lifecycle",
							"aws-us-east-1",
							WindowTypeSandboxRuntimeMiBMilliseconds,
							SubjectTypeSandbox,
							"sb-1",
							"team-1",
							"user-1",
							"sb-1",
							"",
							"",
							"tpl-1",
							"cluster-a",
							windowStart,
							windowEnd,
							int64(300_000),
							WindowUnitMiBMilliseconds,
							recordedAt,
							json.RawMessage(`{"source":"test"}`),
						},
					},
				}, nil
			},
		},
	}

	windows, err := repo.ListWindowsAfter(context.Background(), 6, 100)
	if err != nil {
		t.Fatalf("ListWindowsAfter: %v", err)
	}
	if len(windows) != 1 {
		t.Fatalf("window count = %d, want 1", len(windows))
	}
	if windows[0].Sequence != 7 || windows[0].Value != 300_000 || windows[0].SandboxID != "sb-1" {
		t.Fatalf("unexpected window: %+v", windows[0])
	}
}

func TestStorageWindowFromStateSkipsSubByteHour(t *testing.T) {
	start := time.Date(2026, 3, 12, 12, 0, 0, 0, time.UTC)
	window := storageWindowFromState(&StorageProjectionState{
		SubjectType: SubjectTypeVolume,
		SubjectID:   "vol-1",
		Product:     ProductSandbox,
		SizeBytes:   1,
		ObservedAt:  start,
	}, start.Add(time.Millisecond))
	if window != nil {
		t.Fatalf("expected nil window for sub-byte-hour usage, got %+v", window)
	}
}

func TestStorageWindowFromStateUsesRootFSWindowType(t *testing.T) {
	start := time.Date(2026, 3, 12, 12, 0, 0, 0, time.UTC)
	window := storageWindowFromState(&StorageProjectionState{
		SubjectType: SubjectTypeRootFS,
		SubjectID:   "team-1",
		Product:     ProductSandbox,
		TeamID:      "team-1",
		SizeBytes:   1024,
		ObservedAt:  start,
	}, start.Add(time.Hour))
	if window == nil {
		t.Fatal("expected rootfs storage window")
	}
	if window.WindowType != WindowTypeSandboxRootFSByteHours {
		t.Fatalf("window_type = %q, want %q", window.WindowType, WindowTypeSandboxRootFSByteHours)
	}
	if window.SubjectType != SubjectTypeRootFS || window.SubjectID != "team-1" {
		t.Fatalf("unexpected subject: %s/%s", window.SubjectType, window.SubjectID)
	}
}

func TestRecordStorageObservationClosesPreviousWindow(t *testing.T) {
	start := time.Date(2026, 3, 12, 12, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	var insertedWindow bool
	var upsertedState bool
	repo := &Repository{
		db: &fakeDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				if !strings.Contains(sql, "storage_projection_state") {
					return fakeRow{err: errors.New("unexpected query")}
				}
				return fakeRow{values: []any{
					SubjectTypeVolume,
					"vol-1",
					ProductSandbox,
					"",
					"team-1",
					"user-1",
					"",
					"vol-1",
					"",
					"cluster-a",
					"aws-us-east-1",
					int64(1024),
					start,
				}}
			},
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				switch {
				case strings.Contains(sql, "usage_windows"):
					insertedWindow = true
					if args[3] != WindowTypeSandboxVolumeByteHours {
						t.Fatalf("window_type arg = %v, want %s", args[3], WindowTypeSandboxVolumeByteHours)
					}
					if args[15] != int64(2048) {
						t.Fatalf("window value arg = %v, want 2048", args[15])
					}
				case strings.Contains(sql, "storage_projection_state"):
					upsertedState = true
					if args[11] != int64(2048) {
						t.Fatalf("state size arg = %v, want 2048", args[11])
					}
				default:
					t.Fatalf("unexpected exec: %s", sql)
				}
				return pgconn.CommandTag{}, nil
			},
		},
	}

	err := repo.recordStorageObservation(context.Background(), repo.db, &StorageObservation{
		SubjectType: SubjectTypeVolume,
		SubjectID:   "vol-1",
		TeamID:      "team-1",
		UserID:      "user-1",
		VolumeID:    "vol-1",
		RegionID:    "aws-us-east-1",
		ClusterID:   "cluster-a",
		SizeBytes:   2048,
		ObservedAt:  end,
	})
	if err != nil {
		t.Fatalf("recordStorageObservation: %v", err)
	}
	if !insertedWindow {
		t.Fatal("expected usage window insert")
	}
	if !upsertedState {
		t.Fatal("expected projection state upsert")
	}
}
