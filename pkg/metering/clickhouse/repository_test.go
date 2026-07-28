package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type captureConnector struct {
	conn *captureConn
}

func (c *captureConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (c *captureConnector) Driver() driver.Driver {
	return captureDriver{}
}

type captureDriver struct{}

func (captureDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("capture driver requires a connector")
}

type captureConn struct {
	query       string
	args        []driver.NamedValue
	queries     []string
	argsHistory [][]driver.NamedValue
	failExec    int
}

func (c *captureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *captureConn) Close() error {
	return nil
}

func (c *captureConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c *captureConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.query = query
	c.args = append([]driver.NamedValue(nil), args...)
	c.queries = append(c.queries, query)
	c.argsHistory = append(c.argsHistory, append([]driver.NamedValue(nil), args...))
	if c.failExec > 0 && len(c.queries) == c.failExec {
		return nil, errors.New("injected ClickHouse failure")
	}
	return driver.RowsAffected(1), nil
}

func newCaptureRepository(t *testing.T) (*Repository, *captureConn) {
	t.Helper()
	conn := &captureConn{}
	db := sql.OpenDB(&captureConnector{conn: conn})
	t.Cleanup(func() { _ = db.Close() })
	return NewRepository(db, Config{}), conn
}

func TestAppendEventAndWindowPreserveNanosecondTimestampsAndSequences(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	recordedAt := time.Date(2026, 7, 17, 1, 2, 3, 987654321, time.UTC)
	occurredAt := time.Date(2026, 7, 17, 9, 2, 3, 123456789, time.FixedZone("UTC+8", 8*60*60))

	event := &metering.Event{
		Sequence:    41,
		EventID:     "event-1",
		Producer:    "manager.sandbox_lifecycle",
		EventType:   metering.EventTypeSandboxClaimed,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		OccurredAt:  occurredAt,
		RecordedAt:  recordedAt,
	}
	if err := repo.AppendEvent(context.Background(), event); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if strings.Count(conn.query, dateTime64NanoPlaceholder) != 2 {
		t.Fatalf("event insert does not bind both DateTime64 values as nanoseconds: %s", conn.query)
	}
	if got := conn.args[0].Value; got != int64(41) {
		t.Fatalf("event sequence arg = %#v, want 41", got)
	}
	if got := conn.args[14].Value; got != occurredAt.UTC().UnixNano() {
		t.Fatalf("occurred_at arg = %#v, want %d", got, occurredAt.UTC().UnixNano())
	}
	if got := conn.args[15].Value; got != recordedAt.UnixNano() {
		t.Fatalf("recorded_at arg = %#v, want %d", got, recordedAt.UnixNano())
	}

	windowStart := occurredAt.Add(111 * time.Nanosecond)
	windowEnd := occurredAt.Add(time.Second + 222*time.Nanosecond)
	window := &metering.Window{
		Sequence:    42,
		WindowID:    "window-1",
		Producer:    "netd.byte_windows/node-1",
		WindowType:  metering.WindowTypeSandboxEgressBytes,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Value:       10,
		Unit:        metering.WindowUnitBytes,
		RecordedAt:  recordedAt,
	}
	if err := repo.AppendWindow(context.Background(), window); err != nil {
		t.Fatalf("AppendWindow() error = %v", err)
	}
	if strings.Count(conn.query, dateTime64NanoPlaceholder) != 3 {
		t.Fatalf("window insert does not bind all DateTime64 values as nanoseconds: %s", conn.query)
	}
	if got := conn.args[0].Value; got != int64(42) {
		t.Fatalf("window sequence arg = %#v, want 42", got)
	}
	if got := conn.args[14].Value; got != windowStart.UTC().UnixNano() {
		t.Fatalf("window_start arg = %#v, want %d", got, windowStart.UTC().UnixNano())
	}
	if got := conn.args[15].Value; got != windowEnd.UTC().UnixNano() {
		t.Fatalf("window_end arg = %#v, want %d", got, windowEnd.UTC().UnixNano())
	}
	if got := conn.args[18].Value; got != recordedAt.UnixNano() {
		t.Fatalf("window recorded_at arg = %#v, want %d", got, recordedAt.UnixNano())
	}
}

func TestAppendRequiresOutboxSequence(t *testing.T) {
	repo, _ := newCaptureRepository(t)
	now := time.Now().UTC()
	if err := repo.AppendEvent(context.Background(), &metering.Event{
		EventID:     "event-1",
		Producer:    "producer-1",
		EventType:   metering.EventTypeSandboxClaimed,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		OccurredAt:  now,
	}); err == nil || !strings.Contains(err.Error(), "sequence is required") {
		t.Fatalf("AppendEvent() error = %v, want missing sequence", err)
	}
	if err := repo.AppendWindow(context.Background(), &metering.Window{
		WindowID:    "window-1",
		Producer:    "producer-1",
		WindowType:  metering.WindowTypeSandboxEgressBytes,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		WindowStart: now,
		WindowEnd:   now.Add(time.Second),
		Value:       1,
		Unit:        metering.WindowUnitBytes,
	}); err == nil || !strings.Contains(err.Error(), "sequence is required") {
		t.Fatalf("AppendWindow() error = %v, want missing sequence", err)
	}
}

func TestApplyProjectionBatchUsesBoundedMultiRowInsertsAndAdvancesWatermarksLast(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	now := time.Date(2026, 7, 28, 10, 0, 0, 123, time.UTC)
	repo.now = func() time.Time { return now }

	events := make([]*metering.Event, 0, 2)
	windows := make([]*metering.Window, 0, 2)
	sandboxStates := make([]*metering.SandboxProjectionState, 0, 2)
	storageMutations := make([]*metering.StorageProjectionMutation, 0, 2)
	watermarks := make([]*metering.ProducerWatermark, 0, 2)
	for index := 1; index <= 2; index++ {
		events = append(events, &metering.Event{
			Sequence:    int64(index),
			EventID:     "event-" + string(rune('0'+index)),
			Producer:    "producer-1",
			EventType:   metering.EventTypeSandboxClaimed,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			OccurredAt:  now,
			RecordedAt:  now,
		})
		windows = append(windows, &metering.Window{
			Sequence:    int64(index + 2),
			WindowID:    "window-" + string(rune('0'+index)),
			Producer:    "producer-1",
			WindowType:  metering.WindowTypeSandboxEgressBytes,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			WindowStart: now,
			WindowEnd:   now.Add(time.Minute),
			Value:       int64(index),
			Unit:        metering.WindowUnitBytes,
			RecordedAt:  now,
		})
		sandboxStates = append(sandboxStates, &metering.SandboxProjectionState{
			SandboxID:      "sandbox-" + string(rune('0'+index)),
			Namespace:      "default",
			LastObservedAt: now,
		})
		storageMutations = append(storageMutations, &metering.StorageProjectionMutation{
			State: &metering.StorageProjectionState{
				SubjectType: metering.SubjectTypeVolume,
				SubjectID:   "volume-" + string(rune('0'+index)),
				ObservedAt:  now,
			},
		})
		watermarks = append(watermarks, &metering.ProducerWatermark{
			Producer:       "producer-" + string(rune('0'+index)),
			RegionID:       "region-1",
			CompleteBefore: now,
		})
	}
	tombstone := &metering.StorageProjectionMutation{
		State: &metering.StorageProjectionState{
			SubjectType: metering.SubjectTypeVolume,
			SubjectID:   "volume-deleted",
			ObservedAt:  now.Add(-time.Minute),
		},
		Deleted:   true,
		DeletedAt: now,
	}

	err := repo.ApplyProjectionBatch(context.Background(), &metering.ProjectionBatch{
		Events:           events,
		Windows:          windows,
		SandboxStates:    sandboxStates,
		StorageMutations: append(storageMutations, tombstone),
		Watermarks:       watermarks,
	})
	if err != nil {
		t.Fatalf("ApplyProjectionBatch() error = %v", err)
	}
	if len(conn.queries) != 5 {
		t.Fatalf("ClickHouse INSERT count = %d, want 5", len(conn.queries))
	}
	for index, query := range conn.queries {
		if !strings.Contains(query, meteringInsertReliabilitySettings) {
			t.Fatalf("query %d is missing durability settings: %s", index, query)
		}
	}
	if !strings.Contains(conn.queries[len(conn.queries)-1], "producer_watermarks") {
		t.Fatalf("last INSERT does not advance watermarks: %s", conn.queries[len(conn.queries)-1])
	}
	wantArgCounts := []int{36, 42, 42, 48, 10}
	for index, want := range wantArgCounts {
		if got := len(conn.argsHistory[index]); got != want {
			t.Fatalf("query %d argument count = %d, want %d", index, got, want)
		}
	}
}

func TestApplyProjectionBatchValidatesAllRowsBeforeWriting(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	now := time.Now().UTC()
	err := repo.ApplyProjectionBatch(context.Background(), &metering.ProjectionBatch{
		Events: []*metering.Event{{
			Sequence:    1,
			EventID:     "event-1",
			Producer:    "producer-1",
			EventType:   metering.EventTypeSandboxClaimed,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			OccurredAt:  now,
		}},
		Windows: []*metering.Window{{
			Sequence: 2,
			WindowID: "invalid-window",
		}},
	})
	if err == nil {
		t.Fatal("ApplyProjectionBatch() succeeded with an invalid later row")
	}
	if len(conn.queries) != 0 {
		t.Fatalf("ClickHouse INSERT count = %d, want 0", len(conn.queries))
	}
}

func TestApplyProjectionBatchDoesNotAdvanceWatermarkAfterPartialFailure(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	now := time.Now().UTC()
	conn.failExec = 2

	err := repo.ApplyProjectionBatch(context.Background(), &metering.ProjectionBatch{
		Events: []*metering.Event{{
			Sequence:    1,
			EventID:     "event-1",
			Producer:    "producer-1",
			EventType:   metering.EventTypeSandboxClaimed,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			OccurredAt:  now,
			RecordedAt:  now,
		}},
		Windows: []*metering.Window{{
			Sequence:    2,
			WindowID:    "window-1",
			Producer:    "producer-1",
			WindowType:  metering.WindowTypeSandboxEgressBytes,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			WindowStart: now,
			WindowEnd:   now.Add(time.Minute),
			Value:       1,
			Unit:        metering.WindowUnitBytes,
			RecordedAt:  now,
		}},
		Watermarks: []*metering.ProducerWatermark{{
			Producer:       "producer-1",
			CompleteBefore: now,
		}},
	})
	if err == nil {
		t.Fatal("ApplyProjectionBatch() succeeded after an injected partial failure")
	}
	if len(conn.queries) != 2 {
		t.Fatalf("ClickHouse INSERT count = %d, want 2", len(conn.queries))
	}
	for _, query := range conn.queries {
		if strings.Contains(query, "producer_watermarks") {
			t.Fatalf("watermark advanced after a partial failure: %s", query)
		}
	}
}

func TestApplyProjectionBatchPreservesStorageMutationOrder(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	now := time.Now().UTC()
	state := &metering.StorageProjectionState{
		SubjectType: metering.SubjectTypeVolume,
		SubjectID:   "volume-1",
		ObservedAt:  now,
	}

	err := repo.ApplyProjectionBatch(context.Background(), &metering.ProjectionBatch{
		StorageMutations: []*metering.StorageProjectionMutation{
			{
				State:     state,
				Deleted:   true,
				DeletedAt: now,
			},
			{State: state},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProjectionBatch() error = %v", err)
	}
	if len(conn.queries) != 1 {
		t.Fatalf("ClickHouse INSERT count = %d, want 1", len(conn.queries))
	}
	const storageRowArgumentCount = 16
	if got := conn.argsHistory[0][14].Value; got != int64(1) {
		t.Fatalf("first storage mutation deleted arg = %#v, want 1", got)
	}
	if got := conn.argsHistory[0][storageRowArgumentCount+14].Value; got != int64(0) {
		t.Fatalf("second storage mutation deleted arg = %#v, want 0", got)
	}
}

func TestApplyProjectionBatchChunksLargeTables(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	now := time.Now().UTC()
	windows := make([]*metering.Window, 0, maxMeteringInsertBatchSize+1)
	for index := 0; index <= maxMeteringInsertBatchSize; index++ {
		windows = append(windows, &metering.Window{
			Sequence:    int64(index + 1),
			WindowID:    "window-" + time.Duration(index).String(),
			Producer:    "producer-1",
			WindowType:  metering.WindowTypeSandboxEgressBytes,
			SubjectType: metering.SubjectTypeSandbox,
			SubjectID:   "sandbox-1",
			WindowStart: now,
			WindowEnd:   now.Add(time.Minute),
			Value:       1,
			Unit:        metering.WindowUnitBytes,
			RecordedAt:  now,
		})
	}
	if err := repo.ApplyProjectionBatch(context.Background(), &metering.ProjectionBatch{Windows: windows}); err != nil {
		t.Fatalf("ApplyProjectionBatch() error = %v", err)
	}
	if len(conn.queries) != 2 {
		t.Fatalf("ClickHouse INSERT count = %d, want 2", len(conn.queries))
	}
	if got := len(conn.argsHistory[0]); got != maxMeteringInsertBatchSize*21 {
		t.Fatalf("first chunk argument count = %d", got)
	}
	if got := len(conn.argsHistory[1]); got != 21 {
		t.Fatalf("second chunk argument count = %d", got)
	}
}

func TestWatermarkAndProjectionStatePreserveNanosecondTimestamps(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	now := time.Date(2026, 7, 17, 1, 2, 3, 987654321, time.UTC)
	repo.now = func() time.Time { return now }
	completeBefore := now.Add(-time.Second + 123*time.Nanosecond)

	if err := repo.UpsertProducerWatermark(context.Background(), "producer-1", "region-1", completeBefore); err != nil {
		t.Fatalf("UpsertProducerWatermark() error = %v", err)
	}
	if strings.Count(conn.query, dateTime64NanoPlaceholder) != 2 {
		t.Fatalf("watermark insert does not bind both DateTime64 values as nanoseconds: %s", conn.query)
	}
	if got := conn.args[2].Value; got != completeBefore.UnixNano() {
		t.Fatalf("complete_before arg = %#v, want %d", got, completeBefore.UnixNano())
	}
	if got := conn.args[3].Value; got != now.UnixNano() {
		t.Fatalf("updated_at arg = %#v, want %d", got, now.UnixNano())
	}

	claimedAt := now.Add(-time.Minute + 321*time.Nanosecond)
	state := &metering.SandboxProjectionState{
		SandboxID:      "sandbox-1",
		Namespace:      "default",
		ClaimedAt:      &claimedAt,
		Paused:         true,
		LastObservedAt: now,
	}
	if err := repo.UpsertSandboxProjectionState(context.Background(), state); err != nil {
		t.Fatalf("UpsertSandboxProjectionState() error = %v", err)
	}
	if strings.Count(conn.query, nullableDateTime64NanoPlaceholder) != 4 {
		t.Fatalf("sandbox state insert does not use nullable nanosecond bindings: %s", conn.query)
	}
	if got := conn.args[9].Value; got != int64(1) {
		t.Fatalf("claimed_at presence arg = %#v, want 1", got)
	}
	if got := conn.args[10].Value; got != claimedAt.UnixNano() {
		t.Fatalf("claimed_at arg = %#v, want %d", got, claimedAt.UnixNano())
	}
	if got := conn.args[11].Value; got != int64(0) {
		t.Fatalf("active_since presence arg = %#v, want 0", got)
	}
	if got := conn.args[18].Value; got != now.UnixNano() {
		t.Fatalf("last_observed_at arg = %#v, want %d", got, now.UnixNano())
	}
}

func TestCursorAndSchemaKeepNanosecondAndSequenceContracts(t *testing.T) {
	recordedAt := time.Date(2026, 7, 17, 1, 2, 3, 123456789, time.UTC)
	where, args := cursorWhere(&pageCursor{RecordedAt: recordedAt, Producer: "producer-1", ID: "event-1"}, "event_id")
	if !strings.Contains(where, dateTime64NanoPlaceholder) {
		t.Fatalf("cursor predicate does not preserve DateTime64(9): %s", where)
	}
	if got := args[0]; got != recordedAt.UnixNano() {
		t.Fatalf("cursor recorded_at arg = %#v, want %d", got, recordedAt.UnixNano())
	}

	cfg, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	statements := strings.Join(schemaStatements(cfg), "\n")
	if strings.Count(statements, "ADD COLUMN IF NOT EXISTS sequence Int64") != 2 {
		t.Fatalf("schema does not upgrade both export tables with sequence columns: %s", statements)
	}
	if query := watermarkStatusQuery("metering.watermarks"); !strings.Contains(query, "MAX(complete_before)") || strings.Contains(query, "MIN(complete_before)") {
		t.Fatalf("watermark status query does not use the global delivered frontier: %s", query)
	}
}
