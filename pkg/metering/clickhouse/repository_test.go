package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	queryRows   []*captureRows
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

func (c *captureConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.query = query
	c.args = append([]driver.NamedValue(nil), args...)
	c.queries = append(c.queries, query)
	c.argsHistory = append(c.argsHistory, append([]driver.NamedValue(nil), args...))
	if len(c.queryRows) == 0 {
		return nil, errors.New("no captured query rows configured")
	}
	rows := c.queryRows[0]
	c.queryRows = c.queryRows[1:]
	return rows, nil
}

type captureRows struct {
	columns []string
	values  [][]driver.Value
	index   int
	err     error
}

func (r *captureRows) Columns() []string {
	return r.columns
}

func (r *captureRows) Close() error {
	return nil
}

func (r *captureRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return r.errOrEOF()
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (r *captureRows) errOrEOF() error {
	if r.err != nil {
		err := r.err
		r.err = nil
		return err
	}
	return io.EOF
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

func TestListTeamWindowsUsesNarrowFinalKeysAndRawSelectedVersions(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	firstRecordedAt := time.Date(2026, 7, 30, 21, 17, 23, 123, time.UTC)
	secondRecordedAt := firstRecordedAt.Add(time.Nanosecond)
	first := testUsageWindow(
		"window-1",
		"producer-1",
		"region-1",
		"team-1",
		firstRecordedAt,
	)
	second := testUsageWindow(
		"window-2",
		"producer-2",
		"region-1",
		"team-1",
		secondRecordedAt,
	)
	concurrentReplacement := testUsageWindow(
		first.WindowID,
		first.Producer,
		first.RegionID,
		"another-team",
		firstRecordedAt.Add(time.Minute),
	)
	conn.queryRows = []*captureRows{
		{
			columns: []string{"recorded_at", "region_id", "producer", "window_id", "version"},
			values: [][]driver.Value{
				{firstRecordedAt, first.RegionID, first.Producer, first.WindowID, int64(11)},
				{secondRecordedAt, second.RegionID, second.Producer, second.WindowID, int64(22)},
			},
		},
		{
			columns: windowDetailColumns(),
			values: [][]driver.Value{
				windowDetailValues(concurrentReplacement, 12),
				windowDetailValues(first, 11),
				windowDetailValues(first, 10),
				windowDetailValues(second, 22),
			},
		},
	}

	windows, next, err := repo.ListTeamWindows(
		context.Background(),
		" team-1 ",
		metering.WindowTypeSandboxEgressBytes,
		"",
		2,
	)
	if err != nil {
		t.Fatalf("ListTeamWindows() error = %v", err)
	}
	if len(windows) != 2 ||
		windows[0].WindowID != first.WindowID ||
		windows[1].WindowID != second.WindowID {
		t.Fatalf("ListTeamWindows() windows = %#v", windows)
	}
	decoded, err := decodeCursor(next)
	if err != nil {
		t.Fatalf("decode next cursor: %v", err)
	}
	if decoded == nil ||
		!decoded.RecordedAt.Equal(secondRecordedAt) ||
		decoded.Producer != second.Producer ||
		decoded.ID != second.WindowID {
		t.Fatalf("next cursor = %#v", decoded)
	}
	if len(conn.queries) != 2 {
		t.Fatalf("window query count = %d, want 2", len(conn.queries))
	}
	if !strings.Contains(conn.queries[0], "FROM `sandbox0_metering`.`usage_windows` FINAL") ||
		strings.Contains(conn.queries[0], "sequence, window_id") ||
		!strings.Contains(conn.queries[0], "recorded_at, region_id, producer, window_id, version") {
		t.Fatalf("key query is not a narrow FINAL lookup: %s", conn.queries[0])
	}
	if strings.Contains(conn.queries[1], " FINAL") ||
		!strings.Contains(conn.queries[1], "data, version") ||
		strings.Count(conn.queries[1], "(?, ?, ?)") != 2 {
		t.Fatalf("detail query is not a bounded raw-version lookup: %s", conn.queries[1])
	}
	if got := len(conn.argsHistory[0]); got != 3 {
		t.Fatalf("key query argument count = %d, want 3", got)
	}
	if got := len(conn.argsHistory[1]); got != 6 {
		t.Fatalf("detail query argument count = %d, want 6", got)
	}
}

func TestLoadWindowDetailsUsesBoundedBatches(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	keys := make([]windowLookupKey, 0, maxWindowDetailBatchSize+1)
	firstBatch := make([][]driver.Value, 0, maxWindowDetailBatchSize)
	for index := 0; index <= maxWindowDetailBatchSize; index++ {
		recordedAt := time.Date(2026, 7, 30, 22, 0, 0, index, time.UTC)
		window := testUsageWindow(
			fmt.Sprintf("window-%03d", index),
			"producer",
			"region",
			"team",
			recordedAt,
		)
		version := uint64(index + 1)
		keys = append(keys, windowLookupKey{
			RecordedAt: recordedAt,
			RegionID:   window.RegionID,
			Producer:   window.Producer,
			WindowID:   window.WindowID,
			Version:    version,
		})
		values := windowDetailValues(window, version)
		if index < maxWindowDetailBatchSize {
			firstBatch = append(firstBatch, values)
			continue
		}
		conn.queryRows = []*captureRows{
			{
				columns: windowDetailColumns(),
				values:  firstBatch,
			},
			{
				columns: windowDetailColumns(),
				values:  [][]driver.Value{values},
			},
		}
	}

	details, err := repo.loadWindowDetails(context.Background(), keys)
	if err != nil {
		t.Fatalf("loadWindowDetails() error = %v", err)
	}
	if len(details) != len(keys) {
		t.Fatalf("loaded detail count = %d, want %d", len(details), len(keys))
	}
	if len(conn.queries) != 2 {
		t.Fatalf("detail query count = %d, want 2", len(conn.queries))
	}
	if got := strings.Count(conn.queries[0], "(?, ?, ?)"); got != maxWindowDetailBatchSize {
		t.Fatalf("first detail batch size = %d, want %d", got, maxWindowDetailBatchSize)
	}
	if got := strings.Count(conn.queries[1], "(?, ?, ?)"); got != 1 {
		t.Fatalf("second detail batch size = %d, want 1", got)
	}
}

func TestListWindowsRejectsMissingSelectedVersion(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	recordedAt := time.Date(2026, 7, 30, 22, 0, 0, 0, time.UTC)
	window := testUsageWindow("window", "producer", "region", "team", recordedAt)
	conn.queryRows = []*captureRows{
		{
			columns: []string{"recorded_at", "region_id", "producer", "window_id", "version"},
			values: [][]driver.Value{{
				recordedAt,
				window.RegionID,
				window.Producer,
				window.WindowID,
				int64(7),
			}},
		},
		{
			columns: windowDetailColumns(),
			values:  [][]driver.Value{windowDetailValues(window, 8)},
		},
	}

	if _, _, err := repo.ListWindows(context.Background(), "", 1); err == nil ||
		!strings.Contains(err.Error(), "disappeared while loading its selected version") {
		t.Fatalf("ListWindows() error = %v, want missing selected version", err)
	}
}

func testUsageWindow(
	windowID string,
	producer string,
	regionID string,
	teamID string,
	recordedAt time.Time,
) *metering.Window {
	return &metering.Window{
		Sequence:    10,
		WindowID:    windowID,
		Producer:    producer,
		RegionID:    regionID,
		WindowType:  metering.WindowTypeSandboxEgressBytes,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		TeamID:      teamID,
		UserID:      "user-1",
		SandboxID:   "sandbox-1",
		TemplateID:  "template-1",
		ClusterID:   "cluster-1",
		WindowStart: recordedAt.Add(-time.Minute),
		WindowEnd:   recordedAt,
		Value:       42,
		Unit:        metering.WindowUnitBytes,
		RecordedAt:  recordedAt,
		Data:        json.RawMessage(`{"source":"test"}`),
	}
}

func windowDetailColumns() []string {
	return []string{
		"sequence", "window_id", "producer", "region_id", "window_type",
		"subject_type", "subject_id",
		"team_id", "user_id",
		"sandbox_id", "volume_id", "snapshot_id",
		"template_id", "cluster_id",
		"window_start", "window_end",
		"value", "unit", "recorded_at", "data", "version",
	}
}

func windowDetailValues(window *metering.Window, version uint64) []driver.Value {
	return []driver.Value{
		window.Sequence, window.WindowID, window.Producer, window.RegionID, window.WindowType,
		window.SubjectType, window.SubjectID,
		window.TeamID, window.UserID,
		window.SandboxID, window.VolumeID, window.SnapshotID,
		window.TemplateID, window.ClusterID,
		window.WindowStart, window.WindowEnd,
		window.Value, window.Unit, window.RecordedAt, string(window.Data), int64(version),
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
	if !strings.Contains(
		statements,
		"ADD INDEX IF NOT EXISTS usage_windows_recorded_at_minmax recorded_at TYPE minmax",
	) {
		t.Fatalf("schema does not add the recorded_at window skip index: %s", statements)
	}
	if query := watermarkStatusQuery("metering.watermarks"); !strings.Contains(query, "MAX(complete_before)") || strings.Contains(query, "MIN(complete_before)") {
		t.Fatalf("watermark status query does not use the global delivered frontier: %s", query)
	}
}
