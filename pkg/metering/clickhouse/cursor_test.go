package clickhouse

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func TestNextPageCursorAdvancesForPartialPages(t *testing.T) {
	recordedAt := time.Date(2026, 7, 11, 21, 0, 0, 123, time.UTC)

	eventCursor, err := nextEventCursor([]*metering.Event{{
		EventID:    "event-1",
		Producer:   "manager.sandbox_lifecycle",
		RecordedAt: recordedAt,
	}})
	if err != nil {
		t.Fatalf("nextEventCursor() error = %v", err)
	}
	assertPageCursor(t, eventCursor, recordedAt, "manager.sandbox_lifecycle", "event-1")

	windowCursor, err := nextWindowCursor([]*metering.Window{{
		WindowID:   "window-1",
		Producer:   "manager.sandbox_lifecycle",
		RecordedAt: recordedAt,
	}})
	if err != nil {
		t.Fatalf("nextWindowCursor() error = %v", err)
	}
	assertPageCursor(t, windowCursor, recordedAt, "manager.sandbox_lifecycle", "window-1")
}

func TestNextPageCursorIsEmptyForEmptyPages(t *testing.T) {
	if cursor, err := nextEventCursor(nil); err != nil || cursor != "" {
		t.Fatalf("nextEventCursor(nil) = %q, %v; want empty cursor", cursor, err)
	}
	if cursor, err := nextWindowCursor(nil); err != nil || cursor != "" {
		t.Fatalf("nextWindowCursor(nil) = %q, %v; want empty cursor", cursor, err)
	}
}

func assertPageCursor(t *testing.T, encoded string, recordedAt time.Time, producer, id string) {
	t.Helper()
	cursor, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decodeCursor() error = %v", err)
	}
	if !cursor.RecordedAt.Equal(recordedAt) || cursor.Producer != producer || cursor.ID != id {
		t.Fatalf("cursor = %+v, want recorded_at=%s producer=%q id=%q", cursor, recordedAt, producer, id)
	}
}
