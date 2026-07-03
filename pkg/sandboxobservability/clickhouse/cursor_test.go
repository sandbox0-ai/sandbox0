package clickhouse

import (
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestPageCursorRoundTrip(t *testing.T) {
	event := sandboxobservability.Event{
		OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 4, time.FixedZone("offset", 8*60*60)),
		IngestedAt: time.Date(2026, 7, 1, 1, 2, 4, 5, time.UTC),
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
		Cursor:     "netd:cursor:1",
	}

	encoded, err := encodePageCursor(event)
	if err != nil {
		t.Fatalf("encodePageCursor() error = %v", err)
	}
	decoded, err := decodePageCursor(encoded)
	if err != nil {
		t.Fatalf("decodePageCursor() error = %v", err)
	}
	if !decoded.OccurredAt.Equal(event.OccurredAt) ||
		!decoded.IngestedAt.Equal(event.IngestedAt) ||
		decoded.Source != string(event.Source) ||
		decoded.EventType != string(event.EventType) ||
		decoded.Cursor != event.Cursor {
		t.Fatalf("decoded cursor = %+v", decoded)
	}
	if decoded.OccurredAt.Location() != time.UTC || decoded.IngestedAt.Location() != time.UTC {
		t.Fatalf("decoded cursor times must be UTC: %+v", decoded)
	}
}

func TestPageCursorRejectsInvalidValue(t *testing.T) {
	_, err := decodePageCursor("not-base64")
	if !errors.Is(err, sandboxobservability.ErrInvalidCursor) {
		t.Fatalf("decodePageCursor() error = %v, want ErrInvalidCursor", err)
	}
}
