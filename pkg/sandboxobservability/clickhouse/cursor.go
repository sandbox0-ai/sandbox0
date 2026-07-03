package clickhouse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type pageCursor struct {
	OccurredAt time.Time `json:"occurred_at"`
	IngestedAt time.Time `json:"ingested_at"`
	Source     string    `json:"source"`
	EventType  string    `json:"event_type"`
	Cursor     string    `json:"cursor"`
}

func encodePageCursor(event sandboxobservability.Event) (string, error) {
	if event.Cursor == "" {
		return "", fmt.Errorf("event cursor is empty")
	}
	return encodeGenericPageCursor(event.OccurredAt, event.IngestedAt, string(event.Source), string(event.EventType), event.Cursor)
}

func encodeGenericPageCursor(occurredAt, ingestedAt time.Time, source, eventType, cursor string) (string, error) {
	if cursor == "" {
		return "", fmt.Errorf("cursor is empty")
	}
	payload := pageCursor{
		OccurredAt: occurredAt.UTC(),
		IngestedAt: ingestedAt.UTC(),
		Source:     source,
		EventType:  eventType,
		Cursor:     cursor,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodePageCursor(value string) (*pageCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%w: decode", sandboxobservability.ErrInvalidCursor)
	}
	var cursor pageCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return nil, fmt.Errorf("%w: unmarshal", sandboxobservability.ErrInvalidCursor)
	}
	if cursor.OccurredAt.IsZero() || cursor.IngestedAt.IsZero() || cursor.Source == "" || cursor.EventType == "" || cursor.Cursor == "" {
		return nil, fmt.Errorf("%w: missing fields", sandboxobservability.ErrInvalidCursor)
	}
	cursor.OccurredAt = cursor.OccurredAt.UTC()
	cursor.IngestedAt = cursor.IngestedAt.UTC()
	return &cursor, nil
}
