package clickhouse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

type pageCursor struct {
	RecordedAt time.Time `json:"recorded_at"`
	Producer   string    `json:"producer"`
	ID         string    `json:"id"`
}

func encodeCursor(recordedAt time.Time, producer, id string) (string, error) {
	if recordedAt.IsZero() || producer == "" || id == "" {
		return "", fmt.Errorf("cursor fields are required")
	}
	payload := pageCursor{
		RecordedAt: recordedAt.UTC(),
		Producer:   producer,
		ID:         id,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeCursor(value string) (*pageCursor, error) {
	if value == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	var cursor pageCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	if cursor.RecordedAt.IsZero() || cursor.Producer == "" || cursor.ID == "" {
		return nil, fmt.Errorf("invalid cursor")
	}
	cursor.RecordedAt = cursor.RecordedAt.UTC()
	return &cursor, nil
}
