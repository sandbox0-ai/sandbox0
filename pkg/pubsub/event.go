package pubsub

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/clock"
)

// EventBase carries the authoritative event timestamp and an optional local
// clock used by producers before serialization.
type EventBase struct {
	Clock     *clock.Clock `json:"-"`
	Timestamp time.Time    `json:"ts"`
}
