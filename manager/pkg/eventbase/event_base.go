// Package eventbase constructs manager-owned pubsub event metadata.
package eventbase

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/clock"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
)

func New(clk *clock.Clock) pubsub.EventBase {
	event := pubsub.EventBase{Clock: clk}
	if clk != nil {
		event.Timestamp = clk.Now().UTC()
	} else {
		event.Timestamp = time.Now().UTC()
	}
	return event
}
