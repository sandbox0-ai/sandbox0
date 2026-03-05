package pubsub

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/clock"
)

// TemplateIdleChannel is the PostgreSQL NOTIFY channel for template stats updates.
const TemplateIdleChannel = "template_idle_events"

// EventBase provides a shared clock for pubsub events.
type EventBase struct {
	Clock     *clock.Clock `json:"-"`
	Timestamp time.Time    `json:"ts"`
}

// NewEventBase creates a base event with a synchronized clock.
func NewEventBase(clk *clock.Clock) EventBase {
	eb := EventBase{Clock: clk}
	if clk != nil {
		eb.Timestamp = clk.Now().UTC()
	} else {
		eb.Timestamp = time.Now().UTC()
	}
	return eb
}

// TemplateIdleEvent represents template idle/active counts in a cluster.
type TemplateIdleEvent struct {
	EventBase
	ClusterID   string `json:"cluster_id"`
	TemplateID  string `json:"template_id"`
	IdleCount   int32  `json:"idle_count"`
	ActiveCount int32  `json:"active_count"`
}
