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

// TemplateIdleEvent represents template idle/active counts in a cluster.
type TemplateIdleEvent struct {
	EventBase
	ClusterID   string `json:"cluster_id"`
	TemplateID  string `json:"template_id"`
	IdleCount   int32  `json:"idle_count"`
	ActiveCount int32  `json:"active_count"`
}
