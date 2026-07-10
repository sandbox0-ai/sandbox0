package http

import (
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/promutil"
)

var trackedConnStates = [...]string{"new", "active", "idle"}

// ConnStateTracker reports the current number of HTTP server connections in
// each non-terminal state. A tracker can be shared by one or more HTTP servers
// that use the same metrics registry and service name.
type ConnStateTracker struct {
	mu          sync.Mutex
	states      map[net.Conn]http.ConnState
	connections *prometheus.GaugeVec
}

// NewConnStateTracker creates an HTTP server connection state tracker. The
// returned tracker is a no-op when metrics are disabled or no registry is
// configured.
func NewConnStateTracker(cfg ServerConfig) *ConnStateTracker {
	tracker := &ConnStateTracker{}
	if cfg.Disabled || cfg.DisableMetrics || cfg.Registry == nil || strings.TrimSpace(cfg.ServiceName) == "" {
		return tracker
	}

	prefix := promutil.MetricPrefix(cfg.ServiceName)
	tracker.states = make(map[net.Conn]http.ConnState)
	tracker.connections = promutil.RegisterGaugeVec(cfg.Registry, prometheus.GaugeOpts{
		Name: prefix + "_http_server_connections",
		Help: "Current number of HTTP server connections by state",
	}, []string{"state"})
	for _, state := range trackedConnStates {
		tracker.connections.WithLabelValues(state)
	}
	return tracker
}

// Wrap returns a net/http ConnState callback that updates connection metrics
// before invoking next. When tracking is disabled, Wrap returns next unchanged.
func (t *ConnStateTracker) Wrap(next func(net.Conn, http.ConnState)) func(net.Conn, http.ConnState) {
	if t == nil || t.connections == nil {
		return next
	}
	return func(conn net.Conn, state http.ConnState) {
		t.track(conn, state)
		if next != nil {
			next(conn, state)
		}
	}
}

func (t *ConnStateTracker) track(conn net.Conn, state http.ConnState) {
	if conn == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	previous, ok := t.states[conn]
	if ok && previous == state {
		return
	}
	if ok {
		t.connections.WithLabelValues(connStateLabel(previous)).Dec()
	}

	if label := connStateLabel(state); label != "" {
		t.states[conn] = state
		t.connections.WithLabelValues(label).Inc()
		return
	}
	delete(t.states, conn)
}

func connStateLabel(state http.ConnState) string {
	switch state {
	case http.StateNew:
		return "new"
	case http.StateActive:
		return "active"
	case http.StateIdle:
		return "idle"
	default:
		return ""
	}
}
