package http

import (
	"net"
	stdhttp "net/http"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestConnStateTrackerTracksTransitions(t *testing.T) {
	registry := prometheus.NewRegistry()
	callback := NewConnStateTracker(ServerConfig{
		ServiceName: "cluster-gateway",
		Registry:    registry,
	}).Wrap(nil)

	first, firstPeer := net.Pipe()
	defer first.Close()
	defer firstPeer.Close()
	second, secondPeer := net.Pipe()
	defer second.Close()
	defer secondPeer.Close()

	callback(first, stdhttp.StateNew)
	callback(second, stdhttp.StateNew)
	assertConnectionStates(t, registry, "cluster_gateway", 2, 0, 0)

	callback(first, stdhttp.StateActive)
	callback(second, stdhttp.StateActive)
	assertConnectionStates(t, registry, "cluster_gateway", 0, 2, 0)

	callback(first, stdhttp.StateIdle)
	assertConnectionStates(t, registry, "cluster_gateway", 0, 1, 1)

	callback(first, stdhttp.StateActive)
	callback(first, stdhttp.StateActive)
	assertConnectionStates(t, registry, "cluster_gateway", 0, 2, 0)

	callback(first, stdhttp.StateHijacked)
	assertConnectionStates(t, registry, "cluster_gateway", 0, 1, 0)

	callback(second, stdhttp.StateClosed)
	assertConnectionStates(t, registry, "cluster_gateway", 0, 0, 0)

	// Repeated terminal notifications must not drive a gauge negative.
	callback(first, stdhttp.StateClosed)
	callback(second, stdhttp.StateHijacked)
	assertConnectionStates(t, registry, "cluster_gateway", 0, 0, 0)
}

func TestConnStateTrackerComposesExistingCallback(t *testing.T) {
	registry := prometheus.NewRegistry()
	var got []stdhttp.ConnState
	callback := NewConnStateTracker(ServerConfig{
		ServiceName: "manager",
		Registry:    registry,
	}).Wrap(func(_ net.Conn, state stdhttp.ConnState) {
		got = append(got, state)
	})

	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	callback(conn, stdhttp.StateNew)
	callback(conn, stdhttp.StateActive)
	callback(conn, stdhttp.StateClosed)

	want := []stdhttp.ConnState{stdhttp.StateNew, stdhttp.StateActive, stdhttp.StateClosed}
	if len(got) != len(want) {
		t.Fatalf("existing callback calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("existing callback call %d = %v, want %v", i, got[i], want[i])
		}
	}
	assertConnectionStates(t, registry, "manager", 0, 0, 0)
}

func TestConnStateTrackerHandlesConcurrentConnections(t *testing.T) {
	registry := prometheus.NewRegistry()
	callback := NewConnStateTracker(ServerConfig{
		ServiceName: "scheduler",
		Registry:    registry,
	}).Wrap(nil)

	const connectionCount = 32
	connections := make([]net.Conn, 0, connectionCount*2)
	var wg sync.WaitGroup
	for range connectionCount {
		conn, peer := net.Pipe()
		connections = append(connections, conn, peer)
		wg.Add(1)
		go func() {
			defer wg.Done()
			callback(conn, stdhttp.StateNew)
			callback(conn, stdhttp.StateActive)
			callback(conn, stdhttp.StateIdle)
			callback(conn, stdhttp.StateClosed)
		}()
	}
	wg.Wait()
	for _, conn := range connections {
		conn.Close()
	}

	assertConnectionStates(t, registry, "scheduler", 0, 0, 0)
}

func TestConnStateTrackerDisabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  ServerConfig
	}{
		{
			name: "metrics disabled",
			cfg: ServerConfig{
				ServiceName:    "manager",
				Registry:       prometheus.NewRegistry(),
				DisableMetrics: true,
			},
		},
		{
			name: "provider disabled",
			cfg: ServerConfig{
				ServiceName: "manager",
				Registry:    prometheus.NewRegistry(),
				Disabled:    true,
			},
		},
		{
			name: "nil registry",
			cfg: ServerConfig{
				ServiceName: "manager",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			next := func(net.Conn, stdhttp.ConnState) { calls++ }
			callback := NewConnStateTracker(tt.cfg).Wrap(next)
			if callback == nil {
				t.Fatal("Wrap returned nil for a non-nil existing callback")
			}
			callback(nil, stdhttp.StateNew)
			if calls != 1 {
				t.Fatalf("existing callback calls = %d, want 1", calls)
			}
			if registry, ok := tt.cfg.Registry.(*prometheus.Registry); ok {
				families, err := registry.Gather()
				if err != nil {
					t.Fatalf("gather metrics: %v", err)
				}
				if len(families) != 0 {
					t.Fatalf("registered metric families = %d, want 0", len(families))
				}
			}
		})
	}
}

func assertConnectionStates(t *testing.T, registry *prometheus.Registry, prefix string, newCount, activeCount, idleCount float64) {
	t.Helper()
	want := map[string]float64{
		"new":    newCount,
		"active": activeCount,
		"idle":   idleCount,
	}
	for state, count := range want {
		got, ok := metricValue(t, registry, prefix+"_http_server_connections", map[string]string{"state": state})
		if !ok || got != count {
			t.Fatalf("connections metric state=%q = %v, ok=%v; want %v", state, got, ok, count)
		}
	}
}
