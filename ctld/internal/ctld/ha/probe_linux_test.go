//go:build linux

package ha

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestProbeReadinessTracksPrimaryStandbyAndService(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe, err := StartProbeServer(ctx, filepath.Join(t.TempDir(), "probe.sock"), coordinator)
	if err != nil {
		t.Fatalf("StartProbeServer() error = %v", err)
	}
	defer probe.Close()

	coordinator.setState(func(state *State) { *state = State{Role: RoleStandby, Synchronized: true} })
	if err := RunProbe(ctx, probe.socket, "ready", ":8095"); err != nil {
		t.Fatalf("standby readiness error = %v", err)
	}

	coordinator.setState(func(state *State) { *state = State{Role: RolePrimary, Synchronized: true} })
	if err := RunProbe(ctx, probe.socket, "ready", ":8095"); err == nil {
		t.Fatal("primary readiness succeeded before service was ready")
	}
	probe.SetServiceReady(true)
	if err := RunProbe(ctx, probe.socket, "ready", ":8095"); err != nil {
		t.Fatalf("primary readiness error = %v", err)
	}
}

func TestLiveProbeChecksActiveHTTPServer(t *testing.T) {
	coordinator := newTestCoordinator(t, t.TempDir(), "a")
	coordinator.setState(func(state *State) { *state = State{Role: RolePrimary, Synchronized: true} })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe, err := StartProbeServer(ctx, filepath.Join(t.TempDir(), "probe.sock"), coordinator)
	if err != nil {
		t.Fatalf("StartProbeServer() error = %v", err)
	}
	defer probe.Close()
	if err := RunProbe(ctx, probe.socket, "live", "127.0.0.1:1"); err != nil {
		t.Fatalf("starting primary live probe error = %v", err)
	}
	probe.SetServiceReady(true)

	active := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer active.Close()
	if err := RunProbe(ctx, probe.socket, "live", active.Listener.Addr().String()); err != nil {
		t.Fatalf("live probe error = %v", err)
	}
	active.Close()
	if err := RunProbe(ctx, probe.socket, "live", active.Listener.Addr().String()); err == nil {
		t.Fatal("live probe succeeded after active HTTP server stopped")
	}
}
