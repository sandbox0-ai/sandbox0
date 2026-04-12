package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

func TestProbeHandlersUseProbeCheckers(t *testing.T) {
	server := &Server{
		probeRunner: func(kind sandboxprobe.Kind) sandboxprobe.Response {
			return sandboxprobe.Failed(kind, "WarmProcessNotRunning", "warm process is not running", nil)
		},
	}

	for _, tt := range []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{name: "health", path: "/healthz", handler: server.healthHandler},
		{name: "ready", path: "/readyz", handler: server.readyHandler},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			tt.handler(recorder, req)

			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestProbeHandlersSucceedWhenProbeCheckerPasses(t *testing.T) {
	server := &Server{
		probeRunner: func(kind sandboxprobe.Kind) sandboxprobe.Response {
			return sandboxprobe.Passed(kind, "SandboxProbePassed", "sandbox probe passed", nil)
		},
	}

	for _, tt := range []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{name: "health", path: "/healthz", handler: server.healthHandler},
		{name: "ready", path: "/readyz", handler: server.readyHandler},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			tt.handler(recorder, req)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
			}
		})
	}
}

func TestSandboxProbeHandlerWritesProbeResponse(t *testing.T) {
	server := &Server{
		probeRunner: func(kind sandboxprobe.Kind) sandboxprobe.Response {
			return sandboxprobe.Failed(kind, "WarmProcessNotRunning", "warm process is not running", nil)
		},
	}
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/sandbox-probes/readiness", nil)
	req = mux.SetURLVars(req, map[string]string{"kind": "readiness"})

	server.sandboxProbeHandler(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	var result sandboxprobe.Response
	if err := json.NewDecoder(recorder.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Kind != sandboxprobe.KindReadiness || result.Status != sandboxprobe.StatusFailed {
		t.Fatalf("result = %#v, want failed readiness", result)
	}
}
