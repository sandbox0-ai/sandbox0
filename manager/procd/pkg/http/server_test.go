package http

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/procdconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCommandReadyProbeReturnsStableProcessIdentity(t *testing.T) {
	instanceID := uuid.NewString()
	server := &Server{instanceID: instanceID}
	for attempt := 0; attempt < 2; attempt++ {
		recorder := httptest.NewRecorder()
		server.commandReadyProbeHandler(recorder, httptest.NewRequest(http.MethodPut, "/api/v1/runtime/command-ready-probe", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("attempt %d status = %d, want %d", attempt, recorder.Code, http.StatusOK)
		}
		response, apiErr, err := spec.DecodeResponse[procdapi.CommandReadyProbeResponse](recorder.Body)
		if err != nil || apiErr != nil {
			t.Fatalf("attempt %d decode response: response=%+v apiErr=%+v err=%v", attempt, response, apiErr, err)
		}
		if response.InstanceID != instanceID || response.Status != "ready" {
			t.Fatalf("attempt %d response = %+v", attempt, response)
		}
	}
}

func TestCommandReadyProbeFailsWithoutProcessIdentity(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	server.commandReadyProbeHandler(recorder, httptest.NewRequest(http.MethodPut, "/api/v1/runtime/command-ready-probe", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestCommandReadyProbeRouteUsesPublicContractPath(t *testing.T) {
	server := NewServer(
		&procdconfig.Config{},
		nil,
		nil,
		nil,
		nil,
		nil,
		zap.NewNop(),
		nil,
		nil,
		nil,
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, procdapi.CommandReadyProbePath, nil)
	server.router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want routed authentication failure %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestProbeHandlersUseProbeCheckers(t *testing.T) {
	server := &Server{
		probeRunner: func(kind sandboxprobe.Kind) sandboxprobe.Response {
			return sandboxprobe.Failed(kind, "SandboxProbeFailed", "sandbox probe failed", nil)
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
			return sandboxprobe.Failed(kind, "SandboxProbeFailed", "sandbox probe failed", nil)
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

func TestRuntimeReadyMiddlewareFailsClosed(t *testing.T) {
	server := &Server{
		runtimeGate: func() (bool, string) {
			return false, "runtime assignment is recovering"
		},
	}
	nextCalled := false
	handler := server.runtimeReadyMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		nextCalled = true
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/contexts", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if nextCalled {
		t.Fatal("runtime-gated handler was called before the assignment was ready")
	}
}

func TestRuntimeReadyMiddlewareGatesLifecycleControlsUntilActivation(t *testing.T) {
	ready := false
	server := &Server{
		runtimeGate: func() (bool, string) {
			return ready, "runtime assignment is recovering"
		},
	}
	for _, path := range []string{
		"/api/v1/lifecycle/barrier",
		"/api/v1/sandbox/pause",
		"/api/v1/sandbox/resume",
		"/api/v1/webhook/publish",
	} {
		t.Run(path, func(t *testing.T) {
			ready = false
			nextCalled := false
			handler := server.runtimeReadyMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusNoContent)
			}))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
			require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
			require.False(t, nextCalled)
			ready = true
			recorder = httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))

			if recorder.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
			}
			if !nextCalled {
				t.Fatal("lifecycle recovery control was blocked by runtime readiness")
			}
		})
	}
}

func TestServerDoesNotExposeInitializeCompatibilityEndpoint(t *testing.T) {
	server := NewServer(
		&procdconfig.Config{},
		nil,
		nil,
		nil,
		nil,
		nil,
		zap.NewNop(),
		nil,
		nil,
		nil,
	)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/initialize", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	server.router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestListenerAcceptsProbesBeforeActivationWithoutPublishingReadiness(t *testing.T) {
	var active atomic.Bool
	server := NewServer(&procdconfig.Config{}, nil, nil, nil, nil, nil, zap.NewNop(), nil,
		func(kind sandboxprobe.Kind) sandboxprobe.Response {
			if active.Load() {
				return sandboxprobe.Passed(kind, "Ready", "ready", nil)
			}
			return sandboxprobe.Suspended(kind, "Activating", "recovering sessions", nil)
		}, func() (bool, string) { return active.Load(), "recovering sessions" })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
		require.ErrorIs(t, <-done, http.ErrServerClosed)
	})
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/readyz")
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	// Use the same gate as authenticated command-ready requests. TCP acceptance
	// must not invoke the command handler before activation commits.
	recorder := httptest.NewRecorder()
	server.runtimeReadyMiddleware(http.HandlerFunc(server.commandReadyProbeHandler)).ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, procdapi.CommandReadyProbePath, nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	active.Store(true)
	response, err = client.Get("http://" + listener.Addr().String() + "/readyz")
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	recorder = httptest.NewRecorder()
	server.runtimeReadyMiddleware(http.HandlerFunc(server.commandReadyProbeHandler)).ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, procdapi.CommandReadyProbePath, nil))
	require.Equal(t, http.StatusOK, recorder.Code)
}
