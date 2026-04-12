package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeHandlersUseProbeCheckers(t *testing.T) {
	warmErr := errors.New("warm process is not running")
	server := &Server{
		healthChecker: func() error { return warmErr },
		readyChecker:  func() error { return warmErr },
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
		healthChecker: func() error { return nil },
		readyChecker:  func() error { return nil },
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
