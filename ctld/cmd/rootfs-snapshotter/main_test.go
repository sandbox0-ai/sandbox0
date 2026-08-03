package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type staticReadiness struct {
	err error
}

func (r staticReadiness) Ready(context.Context) error { return r.err }

func TestHealthHandlerSeparatesLivenessAndReadiness(t *testing.T) {
	tests := []struct {
		name   string
		ready  bool
		path   string
		status int
	}{
		{name: "live before ready", path: "/healthz", status: http.StatusOK},
		{name: "not ready", path: "/readyz", status: http.StatusServiceUnavailable},
		{name: "ready", ready: true, path: "/readyz", status: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			readiness := staticReadiness{err: errors.New("not ready")}
			if tt.ready {
				readiness.err = nil
			}
			healthHandler(readiness).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if recorder.Code != tt.status {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.status)
			}
		})
	}
}

type staticServiceReadiness bool

func (r staticServiceReadiness) Ready() bool { return bool(r) }

type staticRegistrationReadiness struct {
	err error
}

func (r staticRegistrationReadiness) Registered(context.Context) error { return r.err }

func TestCombinedReadinessRequiresServiceAndContainerdRegistration(t *testing.T) {
	registrationError := errors.New("proxy plugin missing")
	tests := []struct {
		name         string
		serviceReady bool
		registered   error
		wantReady    bool
	}{
		{name: "service unavailable"},
		{name: "registration missing", serviceReady: true, registered: registrationError},
		{name: "ready", serviceReady: true, wantReady: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready := combinedReadiness{
				service:      staticServiceReadiness(tt.serviceReady),
				registration: staticRegistrationReadiness{err: tt.registered},
			}
			err := ready.Ready(context.Background())
			if (err == nil) != tt.wantReady {
				t.Fatalf("Ready() error = %v, wantReady = %v", err, tt.wantReady)
			}
		})
	}
}
