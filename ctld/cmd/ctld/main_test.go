package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCtldShutdownBudgetFitsDeploymentGracePeriod(t *testing.T) {
	const deployedTerminationGrace = 45 * time.Second
	shutdownBudget := httpShutdownTimeout + runtimeMetricsShutdownTimeout +
		networkRuntimeShutdownTimeout + nomadRuntimeShutdownTimeout
	assert.LessOrEqual(t, shutdownBudget+shutdownGraceMargin, deployedTerminationGrace)
	assert.Equal(t, minimumTerminationGrace, shutdownBudget+shutdownGraceMargin)
}

func TestCtldHealthEndpoints(t *testing.T) {
	server := newHTTPServer(":0", nil)
	for _, path := range []string{"/healthz", "/readyz"} {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "ok", recorder.Body.String())
	}
}

func TestHealthControllerExposesPrimaryState(t *testing.T) {
	healthy, ready := false, false
	server := newHTTPServer(":0", healthController{
		healthy: func() bool { return healthy },
		ready:   func() bool { return ready },
	})
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusServiceUnavailable},
		{path: "/readyz", want: http.StatusServiceUnavailable},
	} {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		assert.Equal(t, test.want, recorder.Code)
	}
	healthy, ready = true, true
	for _, path := range []string{"/healthz", "/readyz"} {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusOK, recorder.Code)
	}
}

func TestCtldHTTPDoesNotExposeLegacyRuntimeControl(t *testing.T) {
	server := newHTTPServer(":0", nil)
	for _, path := range []string{
		"/api/v1/rootfs/inspect",
		"/api/v1/pods/default/sandbox/probes/readiness",
		"/api/v1/sandboxes/sandbox-1/probes/liveness",
		"/api/v1/runtime/watch",
	} {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
		assert.Equal(t, http.StatusNotFound, recorder.Code)
	}
}
