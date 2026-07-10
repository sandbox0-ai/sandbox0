package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestNewMetricsServerExposesDefaultCollectors(t *testing.T) {
	server := newMetricsServer()
	wantAddr := fmt.Sprintf(":%d", config.DefaultSSHGatewayMetricsPort)
	if server.Addr != wantAddr {
		t.Fatalf("metrics server address = %q, want %q", server.Addr, wantAddr)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	server.Handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, metric := range []string{"go_goroutines", "process_cpu_seconds_total"} {
		if !strings.Contains(body, metric) {
			t.Fatalf("GET /metrics response missing %q", metric)
		}
	}
}
