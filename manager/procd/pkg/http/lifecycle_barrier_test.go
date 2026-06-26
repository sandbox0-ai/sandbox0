package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProcdRequestMutatesRuntimeSkipsLifecycleControlEndpoints(t *testing.T) {
	for _, path := range []string{
		"/api/v1/lifecycle/barrier",
		"/api/v1/sandbox/pause",
		"/api/v1/sandbox/resume",
	} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		if procdRequestMutatesRuntime(req) {
			t.Fatalf("procdRequestMutatesRuntime(%s) = true, want false", path)
		}
	}
}

func TestProcdRequestMutatesRuntimeBlocksMutatingAPIRequests(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/write", nil)
	if !procdRequestMutatesRuntime(req) {
		t.Fatal("procdRequestMutatesRuntime(write) = false, want true")
	}
}
