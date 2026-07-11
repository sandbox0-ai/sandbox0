package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestLifecycleBarrierReportsBlockingOperation(t *testing.T) {
	barrier := newLifecycleBarrier()
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/contexts/ctx-stuck", nil)
	release, ok := barrier.enter(request)
	if !ok {
		t.Fatal("enter() rejected request before barrier activation")
	}
	defer release()

	barrierRequest := httptest.NewRequest(http.MethodPut, "/api/v1/lifecycle/barrier", nil)
	response, err := barrier.setActive(barrierRequest, lifecycleBarrierRequest{Active: true, WaitTimeoutMS: 20})
	if err == nil {
		t.Fatal("setActive() succeeded with a blocking operation")
	}
	if !strings.Contains(err.Error(), "DELETE /api/v1/contexts/ctx-stuck") {
		t.Fatalf("setActive() error = %q, want blocking operation details", err)
	}
	if response.InFlight != 1 || len(response.Operations) != 1 {
		t.Fatalf("setActive() response = %#v, want one blocking operation", response)
	}
	operation := response.Operations[0]
	if operation.Method != http.MethodDelete || operation.Path != "/api/v1/contexts/ctx-stuck" {
		t.Fatalf("blocking operation = %#v", operation)
	}
}
