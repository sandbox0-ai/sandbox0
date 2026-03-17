package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func TestHTTPEgressAuthResolverResolve(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/resolve" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		expiresAt := time.Now().UTC().Add(time.Minute)
		if err := spec.WriteSuccess(w, http.StatusOK, &egressauth.ResolveResponse{
			AuthRef:   "example-api",
			Headers:   map[string]string{"Authorization": "Bearer test-token"},
			ExpiresAt: &expiresAt,
		}); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer ts.Close()

	resolver := newHTTPEgressAuthResolver(ts.URL, time.Second)
	resp, err := resolver.Resolve(t.Context(), &egressauth.ResolveRequest{
		SandboxID:   "sbx_123",
		TeamID:      "team_123",
		AuthRef:     "example-api",
		Destination: "api.example.com",
		Protocol:    "http",
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resp == nil || resp.AuthRef != "example-api" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer test-token" {
		t.Fatalf("authorization header = %q", got)
	}
}
