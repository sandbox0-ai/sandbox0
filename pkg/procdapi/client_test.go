package procdapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func TestNewProcdClientUsesDefaultTimeout(t *testing.T) {
	client := NewProcdClient(ProcdClientConfig{})
	if client == nil || client.httpClient == nil {
		t.Fatal("NewProcdClient() returned an unconfigured client")
	}
	if got := client.httpClient.Timeout; got != 30*time.Second {
		t.Fatalf("timeout = %s, want 30s", got)
	}
}

func TestStatsSendsInternalTokenAndDecodesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/sandbox/stats" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Internal-Token"); got != "token-a" {
			t.Fatalf("X-Internal-Token = %q", got)
		}
		if err := spec.WriteSuccess(w, http.StatusOK, StatsResponse{
			SandboxResourceUsage: SandboxResourceUsage{ContextCount: 3},
		}); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer server.Close()

	response, err := NewProcdClient(ProcdClientConfig{}).Stats(context.Background(), server.URL, "token-a")
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if response.ContextCount != 3 {
		t.Fatalf("ContextCount = %d, want 3", response.ContextCount)
	}
}

func TestStatsReturnsProcdErrorMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "procd unavailable"); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer server.Close()

	_, err := NewProcdClient(ProcdClientConfig{}).Stats(context.Background(), server.URL, "")
	if err == nil || err.Error() != "stats failed: procd unavailable" {
		t.Fatalf("Stats() error = %v", err)
	}
}

func TestProbeCommandReadyReturnsProcessAndRawResponseDigest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != CommandReadyProbePath {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Internal-Token"); got != "probe-token" {
			t.Fatalf("X-Internal-Token = %q", got)
		}
		if err := spec.WriteSuccess(w, http.StatusOK, CommandReadyProbeResponse{
			InstanceID: "procd-instance-1", Status: "ready",
		}); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer server.Close()

	result, err := NewProcdClient(ProcdClientConfig{}).ProbeCommandReady(
		context.Background(), server.URL, "probe-token",
	)
	if err != nil {
		t.Fatalf("ProbeCommandReady() error = %v", err)
	}
	if result.InstanceID != "procd-instance-1" || result.Status != "ready" || len(result.ResponseBodyDigest) != 64 {
		t.Fatalf("result = %+v", result)
	}
}
