package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func TestRunAcceptsSynchronizedRegionalClaimDistribution(t *testing.T) {
	var claims atomic.Int64
	var deletes atomic.Int64
	var mu sync.Mutex
	requestIDs := map[string]struct{}{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch request.Method {
		case http.MethodPost:
			index := claims.Add(1)
			requestID := request.Header.Get("X-Request-ID")
			mu.Lock()
			if _, duplicate := requestIDs[requestID]; duplicate || requestID == "" {
				mu.Unlock()
				writer.WriteHeader(http.StatusConflict)
				return
			}
			requestIDs[requestID] = struct{}{}
			mu.Unlock()
			writer.Header().Add("Server-Timing", "proxy;dur=1")
			writer.Header().Add("Server-Timing", fmt.Sprintf("sandbox0-command-ready;dur=%d.000", 100+index))
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: fmt.Sprintf("sandbox-%d", index)})
		case http.MethodDelete:
			deletes.Add(1)
			_ = spec.WriteSuccess(writer, http.StatusAccepted, struct{}{})
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 2, concurrency: 2, requestTimeout: time.Second, hardLimit: time.Second,
		p50Target: 500 * time.Millisecond, cleanup: true, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passed || result.CommandReady.Count != 4 || result.CommandReady.P50 != 102*time.Millisecond ||
		result.CommandReady.P99 != 104*time.Millisecond || result.Errors != 0 || result.SLOMisses != 0 {
		t.Fatalf("report = %+v", result)
	}
	if claims.Load() != 4 || deletes.Load() != 4 {
		t.Fatalf("claims=%d deletes=%d", claims.Load(), deletes.Load())
	}
}

func TestRunRejectsAnySuccessfulSampleBeyondHardLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=1000.001")
		writer.Header().Set("Sandbox0-Command-Ready-SLO", "missed")
		_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-slow"})
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: time.Second,
		p50Target: 500 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.SLOMisses != 1 {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestCommandReadyDurationRequiresExactMetric(t *testing.T) {
	duration, err := commandReadyDuration("other;dur=1.2, sandbox0-command-ready;desc=ready;dur=499.125")
	if err != nil || duration != 499125*time.Microsecond {
		t.Fatalf("duration=%s error=%v", duration, err)
	}
	if _, err := commandReadyDuration("other;dur=1"); err == nil {
		t.Fatal("missing command-ready metric was accepted")
	}
	for _, value := range []string{
		"sandbox0-command-ready",
		"sandbox0-command-ready;dur=NaN",
		"sandbox0-command-ready;dur=+Inf",
		"sandbox0-command-ready;dur=1;dur=2",
		"sandbox0-command-ready;dur=1,sandbox0-command-ready;dur=1",
	} {
		if _, err := commandReadyDuration(value); err == nil {
			t.Fatalf("invalid Server-Timing %q was accepted", value)
		}
	}
}

func TestConfigRejectsNonClaimEndpoint(t *testing.T) {
	cfg := config{
		endpoint: "https://example.test/api/v1/sandboxes/extra", token: "token", body: []byte(`{}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: time.Second,
		p50Target: 500 * time.Millisecond,
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("non-claim endpoint was accepted")
	}
}
