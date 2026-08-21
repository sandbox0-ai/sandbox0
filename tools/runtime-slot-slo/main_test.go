package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
		case http.MethodGet:
			_ = spec.WriteError(writer, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 2, concurrency: 2, requestTimeout: time.Second, hardLimit: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		p50Target: 500 * time.Millisecond, client: server.Client(),
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
	bodyDigest := sha256.Sum256(cfg.body)
	if result.Version != 5 || len(result.ExecutableSHA256) != sha256.Size*2 ||
		result.ClaimBodySHA256 != fmt.Sprintf("%x", bodyDigest[:]) ||
		result.RequestTimeout != cfg.requestTimeout || result.CleanupTimeout != cfg.cleanupTimeout ||
		result.CleanupPoll != cfg.cleanupPoll || result.BatchSettle != cfg.settle || result.Cleanup.Count != 4 {
		t.Fatalf("cleanup convergence report = %+v", result)
	}
}

func TestRunRejectsCleanupThatNeverConverges(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=100")
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-stuck"})
		case http.MethodDelete, http.MethodGet:
			_ = spec.WriteSuccess(writer, http.StatusOK, struct{}{})
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second,
		cleanupTimeout: 30 * time.Millisecond, cleanupPoll: 10 * time.Millisecond,
		hardLimit: time.Second, p50Target: 500 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.CleanupErrors != 1 || result.Cleanup.Count != 0 ||
		result.Samples[0].CleanupError == "" {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestRunRejectsNoncanonicalCleanupAbsence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=100")
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-1"})
		case http.MethodDelete:
			_ = spec.WriteSuccess(writer, http.StatusOK, struct{}{})
		case http.MethodGet:
			writer.WriteHeader(http.StatusNotFound)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		hardLimit: time.Second, p50Target: 500 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.CleanupErrors != 1 ||
		result.Samples[0].CleanupError != "sandbox sandbox-1 absence response is not a canonical not_found envelope" {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestRunDoesNotStartNextBatchBeforeCleanupConverges(t *testing.T) {
	var claims atomic.Int64
	var firstGets atomic.Int64
	var firstAbsent atomic.Bool
	var overlap atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			index := claims.Add(1)
			if index == 2 && !firstAbsent.Load() {
				overlap.Store(true)
			}
			writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=100")
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: fmt.Sprintf("sandbox-%d", index)})
		case http.MethodDelete:
			_ = spec.WriteSuccess(writer, http.StatusOK, struct{}{})
		case http.MethodGet:
			if request.URL.Path == "/api/v1/sandboxes/sandbox-1" && firstGets.Add(1) == 1 {
				_ = spec.WriteSuccess(writer, http.StatusOK, struct{}{})
				return
			}
			if request.URL.Path == "/api/v1/sandboxes/sandbox-1" {
				firstAbsent.Store(true)
			}
			_ = spec.WriteError(writer, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 2, concurrency: 1, requestTimeout: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		hardLimit: time.Second, p50Target: 500 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err != nil || !result.Passed || overlap.Load() || !firstAbsent.Load() || firstGets.Load() != 2 {
		t.Fatalf("report=%+v error=%v overlap=%v first_absent=%v first_gets=%d",
			result, err, overlap.Load(), firstAbsent.Load(), firstGets.Load())
	}
}

func TestRunRejectsReplayedSandboxIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=100")
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-replayed"})
		case http.MethodDelete:
			_ = spec.WriteSuccess(writer, http.StatusAccepted, struct{}{})
		case http.MethodGet:
			_ = spec.WriteError(writer, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 2, concurrency: 1, requestTimeout: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		hardLimit: time.Second, p50Target: 500 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.Errors != 1 ||
		result.Samples[1].Error != "claim sandbox_id duplicates sample 0" {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestRunRejectsAnySuccessfulSampleBeyondHardLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=1000.001")
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "missed")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-slow"})
		case http.MethodDelete:
			_ = spec.WriteSuccess(writer, http.StatusAccepted, struct{}{})
		case http.MethodGet:
			_ = spec.WriteError(writer, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		p50Target: 500 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.SLOMisses != 1 {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestRunRejectsSlowPublicRoundTripDespiteFastReportedCommandReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodPost:
			time.Sleep(100 * time.Millisecond)
			writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=1")
			writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-underreported"})
		case http.MethodDelete:
			_ = spec.WriteSuccess(writer, http.StatusAccepted, struct{}{})
		case http.MethodGet:
			_ = spec.WriteError(writer, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: 50 * time.Millisecond,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		p50Target: 25 * time.Millisecond, client: server.Client(),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.SLOMisses != 0 || result.WallMisses != 1 ||
		result.Wall.Count != 1 || result.Wall.Max <= cfg.hardLimit {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestAcceptanceClientDoesNotFollowClaimRedirects(t *testing.T) {
	var redirectedRequests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		writer.Header().Set("Server-Timing", "sandbox0-command-ready;dur=1")
		writer.Header().Set("Sandbox0-Command-Ready-SLO", "met")
		_ = spec.WriteSuccess(writer, http.StatusCreated, claimResponse{SandboxID: "sandbox-redirected"})
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+"/api/v1/sandboxes", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	cfg := config{
		endpoint: redirect.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: time.Second,
		p50Target: 500 * time.Millisecond, client: newHTTPClient(1, time.Second),
	}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.Errors != 1 || redirectedRequests.Load() != 0 {
		t.Fatalf("report=%+v error=%v redirected_requests=%d", result, err, redirectedRequests.Load())
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

func TestClaimIdentityAndSLOHeaderRequireCanonicalWireValues(t *testing.T) {
	for _, value := range []string{"sandbox-1", "rs-mrswmylvnr2a-default-abcde"} {
		if !validClaimSandboxID(value) {
			t.Fatalf("canonical sandbox ID %q was rejected", value)
		}
	}
	for _, value := range []string{"", " sandbox-1", "sandbox/1", "sandbox%2f1", ".", "..", strings.Repeat("a", 513)} {
		if validClaimSandboxID(value) {
			t.Fatalf("unsafe sandbox ID %q was accepted", value)
		}
	}
	within, err := commandReadyWithinSLO([]string{"met"})
	if err != nil || !within {
		t.Fatalf("met header = %v, %v", within, err)
	}
	within, err = commandReadyWithinSLO([]string{"missed"})
	if err != nil || within {
		t.Fatalf("missed header = %v, %v", within, err)
	}
	for _, values := range [][]string{nil, {"met", "missed"}, {"met, missed"}, {"unknown"}} {
		if _, err := commandReadyWithinSLO(values); err == nil {
			t.Fatalf("ambiguous SLO headers %#v were accepted", values)
		}
	}
}

func TestConfigRejectsNonClaimEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"https://example.test/api/v1/sandboxes/extra",
		"https://example.test/api/v1//sandboxes",
		"https://example.test/api/v1/other/../sandboxes",
		"https://example.test/api/v1/%73andboxes",
		"https://example.test/api/v1/sandboxes?",
	} {
		cfg := config{
			endpoint: endpoint, token: "token", body: []byte(`{}`),
			batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: time.Second,
			cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
			p50Target: 500 * time.Millisecond,
		}
		if err := cfg.validate(); err == nil {
			t.Fatalf("noncanonical claim endpoint %q was accepted", endpoint)
		}
	}
}

func TestConfigRejectsCleartextClaimEndpoint(t *testing.T) {
	cfg := config{
		endpoint: "http://example.test/api/v1/sandboxes", token: "token", body: []byte(`{}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, hardLimit: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		p50Target: 500 * time.Millisecond,
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("cleartext claim endpoint was accepted")
	}
}
