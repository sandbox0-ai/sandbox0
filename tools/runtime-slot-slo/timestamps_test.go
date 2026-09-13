package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiagnosticTimestampsIncludeFailedAttemptsWithoutRetry(t *testing.T) {
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := config{endpoint: server.URL, requestTimeout: time.Second, client: server.Client()}
	s := claim(context.Background(), cfg, 0, 0, 0, new(sync.Map))
	if s.ClaimSucceeded || s.Error == "" || s.ClaimStartedAt.IsZero() ||
		!s.ClaimCompletedAt.After(s.ClaimStartedAt) || s.WallDuration <= 0 || len(s.Steps) != 0 {
		t.Fatalf("failed claim timestamp boundary: %+v", s)
	}
	step := executeCommand(context.Background(), cfg, "test-sandbox", shellWorkload().Steps[0])
	if step.Passed || step.Error == "" || step.StartedAt.IsZero() ||
		!step.CompletedAt.After(step.StartedAt) || step.Duration <= 0 || posts.Load() != 2 {
		t.Fatalf("failed command timestamp boundary or unexpected retries: %+v posts=%d", step, posts.Load())
	}
}

func TestDiagnosticClaimTimestampsRemainZeroBeforeDispatch(t *testing.T) {
	cfg := config{endpoint: "://invalid", requestTimeout: time.Second}
	s := claim(context.Background(), cfg, 0, 0, 0, new(sync.Map))
	if s.Error == "" || !s.ClaimStartedAt.IsZero() || !s.ClaimCompletedAt.IsZero() || s.WallDuration != 0 {
		t.Fatalf("request construction must not invent a dispatch timestamp: %+v", s)
	}
}
