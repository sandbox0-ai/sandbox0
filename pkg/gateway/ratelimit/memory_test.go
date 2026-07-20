package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestMemoryLimiterAllowsBurstThenLimits(t *testing.T) {
	limiter := NewMemoryLimiter(MemoryConfig{CleanupInterval: time.Minute})
	defer limiter.Close()

	limit := Limit{RPS: 1, Burst: 1}
	decision, err := limiter.Allow(context.Background(), "team-1", limit)
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	if !decision.Allowed {
		t.Fatal("first request was limited")
	}

	decision, err = limiter.Allow(context.Background(), "team-1", limit)
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	if decision.Allowed {
		t.Fatal("second immediate request was allowed")
	}
	if decision.RetryAfter <= 0 {
		t.Fatalf("RetryAfter = %s, want positive duration", decision.RetryAfter)
	}
}

func TestMemoryLimiterUpdatesLimitForExistingKey(t *testing.T) {
	limiter := NewMemoryLimiter(MemoryConfig{CleanupInterval: time.Minute})
	defer limiter.Close()

	key := "team-1"
	if decision, err := limiter.Allow(context.Background(), key, Limit{RPS: 1, Burst: 1}); err != nil || !decision.Allowed {
		t.Fatalf("initial Allow() = %+v, %v", decision, err)
	}
	if decision, err := limiter.Allow(context.Background(), key, Limit{RPS: 1, Burst: 1}); err != nil || decision.Allowed {
		t.Fatalf("second Allow() = %+v, %v, want limited", decision, err)
	}

	_, err := limiter.Allow(context.Background(), key, Limit{RPS: 100, Burst: 10})
	if err != nil {
		t.Fatalf("updated Allow() error = %v", err)
	}
	for i := 0; i < 9; i++ {
		if decision, err := limiter.Allow(context.Background(), key, Limit{RPS: 100, Burst: 10}); err != nil || !decision.Allowed {
			t.Fatalf("updated Allow() %d = %+v, %v, want allowed", i, decision, err)
		}
	}
}
