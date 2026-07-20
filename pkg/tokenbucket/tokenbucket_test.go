package tokenbucket

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestMemoryBucketSupportsImmediateAndReservedTokens(t *testing.T) {
	bucket := NewMemoryBucket(MemoryConfig{})
	defer bucket.Close()
	limit := Limit{Tokens: 10, Interval: time.Second, Burst: 10}

	first, err := bucket.TryTakeN(context.Background(), "team-1", limit, 10)
	if err != nil || !first.Allowed || first.Remaining != 0 {
		t.Fatalf("first TryTakeN() = %+v, %v", first, err)
	}
	second, err := bucket.TryTakeN(context.Background(), "team-1", limit, 1)
	if err != nil || second.Allowed || second.RetryAfter < 90*time.Millisecond {
		t.Fatalf("second TryTakeN() = %+v, %v, want retry", second, err)
	}
	reservation, err := bucket.ReserveN(context.Background(), "team-1", limit, 10)
	if err != nil || reservation.Delay < 900*time.Millisecond {
		t.Fatalf("ReserveN() = %+v, %v, want future reservation", reservation, err)
	}
}

func TestMemoryBucketResetsStateWhenPolicyChanges(t *testing.T) {
	bucket := NewMemoryBucket(MemoryConfig{})
	defer bucket.Close()
	if decision, err := bucket.TryTakeN(context.Background(), "team-1", Limit{
		Tokens: 1, Interval: time.Second, Burst: 1,
	}, 1); err != nil || !decision.Allowed {
		t.Fatalf("initial TryTakeN() = %+v, %v", decision, err)
	}
	decision, err := bucket.TryTakeN(context.Background(), "team-1", Limit{
		Tokens: 100, Interval: time.Second, Burst: 10,
	}, 10)
	if err != nil || !decision.Allowed {
		t.Fatalf("updated TryTakeN() = %+v, %v, want reset burst", decision, err)
	}
}

func TestMemoryBucketCleanupPreservesUnrefilledState(t *testing.T) {
	bucket := NewMemoryBucket(MemoryConfig{CleanupInterval: 5 * time.Millisecond})
	defer bucket.Close()
	limit := Limit{Tokens: 1, Interval: time.Second, Burst: 1}
	if decision, err := bucket.TryTakeN(context.Background(), "team-1", limit, 1); err != nil || !decision.Allowed {
		t.Fatalf("initial TryTakeN() = %+v, %v", decision, err)
	}

	time.Sleep(30 * time.Millisecond)

	decision, err := bucket.TryTakeN(context.Background(), "team-1", limit, 1)
	if err != nil || decision.Allowed {
		t.Fatalf("TryTakeN() after cleanup = %+v, %v, want depleted state preserved", decision, err)
	}
}

func TestRedisBucketSharesStateAndUsesIntegerTokens(t *testing.T) {
	server := miniredis.RunT(t)
	first, err := NewRedisBucket(context.Background(), RedisConfig{
		URL:       "redis://" + server.Addr() + "/0",
		KeyPrefix: "test",
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket(first): %v", err)
	}
	defer first.Close()
	second, err := NewRedisBucket(context.Background(), RedisConfig{
		URL:       "redis://" + server.Addr() + "/0",
		KeyPrefix: "test",
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisBucket(second): %v", err)
	}
	defer second.Close()

	limit := Limit{Tokens: 100, Interval: time.Second, Burst: 100}
	decision, err := first.TryTakeN(context.Background(), "region:r1:team:t1", limit, 100)
	if err != nil || !decision.Allowed {
		t.Fatalf("first TryTakeN() = %+v, %v", decision, err)
	}
	decision, err = second.TryTakeN(context.Background(), "region:r1:team:t1", limit, 1)
	if err != nil || decision.Allowed || decision.RetryAfter <= 0 {
		t.Fatalf("shared TryTakeN() = %+v, %v, want limited", decision, err)
	}
}

func TestZeroTokenPolicyRejectsWithoutSchedulingForever(t *testing.T) {
	bucket := NewMemoryBucket(MemoryConfig{})
	defer bucket.Close()
	limit := Limit{Tokens: 0, Interval: time.Second, Burst: 0}
	decision, err := bucket.TryTakeN(context.Background(), "team-1", limit, 1)
	if err != nil || decision.Allowed {
		t.Fatalf("TryTakeN() = %+v, %v, want rejected", decision, err)
	}
	if _, err := bucket.ReserveN(context.Background(), "team-1", limit, 1); err == nil {
		t.Fatal("ReserveN() error = nil, want zero-limit error")
	}
}
