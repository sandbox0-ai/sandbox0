// Package tokenbucket provides shared local and Redis-backed token buckets for
// request admission and byte-rate backpressure.
package tokenbucket

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	BackendMemory = "memory"
	BackendRedis  = "redis"

	DefaultBackend         = BackendMemory
	DefaultRedisKeyPrefix  = "sandbox0:tokenbucket"
	DefaultRedisTimeout    = 100 * time.Millisecond
	DefaultCleanupInterval = 10 * time.Minute
)

var (
	ErrLimited = errors.New("token bucket limit is zero")
	ErrClosed  = errors.New("token bucket is closed")
)

// Limit defines how many integer tokens refill during an interval and the
// maximum immediately available burst.
type Limit struct {
	Tokens   int64
	Interval time.Duration
	Burst    int64
}

func (l Limit) Validate() error {
	if l.Tokens < 0 {
		return fmt.Errorf("tokens must be non-negative")
	}
	if l.Interval < time.Millisecond {
		return fmt.Errorf("interval must be at least one millisecond")
	}
	if l.Burst < 0 {
		return fmt.Errorf("burst must be non-negative")
	}
	if l.Tokens > 0 && l.Burst == 0 {
		return fmt.Errorf("burst must be positive when tokens are positive")
	}
	return nil
}

type Decision struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

type Reservation struct {
	Delay time.Duration
}

// Bucket supports immediate admission and future reservations against the same
// token state.
type Bucket interface {
	TryTakeN(ctx context.Context, key string, limit Limit, tokens int64) (Decision, error)
	ReserveN(ctx context.Context, key string, limit Limit, tokens int64) (Reservation, error)
	Close() error
}

type Config struct {
	Backend         string
	RedisURL        string
	RedisKeyPrefix  string
	RedisTimeout    time.Duration
	CleanupInterval time.Duration
	FailOpen        bool
}

func New(ctx context.Context, cfg Config) (Bucket, error) {
	backend := strings.TrimSpace(strings.ToLower(cfg.Backend))
	if backend == "" {
		backend = DefaultBackend
	}
	switch backend {
	case BackendMemory:
		return NewMemoryBucket(MemoryConfig{CleanupInterval: cfg.CleanupInterval}), nil
	case BackendRedis:
		return NewRedisBucket(ctx, RedisConfig{
			URL:       cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout,
			FailOpen:  cfg.FailOpen,
		})
	default:
		return nil, fmt.Errorf("unsupported token bucket backend %q", cfg.Backend)
	}
}

func RetryAfterSeconds(delay time.Duration) int {
	if delay <= 0 {
		return 1
	}
	seconds := int((delay + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func retryAfter(limit Limit, deficit, remainder int64) time.Duration {
	if limit.Tokens <= 0 {
		return 0
	}
	numerator := saturatingMultiply(deficit, limit.Interval.Milliseconds())
	if remainder > 0 && numerator > remainder {
		numerator -= remainder
	}
	return time.Duration(ceilDiv(numerator, limit.Tokens)) * time.Millisecond
}

func ceilDiv(value, divisor int64) int64 {
	if value <= 0 || divisor <= 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func saturatingMultiply(left, right int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > maxInt64/right {
		return maxInt64
	}
	return left * right
}
