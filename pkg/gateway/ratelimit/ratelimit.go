package ratelimit

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
	DefaultRedisKeyPrefix  = "sandbox0:ratelimit"
	DefaultRedisTimeout    = 100 * time.Millisecond
	DefaultCleanupInterval = 10 * time.Minute
)

var (
	ErrLimited = errors.New("rate limit exceeded")
	ErrClosed  = errors.New("rate limiter is closed")
)

// Limit defines a token bucket rate limit.
type Limit struct {
	RPS   int
	Burst int
}

// Decision describes the result of a rate limit check.
type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

// Limiter checks whether one request for a key should be allowed.
type Limiter interface {
	Allow(ctx context.Context, key string, limit Limit) (Decision, error)
	Close() error
}

// Config selects the rate limit backend.
type Config struct {
	Backend         string
	RedisURL        string
	RedisKeyPrefix  string
	RedisTimeout    time.Duration
	CleanupInterval time.Duration
	FailOpen        bool
}

func New(ctx context.Context, cfg Config) (Limiter, error) {
	backend := strings.TrimSpace(strings.ToLower(cfg.Backend))
	if backend == "" {
		backend = DefaultBackend
	}
	switch backend {
	case BackendMemory:
		return NewMemoryLimiter(MemoryConfig{CleanupInterval: cfg.CleanupInterval}), nil
	case BackendRedis:
		return NewRedisLimiter(ctx, RedisConfig{
			URL:       cfg.RedisURL,
			KeyPrefix: cfg.RedisKeyPrefix,
			Timeout:   cfg.RedisTimeout,
			FailOpen:  cfg.FailOpen,
		})
	default:
		return nil, fmt.Errorf("unsupported rate limit backend %q", cfg.Backend)
	}
}

func normalizeLimit(limit Limit) (Limit, bool) {
	if limit.RPS <= 0 || limit.Burst <= 0 {
		return Limit{}, false
	}
	return limit, true
}

func retryAfterFromLimit(limit Limit) time.Duration {
	if limit.RPS <= 0 {
		return time.Second
	}
	d := time.Second / time.Duration(limit.RPS)
	if d <= 0 {
		return time.Millisecond
	}
	return d
}

func RetryAfterSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	seconds := int(d.Round(time.Second) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}
