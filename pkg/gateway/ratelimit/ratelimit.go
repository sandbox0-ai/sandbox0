package ratelimit

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

const (
	BackendMemory = tokenbucket.BackendMemory
	BackendRedis  = tokenbucket.BackendRedis

	DefaultBackend         = tokenbucket.DefaultBackend
	DefaultRedisKeyPrefix  = "sandbox0:ratelimit"
	DefaultRedisTimeout    = tokenbucket.DefaultRedisTimeout
	DefaultCleanupInterval = tokenbucket.DefaultCleanupInterval
)

var (
	ErrLimited = tokenbucket.ErrLimited
	ErrClosed  = tokenbucket.ErrClosed
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
	bucket, err := tokenbucket.New(ctx, tokenbucket.Config{
		Backend:         cfg.Backend,
		RedisURL:        cfg.RedisURL,
		RedisKeyPrefix:  cfg.RedisKeyPrefix,
		RedisTimeout:    cfg.RedisTimeout,
		CleanupInterval: cfg.CleanupInterval,
		FailOpen:        cfg.FailOpen,
	})
	if err != nil {
		return nil, err
	}
	return &bucketLimiter{bucket: bucket}, nil
}

type bucketLimiter struct {
	bucket tokenbucket.Bucket
}

func (l *bucketLimiter) Allow(ctx context.Context, key string, limit Limit) (Decision, error) {
	if limit.RPS <= 0 || limit.Burst <= 0 {
		return Decision{Allowed: true}, nil
	}
	decision, err := l.bucket.TryTakeN(ctx, key, tokenbucket.Limit{
		Tokens:   int64(limit.RPS),
		Interval: time.Second,
		Burst:    int64(limit.Burst),
	}, 1)
	return Decision{
		Allowed:    decision.Allowed,
		Limit:      limit.RPS,
		Remaining:  int(decision.Remaining),
		RetryAfter: decision.RetryAfter,
	}, err
}

func (l *bucketLimiter) Close() error {
	if l == nil || l.bucket == nil {
		return nil
	}
	return l.bucket.Close()
}

func RetryAfterSeconds(d time.Duration) int {
	return tokenbucket.RetryAfterSeconds(d)
}
