package ratelimit

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

type RedisConfig struct {
	URL       string
	KeyPrefix string
	Timeout   time.Duration
	FailOpen  bool
}

type RedisLimiter struct {
	*bucketLimiter
}

func NewRedisLimiter(ctx context.Context, cfg RedisConfig) (*RedisLimiter, error) {
	bucket, err := tokenbucket.NewRedisBucket(ctx, tokenbucket.RedisConfig{
		URL:       cfg.URL,
		KeyPrefix: cfg.KeyPrefix,
		Timeout:   cfg.Timeout,
		FailOpen:  cfg.FailOpen,
	})
	if err != nil {
		return nil, err
	}
	return &RedisLimiter{bucketLimiter: &bucketLimiter{bucket: bucket}}, nil
}

func (l *RedisLimiter) Allow(ctx context.Context, key string, limit Limit) (Decision, error) {
	if l == nil || l.bucketLimiter == nil {
		return Decision{}, ErrClosed
	}
	return l.bucketLimiter.Allow(ctx, key, limit)
}
