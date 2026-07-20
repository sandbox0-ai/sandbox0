package ratelimit

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

type MemoryConfig struct {
	CleanupInterval time.Duration
}

type MemoryLimiter struct {
	*bucketLimiter
}

func NewMemoryLimiter(cfg MemoryConfig) *MemoryLimiter {
	return &MemoryLimiter{bucketLimiter: &bucketLimiter{
		bucket: tokenbucket.NewMemoryBucket(tokenbucket.MemoryConfig{
			CleanupInterval: cfg.CleanupInterval,
		}),
	}}
}

func (l *MemoryLimiter) Allow(ctx context.Context, key string, limit Limit) (Decision, error) {
	if l == nil || l.bucketLimiter == nil {
		return Decision{}, ErrClosed
	}
	return l.bucketLimiter.Allow(ctx, key, limit)
}
