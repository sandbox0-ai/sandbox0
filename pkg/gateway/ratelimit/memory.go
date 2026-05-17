package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type MemoryConfig struct {
	CleanupInterval time.Duration
}

type MemoryLimiter struct {
	mu              sync.Mutex
	entries         map[string]*memoryEntry
	cleanupInterval time.Duration
	stop            chan struct{}
	closed          bool
}

type memoryEntry struct {
	limiter  *rate.Limiter
	limit    Limit
	lastSeen time.Time
}

func NewMemoryLimiter(cfg MemoryConfig) *MemoryLimiter {
	interval := cfg.CleanupInterval
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}
	limiter := &MemoryLimiter{
		entries:         make(map[string]*memoryEntry),
		cleanupInterval: interval,
		stop:            make(chan struct{}),
	}
	go limiter.cleanupLoop()
	return limiter
}

func (l *MemoryLimiter) Allow(_ context.Context, key string, limit Limit) (Decision, error) {
	limit, ok := normalizeLimit(limit)
	if !ok {
		return Decision{Allowed: true}, nil
	}

	now := time.Now()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return Decision{}, ErrClosed
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &memoryEntry{
			limiter: rate.NewLimiter(rate.Limit(limit.RPS), limit.Burst),
			limit:   limit,
		}
		l.entries[key] = entry
	} else if entry.limit != limit {
		entry.limiter.SetLimitAt(now, rate.Limit(limit.RPS))
		entry.limiter.SetBurstAt(now, limit.Burst)
		entry.limit = limit
	}
	entry.lastSeen = now
	allowed := entry.limiter.AllowN(now, 1)
	remaining := entry.limiter.TokensAt(now)
	l.mu.Unlock()

	decision := Decision{
		Allowed:   allowed,
		Limit:     limit.RPS,
		Remaining: int(remaining),
	}
	if !allowed {
		decision.RetryAfter = retryAfterFromLimit(limit)
	}
	return decision, nil
}

func (l *MemoryLimiter) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.stop)
	l.entries = nil
	l.mu.Unlock()
	return nil
}

func (l *MemoryLimiter) cleanupLoop() {
	ticker := time.NewTicker(l.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.cleanup(time.Now())
		case <-l.stop:
			return
		}
	}
}

func (l *MemoryLimiter) cleanup(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	for key, entry := range l.entries {
		if now.Sub(entry.lastSeen) >= l.cleanupInterval {
			delete(l.entries, key)
		}
	}
}
