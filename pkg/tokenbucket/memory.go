package tokenbucket

import (
	"context"
	"sync"
	"time"
)

type MemoryConfig struct {
	CleanupInterval time.Duration
}

type MemoryBucket struct {
	mu              sync.Mutex
	entries         map[string]*memoryEntry
	cleanupInterval time.Duration
	stop            chan struct{}
	closed          bool
}

type memoryEntry struct {
	limit       Limit
	tokens      int64
	remainder   int64
	updatedAtMS int64
	lastSeen    time.Time
}

func NewMemoryBucket(cfg MemoryConfig) *MemoryBucket {
	interval := cfg.CleanupInterval
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}
	bucket := &MemoryBucket{
		entries:         make(map[string]*memoryEntry),
		cleanupInterval: interval,
		stop:            make(chan struct{}),
	}
	go bucket.cleanupLoop()
	return bucket
}

func (b *MemoryBucket) TryTakeN(_ context.Context, key string, limit Limit, requested int64) (Decision, error) {
	if requested <= 0 {
		return Decision{Allowed: true, Remaining: limit.Burst}, nil
	}
	if err := limit.Validate(); err != nil {
		return Decision{}, err
	}
	if limit.Tokens == 0 || limit.Burst == 0 {
		return Decision{RetryAfter: time.Second}, nil
	}

	now := time.Now()
	nowMS := now.UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Decision{}, ErrClosed
	}
	entry := b.entry(key, limit, now, nowMS)
	refillMemoryEntry(entry, nowMS)
	if requested <= entry.tokens {
		entry.tokens -= requested
		return Decision{Allowed: true, Remaining: entry.tokens}, nil
	}
	wait := retryAfter(limit, requested-entry.tokens, entry.remainder)
	if entry.updatedAtMS > nowMS {
		wait += time.Duration(entry.updatedAtMS-nowMS) * time.Millisecond
	}
	return Decision{Remaining: entry.tokens, RetryAfter: wait}, nil
}

func (b *MemoryBucket) ReserveN(_ context.Context, key string, limit Limit, requested int64) (Reservation, error) {
	if requested <= 0 {
		return Reservation{}, nil
	}
	if err := limit.Validate(); err != nil {
		return Reservation{}, err
	}
	if limit.Tokens == 0 || limit.Burst == 0 {
		return Reservation{}, ErrLimited
	}

	now := time.Now()
	nowMS := now.UnixMilli()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Reservation{}, ErrClosed
	}
	entry := b.entry(key, limit, now, nowMS)
	refillMemoryEntry(entry, nowMS)
	if requested <= entry.tokens {
		entry.tokens -= requested
		return Reservation{}, nil
	}

	effectiveNowMS := nowMS
	deficit := requested - entry.tokens
	remainder := entry.remainder
	if entry.updatedAtMS > nowMS {
		effectiveNowMS = entry.updatedAtMS
		deficit = requested
		remainder = 0
	}
	wait := retryAfter(limit, deficit, remainder)
	entry.tokens = 0
	entry.remainder = 0
	entry.updatedAtMS = effectiveNowMS + wait.Milliseconds()
	return Reservation{Delay: time.Duration(entry.updatedAtMS-nowMS) * time.Millisecond}, nil
}

func (b *MemoryBucket) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.stop)
	clear(b.entries)
	b.mu.Unlock()
	return nil
}

func (b *MemoryBucket) entry(key string, limit Limit, now time.Time, nowMS int64) *memoryEntry {
	entry := b.entries[key]
	if entry == nil || entry.limit != limit {
		entry = &memoryEntry{
			limit:       limit,
			tokens:      limit.Burst,
			updatedAtMS: nowMS,
		}
		b.entries[key] = entry
	}
	entry.lastSeen = now
	return entry
}

func refillMemoryEntry(entry *memoryEntry, nowMS int64) {
	if entry == nil || nowMS <= entry.updatedAtMS || entry.tokens >= entry.limit.Burst {
		if entry != nil && entry.tokens >= entry.limit.Burst && nowMS > entry.updatedAtMS {
			entry.updatedAtMS = nowMS
			entry.remainder = 0
		}
		return
	}
	elapsedMS := nowMS - entry.updatedAtMS
	numerator := saturatingMultiply(elapsedMS, entry.limit.Tokens)
	const maxInt64 = int64(^uint64(0) >> 1)
	if numerator > maxInt64-entry.remainder {
		entry.tokens = entry.limit.Burst
		entry.remainder = 0
		entry.updatedAtMS = nowMS
		return
	}
	numerator += entry.remainder
	intervalMS := entry.limit.Interval.Milliseconds()
	added := numerator / intervalMS
	entry.remainder = numerator % intervalMS
	entry.updatedAtMS = nowMS
	entry.tokens += added
	if entry.tokens >= entry.limit.Burst {
		entry.tokens = entry.limit.Burst
		entry.remainder = 0
	}
}

func (b *MemoryBucket) cleanupLoop() {
	ticker := time.NewTicker(b.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			b.mu.Lock()
			if !b.closed {
				for key, entry := range b.entries {
					refillMemoryEntry(entry, now.UnixMilli())
					if now.Sub(entry.lastSeen) >= b.cleanupInterval &&
						entry.updatedAtMS <= now.UnixMilli() &&
						entry.tokens >= entry.limit.Burst {
						delete(b.entries, key)
					}
				}
			}
			b.mu.Unlock()
		case <-b.stop:
			return
		}
	}
}
