package resolver

import (
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

type cacheEntry struct {
	response  *egressauth.ResolveResponse
	expiresAt time.Time
}

type resultCache struct {
	mu         sync.RWMutex
	entries    map[string]cacheEntry
	maxEntries int
}

func newResultCache(maxEntries int) *resultCache {
	if maxEntries <= 0 {
		maxEntries = 2048
	}
	return &resultCache{
		entries:    make(map[string]cacheEntry),
		maxEntries: maxEntries,
	}
}

func (c *resultCache) Get(key string, now time.Time) (*egressauth.ResolveResponse, bool) {
	if c == nil || key == "" {
		return nil, false
	}

	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && !entry.expiresAt.After(now) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}
	return cloneResolveResponse(entry.response), true
}

func (c *resultCache) Set(key string, response *egressauth.ResolveResponse, ttl time.Duration, now time.Time) {
	if c == nil || key == "" || response == nil || ttl <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeExpiredLocked(now)
	if len(c.entries) >= c.maxEntries {
		for existingKey := range c.entries {
			delete(c.entries, existingKey)
			break
		}
	}

	c.entries[key] = cacheEntry{
		response:  cloneResolveResponse(response),
		expiresAt: now.Add(ttl),
	}
}

func (c *resultCache) purgeExpiredLocked(now time.Time) {
	for key, entry := range c.entries {
		if !entry.expiresAt.IsZero() && !entry.expiresAt.After(now) {
			delete(c.entries, key)
		}
	}
}
