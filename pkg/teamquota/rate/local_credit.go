package rate

import (
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	defaultLocalCreditTTL        = 75 * time.Millisecond
	minLocalCreditTTL            = 50 * time.Millisecond
	defaultRequestCreditBatch    = int64(16)
	defaultByteCreditBatch       = int64(64 * 1024)
	defaultLocalCreditMaxEntries = 10_000
)

type localCreditKey struct {
	teamID           string
	key              teamquota.Key
	policyRevision   int64
	enforcementEpoch int64
	redisGeneration  int64
}

type localCreditEntry struct {
	mu        sync.Mutex
	balance   int64
	expiresAt time.Time

	// refs is protected by localCreditCache.mu. An entry with refs > 0
	// cannot be evicted, even before its caller acquires entry.mu.
	refs int
}

type localCreditHandle struct {
	cache  *localCreditCache
	key    localCreditKey
	entry  *localCreditEntry
	cached bool
}

type localCreditCache struct {
	mu      sync.Mutex
	entries map[localCreditKey]*localCreditEntry
	max     int
}

func newLocalCreditCache(maxEntries int) *localCreditCache {
	if maxEntries <= 0 {
		maxEntries = defaultLocalCreditMaxEntries
	}
	return &localCreditCache{
		entries: make(map[localCreditKey]*localCreditEntry),
		max:     maxEntries,
	}
}

func (c *localCreditCache) acquire(key localCreditKey, now time.Time) localCreditHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil {
		entry.refs++
		return localCreditHandle{cache: c, key: key, entry: entry, cached: true}
	}

	for candidate, entry := range c.entries {
		if entry.refs == 0 && !now.Before(entry.expiresAt) {
			delete(c.entries, candidate)
		}
	}
	if len(c.entries) >= c.max {
		var (
			evictionKey localCreditKey
			eviction    *localCreditEntry
		)
		for candidate, entry := range c.entries {
			if entry.refs != 0 {
				continue
			}
			if eviction == nil || entry.expiresAt.Before(eviction.expiresAt) {
				evictionKey = candidate
				eviction = entry
			}
		}
		if eviction != nil {
			// Local credits are reservations already removed from Redis.
			// Eviction intentionally burns any unused remainder.
			delete(c.entries, evictionKey)
		}
	}

	entry := &localCreditEntry{refs: 1}
	if len(c.entries) >= c.max {
		// All bounded entries are in flight. Use an uncached exact grant for
		// this call instead of growing local outstanding credit.
		return localCreditHandle{entry: entry}
	}
	c.entries[key] = entry
	return localCreditHandle{cache: c, key: key, entry: entry, cached: true}
}

func (h localCreditHandle) release() {
	if !h.cached || h.cache == nil || h.entry == nil {
		return
	}
	h.cache.mu.Lock()
	if current := h.cache.entries[h.key]; current == h.entry && h.entry.refs > 0 {
		h.entry.refs--
	}
	h.cache.mu.Unlock()
}

func (c *localCreditCache) invalidate(teamID string, key *teamquota.Key) {
	if c == nil {
		return
	}
	c.mu.Lock()
	entries := make([]*localCreditEntry, 0)
	for candidate, entry := range c.entries {
		if candidate.teamID != teamID || (key != nil && candidate.key != *key) {
			continue
		}
		if entry.refs == 0 {
			delete(c.entries, candidate)
		}
		entries = append(entries, entry)
	}
	c.mu.Unlock()
	for _, entry := range entries {
		entry.mu.Lock()
		entry.balance = 0
		entry.expiresAt = time.Time{}
		entry.mu.Unlock()
	}
}

func localCreditIdentity(
	teamID string,
	key teamquota.Key,
	revision int64,
	version guard.Version,
) localCreditKey {
	return localCreditKey{
		teamID:           teamID,
		key:              key,
		policyRevision:   revision,
		enforcementEpoch: version.EnforcementEpoch,
		redisGeneration:  version.RedisGeneration,
	}
}

func localCreditBatch(key teamquota.Key) int64 {
	if teamquota.UnitForKey(key) == "bytes" {
		return defaultByteCreditBatch
	}
	return defaultRequestCreditBatch
}
