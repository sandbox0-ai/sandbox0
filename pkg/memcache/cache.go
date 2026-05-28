package memcache

import (
	"container/list"
	"sync"
	"time"
)

// Entry represents a cached item with expiration
type Entry[V any] struct {
	value      V
	expiration time.Time
}

// Cache is a thread-safe in-memory cache with TTL and LRU eviction
// It prevents memory leaks by limiting the maximum number of entries (LRU)
// and automatically expiring stale entries (TTL)
type Cache[K comparable, V any] struct {
	mu sync.RWMutex

	// Core data structures
	items map[K]*list.Element // map key to list element
	lru   *list.List          // doubly linked list for LRU

	// Configuration
	maxSize int           // max entries (0 = unlimited, not recommended)
	ttl     time.Duration // time-to-live for entries

	// Stats
	hits   uint64
	misses uint64

	// Cleanup
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
	cleanupOnce     sync.Once
}

// lruEntry wraps the key and cached entry for LRU list
type lruEntry[K comparable, V any] struct {
	key   K
	entry *Entry[V]
}

// Config configures the cache behavior
type Config struct {
	// MaxSize limits the maximum number of entries
	// When exceeded, least recently used entries are evicted
	// Set to 0 for unlimited (not recommended for production)
	MaxSize int

	// TTL is the time-to-live for cache entries
	// Entries older than TTL are considered expired
	TTL time.Duration

	// CleanupInterval determines how often expired entries are removed
	// If 0, defaults to TTL/2 (or 1 minute if TTL is 0)
	CleanupInterval time.Duration
}

// New creates a new cache with the given configuration
func New[K comparable, V any](cfg Config) *Cache[K, V] {
	if cfg.CleanupInterval == 0 {
		if cfg.TTL > 0 {
			cfg.CleanupInterval = cfg.TTL / 2
		} else {
			cfg.CleanupInterval = time.Minute
		}
	}

	c := &Cache[K, V]{
		items:           make(map[K]*list.Element),
		lru:             list.New(),
		maxSize:         cfg.MaxSize,
		ttl:             cfg.TTL,
		cleanupInterval: cfg.CleanupInterval,
		stopCleanup:     make(chan struct{}),
	}

	// Start background cleanup goroutine
	go c.cleanupLoop()

	return c
}

// Set stores a value in the cache
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	entry := &Entry[V]{
		value:      value,
		expiration: now.Add(c.ttl),
	}

	// If key exists, update and move to front
	if elem, exists := c.items[key]; exists {
		elem.Value.(*lruEntry[K, V]).entry = entry
		c.lru.MoveToFront(elem)
		return
	}

	// Add new entry
	elem := c.lru.PushFront(&lruEntry[K, V]{
		key:   key,
		entry: entry,
	})
	c.items[key] = elem

	// Evict LRU if over capacity
	if c.maxSize > 0 && c.lru.Len() > c.maxSize {
		c.evictLRU()
	}
}

// Get retrieves a value from the cache
// Returns the value and true if found and not expired, zero value and false otherwise
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.items[key]
	if !exists {
		c.misses++
		var zero V
		return zero, false
	}

	lruEnt := elem.Value.(*lruEntry[K, V])

	// Check expiration
	if c.ttl > 0 && time.Now().After(lruEnt.entry.expiration) {
		c.misses++
		c.deleteElement(elem)
		var zero V
		return zero, false
	}

	// Move to front (most recently used)
	c.lru.MoveToFront(elem)
	c.hits++

	return lruEnt.entry.value, true
}

// Delete removes a key from the cache
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[key]; exists {
		c.deleteElement(elem)
	}
}

// Clear removes all entries from the cache
func (c *Cache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*list.Element)
	c.lru.Init()
}

// Len returns the current number of entries in the cache
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Stats returns cache statistics
func (c *Cache[K, V]) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}

	return Stats{
		Hits:    c.hits,
		Misses:  c.misses,
		Size:    len(c.items),
		HitRate: hitRate,
	}
}

// Stats contains cache statistics
type Stats struct {
	Hits    uint64
	Misses  uint64
	Size    int
	HitRate float64
}

// Close stops the background cleanup goroutine
// It should be called when the cache is no longer needed
func (c *Cache[K, V]) Close() {
	c.cleanupOnce.Do(func() {
		close(c.stopCleanup)
	})
}

// evictLRU removes the least recently used entry
// Must be called with lock held
func (c *Cache[K, V]) evictLRU() {
	elem := c.lru.Back()
	if elem != nil {
		c.deleteElement(elem)
	}
}

// deleteElement removes an element from the cache
// Must be called with lock held
func (c *Cache[K, V]) deleteElement(elem *list.Element) {
	lruEnt := elem.Value.(*lruEntry[K, V])
	delete(c.items, lruEnt.key)
	c.lru.Remove(elem)
}

// cleanupLoop periodically removes expired entries
func (c *Cache[K, V]) cleanupLoop() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCleanup:
			return
		}
	}
}

// cleanup removes expired entries
func (c *Cache[K, V]) cleanup() {
	if c.ttl == 0 {
		return // no TTL, nothing to cleanup
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	toDelete := make([]*list.Element, 0)

	// Collect expired entries
	for elem := c.lru.Front(); elem != nil; elem = elem.Next() {
		lruEnt := elem.Value.(*lruEntry[K, V])
		if now.After(lruEnt.entry.expiration) {
			toDelete = append(toDelete, elem)
		}
	}

	// Delete expired entries
	for _, elem := range toDelete {
		c.deleteElement(elem)
	}
}
