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
// and discarding stale entries when read (TTL).
type Cache[K comparable, V any] struct {
	mu sync.RWMutex

	// Core data structures
	items map[K]*list.Element // map key to list element
	lru   *list.List          // doubly linked list for LRU

	// Configuration
	maxSize int           // max entries (0 = unlimited, not recommended)
	ttl     time.Duration // time-to-live for entries
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
}

// New creates a new cache with the given configuration
func New[K comparable, V any](cfg Config) *Cache[K, V] {
	return &Cache[K, V]{
		items:   make(map[K]*list.Element),
		lru:     list.New(),
		maxSize: cfg.MaxSize,
		ttl:     cfg.TTL,
	}
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
		var zero V
		return zero, false
	}

	lruEnt := elem.Value.(*lruEntry[K, V])

	// Check expiration
	if c.ttl > 0 && time.Now().After(lruEnt.entry.expiration) {
		c.deleteElement(elem)
		var zero V
		return zero, false
	}

	// Move to front (most recently used)
	c.lru.MoveToFront(elem)

	return lruEnt.entry.value, true
}

// Clear removes all entries from the cache
func (c *Cache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[K]*list.Element)
	c.lru.Init()
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
