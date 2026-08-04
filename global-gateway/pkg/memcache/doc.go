// Package memcache provides a thread-safe, bounded in-memory cache with TTL
// and LRU eviction. Expired entries are discarded when read; MaxSize bounds
// retained entries between reads.
package memcache
