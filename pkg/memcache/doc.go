// Package memcache provides a thread-safe in-memory cache with TTL and LRU eviction.
//
// # Overview
//
// This package implements a generic, thread-safe cache that prevents memory leaks
// through two mechanisms:
//  1. LRU (Least Recently Used) eviction: When MaxSize is reached, the least
//     recently used entry is automatically evicted
//  2. TTL (Time-To-Live) expiration: Entries older than TTL are automatically
//     removed by a background cleanup goroutine
//
// # Usage
//
// Create a cache with configuration:
//
//	cache := memcache.New[string, *MyType](memcache.Config{
//	    MaxSize:         1000,
//	    TTL:             5 * time.Minute,
//	    CleanupInterval: time.Minute,
//	})
//	defer cache.Close() // Important: stops background cleanup goroutine
//
// Set and get values:
//
//	cache.Set("key", value)
//	if val, ok := cache.Get("key"); ok {
//	    // Use val
//	}
//
// # Performance
//
// All operations are O(1) average case:
//   - Set: ~237 ns/op
//   - Get: ~118 ns/op
//   - Get (parallel): ~24 ns/op
//
// # Thread Safety
//
// All operations are thread-safe and can be called concurrently from multiple
// goroutines without external synchronization.
//
// # Use Cases
//
// This cache is designed for:
//   - Caching API responses to reduce backend load
//   - Storing frequently accessed data with automatic expiration
//   - Preventing excessive database queries
//   - Improving response times for hot data
//
// # Examples
//
// See the README.md file for detailed examples and usage patterns.
package memcache
