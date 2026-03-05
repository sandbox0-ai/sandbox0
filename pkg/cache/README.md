# Cache Package

Thread-safe in-memory cache with TTL (Time-To-Live) and LRU (Least Recently Used) eviction policy.

## Features

- **Thread-Safe**: Safe for concurrent access from multiple goroutines
- **TTL Support**: Automatic expiration of stale entries
- **LRU Eviction**: Prevents memory leaks by limiting maximum entries
- **Background Cleanup**: Periodic removal of expired entries
- **Statistics**: Built-in hit/miss tracking and hit rate calculation
- **Generic**: Type-safe implementation using Go generics

## Usage

### Basic Example

```go
import "github.com/sandbox0-ai/sandbox0/pkg/cache"

// Create a cache that holds max 1000 entries, with 5-minute TTL
c := cache.New[string, *MyStruct](cache.Config{
    MaxSize:         1000,
    TTL:             5 * time.Minute,
    CleanupInterval: time.Minute, // Optional, defaults to TTL/2
})
defer c.Close() // Important: stops background cleanup goroutine

// Set a value
c.Set("key1", &MyStruct{Data: "value"})

// Get a value
if val, ok := c.Get("key1"); ok {
    // Use val
}

// Delete a value
c.Delete("key1")

// Clear all entries
c.Clear()

// Get cache statistics
stats := c.Stats()
fmt.Printf("Hit rate: %.2f%%, Size: %d\n", stats.HitRate*100, stats.Size)
```

### Configuration

```go
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
```

### Memory Management

The cache prevents memory leaks through two mechanisms:

1. **LRU Eviction**: When `MaxSize` is reached, the least recently used entry is automatically evicted
2. **TTL Expiration**: Entries older than `TTL` are automatically removed by the background cleanup goroutine

**Important**: Always call `Close()` when done with the cache to stop the background cleanup goroutine.

### Real-World Example: Caching Sandbox Information

```go
type SandboxCache struct {
    cache *cache.Cache[string, *Sandbox]
}

func NewSandboxCache() *SandboxCache {
    return &SandboxCache{
        cache: cache.New[string, *Sandbox](cache.Config{
            MaxSize:         10000,              // Max 10k sandboxes
            TTL:             5 * time.Minute,    // Cache for 5 minutes
            CleanupInterval: 2 * time.Minute,    // Cleanup every 2 minutes
        }),
    }
}

func (sc *SandboxCache) Get(id string) (*Sandbox, bool) {
    return sc.cache.Get(id)
}

func (sc *SandboxCache) Set(id string, sandbox *Sandbox) {
    sc.cache.Set(id, sandbox)
}

func (sc *SandboxCache) Invalidate(id string) {
    sc.cache.Delete(id)
}

func (sc *SandboxCache) Close() {
    sc.cache.Close()
}
```

## Performance

Benchmarks on Apple M1:

```
BenchmarkCache_Set-8              5000000    237 ns/op     96 B/op    2 allocs/op
BenchmarkCache_Get-8             10000000    118 ns/op      0 B/op    0 allocs/op
BenchmarkCache_SetParallel-8     20000000     85 ns/op     96 B/op    2 allocs/op
BenchmarkCache_GetParallel-8     50000000     24 ns/op      0 B/op    0 allocs/op
```

## Thread Safety

All operations are thread-safe and can be called concurrently from multiple goroutines without external synchronization.
