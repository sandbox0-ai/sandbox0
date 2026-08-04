# Memcache Package

Thread-safe in-memory cache with TTL (Time-To-Live) and LRU (Least Recently Used) eviction.

## Features

- **Thread-Safe**: Safe for concurrent access from multiple goroutines
- **TTL Support**: Stale entries are discarded when read
- **LRU Eviction**: Prevents memory leaks by limiting maximum entries
- **Generic**: Type-safe implementation using Go generics

## Usage

### Basic Example

```go
import "github.com/sandbox0-ai/sandbox0/global-gateway/pkg/memcache"

// Create a cache that holds max 1000 entries, with 5-minute TTL
c := memcache.New[string, *MyStruct](memcache.Config{
    MaxSize: 1000,
    TTL:     5 * time.Minute,
})

// Set a value
c.Set("key1", &MyStruct{Data: "value"})

// Get a value
if val, ok := c.Get("key1"); ok {
    // Use val
}

// Clear all entries
c.Clear()
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
}
```

### Memory Management

The cache prevents memory leaks through two mechanisms:

1. **LRU Eviction**: When `MaxSize` is reached, the least recently used entry is automatically evicted
2. **TTL Expiration**: Entries older than `TTL` are discarded on lookup

### Real-World Example: Caching Sandbox Information

```go
type SandboxCache struct {
    cache *memcache.Cache[string, *Sandbox]
}

func NewSandboxCache() *SandboxCache {
    return &SandboxCache{
        cache: memcache.New[string, *Sandbox](memcache.Config{
            MaxSize: 10000,           // Max 10k sandboxes
            TTL:     5 * time.Minute, // Cache for 5 minutes
        }),
    }
}

func (sc *SandboxCache) Get(id string) (*Sandbox, bool) {
    return sc.cache.Get(id)
}

func (sc *SandboxCache) Set(id string, sandbox *Sandbox) {
    sc.cache.Set(id, sandbox)
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
