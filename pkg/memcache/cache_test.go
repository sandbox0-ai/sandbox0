package memcache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_BasicOperations(t *testing.T) {
	c := New[string, int](Config{
		MaxSize:         100,
		TTL:             time.Minute,
		CleanupInterval: time.Second,
	})
	defer c.Close()

	// Set and Get
	c.Set("key1", 100)
	val, ok := c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, 100, val)

	// Get non-existent key
	_, ok = c.Get("key2")
	assert.False(t, ok)

	// Update existing key
	c.Set("key1", 200)
	val, ok = c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, 200, val)

	// Delete
	c.Delete("key1")
	_, ok = c.Get("key1")
	assert.False(t, ok)
}

func TestCache_TTL(t *testing.T) {
	c := New[string, string](Config{
		MaxSize:         100,
		TTL:             100 * time.Millisecond,
		CleanupInterval: 50 * time.Millisecond,
	})
	defer c.Close()

	c.Set("key1", "value1")

	// Should exist immediately
	val, ok := c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, "value1", val)

	// Should expire after TTL
	time.Sleep(150 * time.Millisecond)
	_, ok = c.Get("key1")
	assert.False(t, ok)
}

func TestCache_LRUEviction(t *testing.T) {
	c := New[int, string](Config{
		MaxSize:         3,
		TTL:             time.Minute,
		CleanupInterval: time.Second,
	})
	defer c.Close()

	// Fill cache to capacity
	c.Set(1, "one")
	c.Set(2, "two")
	c.Set(3, "three")

	// All should exist
	_, ok := c.Get(1)
	assert.True(t, ok)
	_, ok = c.Get(2)
	assert.True(t, ok)
	_, ok = c.Get(3)
	assert.True(t, ok)

	// Add one more, should evict LRU (which is 1)
	c.Set(4, "four")

	// 1 should be evicted
	_, ok = c.Get(1)
	assert.False(t, ok)

	// Others should still exist
	_, ok = c.Get(2)
	assert.True(t, ok)
	_, ok = c.Get(3)
	assert.True(t, ok)
	_, ok = c.Get(4)
	assert.True(t, ok)

	// Access 2 to make it most recently used
	c.Get(2)

	// Add another item, should evict 3 (LRU)
	c.Set(5, "five")

	_, ok = c.Get(3)
	assert.False(t, ok)
	_, ok = c.Get(2)
	assert.True(t, ok)
}

func TestCache_Clear(t *testing.T) {
	c := New[string, int](Config{
		MaxSize:         100,
		TTL:             time.Minute,
		CleanupInterval: time.Second,
	})
	defer c.Close()

	c.Set("key1", 1)
	c.Set("key2", 2)
	c.Set("key3", 3)

	assert.Equal(t, 3, c.Len())

	c.Clear()

	assert.Equal(t, 0, c.Len())
	_, ok := c.Get("key1")
	assert.False(t, ok)
}

func TestCache_Stats(t *testing.T) {
	c := New[string, int](Config{
		MaxSize:         100,
		TTL:             time.Minute,
		CleanupInterval: time.Second,
	})
	defer c.Close()

	c.Set("key1", 1)
	c.Set("key2", 2)

	// Hit
	c.Get("key1")
	// Hit
	c.Get("key2")
	// Miss
	c.Get("key3")

	stats := c.Stats()
	assert.Equal(t, uint64(2), stats.Hits)
	assert.Equal(t, uint64(1), stats.Misses)
	assert.Equal(t, 2, stats.Size)
	assert.InDelta(t, 0.666, stats.HitRate, 0.01)
}

func TestCache_Cleanup(t *testing.T) {
	c := New[string, string](Config{
		MaxSize:         100,
		TTL:             50 * time.Millisecond,
		CleanupInterval: 30 * time.Millisecond,
	})
	defer c.Close()

	c.Set("key1", "value1")
	c.Set("key2", "value2")

	assert.Equal(t, 2, c.Len())

	// Wait for cleanup to run
	time.Sleep(100 * time.Millisecond)

	// Entries should be cleaned up
	assert.Equal(t, 0, c.Len())
}

func TestCache_Concurrency(t *testing.T) {
	c := New[int, int](Config{
		MaxSize:         1000,
		TTL:             time.Second,
		CleanupInterval: 100 * time.Millisecond,
	})
	defer c.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := base*numOps + j
				c.Set(key, key*2)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := base*numOps + j
				c.Get(key)
			}
		}(i)
	}

	// Concurrent deletes
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOps/2; j++ {
				key := base*numOps + j
				c.Delete(key)
			}
		}(i)
	}

	wg.Wait()

	// Just verify cache is still operational
	c.Set(9999, 9999)
	val, ok := c.Get(9999)
	assert.True(t, ok)
	assert.Equal(t, 9999, val)
}

func TestCache_ZeroTTL(t *testing.T) {
	c := New[string, string](Config{
		MaxSize:         10,
		TTL:             0, // No expiration
		CleanupInterval: time.Second,
	})
	defer c.Close()

	c.Set("key1", "value1")

	// Should never expire
	time.Sleep(100 * time.Millisecond)
	val, ok := c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, "value1", val)
}

func BenchmarkCache_Set(b *testing.B) {
	c := New[int, int](Config{
		MaxSize:         10000,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
	})
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i%10000, i)
	}
}

func BenchmarkCache_Get(b *testing.B) {
	c := New[int, int](Config{
		MaxSize:         10000,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
	})
	defer c.Close()

	// Pre-populate cache
	for i := 0; i < 10000; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(i % 10000)
	}
}

func BenchmarkCache_SetParallel(b *testing.B) {
	c := New[int, int](Config{
		MaxSize:         10000,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
	})
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i%10000, i)
			i++
		}
	})
}

func BenchmarkCache_GetParallel(b *testing.B) {
	c := New[int, int](Config{
		MaxSize:         10000,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
	})
	defer c.Close()

	// Pre-populate cache
	for i := 0; i < 10000; i++ {
		c.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(i % 10000)
			i++
		}
	})
}
