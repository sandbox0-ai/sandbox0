package http

import (
	"testing"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSandboxCache(t *testing.T) {
	// Create a cache
	c := cache.New[string, *mgr.Sandbox](cache.Config{
		MaxSize:         100,
		TTL:             time.Minute,
		CleanupInterval: time.Second,
	})
	defer c.Close()

	// Test cache miss
	_, ok := c.Get("sandbox-1")
	assert.False(t, ok)

	// Test cache set and get
	sandbox := &mgr.Sandbox{
		ID:           "sandbox-1",
		TeamID:       "team-1",
		UserID:       "user-1",
		InternalAddr: "http://10.0.0.1:8080",
		Status:       "running",
	}

	c.Set("sandbox-1", sandbox)

	// Test cache hit
	cached, ok := c.Get("sandbox-1")
	require.True(t, ok)
	assert.Equal(t, sandbox.ID, cached.ID)
	assert.Equal(t, sandbox.TeamID, cached.TeamID)
	assert.Equal(t, sandbox.InternalAddr, cached.InternalAddr)

	// Test cache stats
	stats := c.Stats()
	assert.Equal(t, uint64(1), stats.Hits)
	assert.Equal(t, uint64(1), stats.Misses)
	assert.Equal(t, 1, stats.Size)

	// Test cache delete
	c.Delete("sandbox-1")
	_, ok = c.Get("sandbox-1")
	assert.False(t, ok)
}

func TestSandboxCache_TTL(t *testing.T) {
	// Create a cache with short TTL
	c := cache.New[string, *mgr.Sandbox](cache.Config{
		MaxSize:         100,
		TTL:             100 * time.Millisecond,
		CleanupInterval: 50 * time.Millisecond,
	})
	defer c.Close()

	sandbox := &mgr.Sandbox{
		ID:           "sandbox-1",
		TeamID:       "team-1",
		InternalAddr: "http://10.0.0.1:8080",
	}

	c.Set("sandbox-1", sandbox)

	// Should exist immediately
	_, ok := c.Get("sandbox-1")
	assert.True(t, ok)

	// Should expire after TTL
	time.Sleep(150 * time.Millisecond)
	_, ok = c.Get("sandbox-1")
	assert.False(t, ok)
}

func TestSandboxCache_LRUEviction(t *testing.T) {
	// Create a cache with small max size
	c := cache.New[string, *mgr.Sandbox](cache.Config{
		MaxSize:         3,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
	})
	defer c.Close()

	// Fill cache to capacity
	for i := 1; i <= 3; i++ {
		sandbox := &mgr.Sandbox{
			ID:           "sandbox-" + string(rune('0'+i)),
			TeamID:       "team-1",
			InternalAddr: "http://10.0.0.1:8080",
		}
		c.Set(sandbox.ID, sandbox)
	}

	// All should exist
	for i := 1; i <= 3; i++ {
		id := "sandbox-" + string(rune('0'+i))
		_, ok := c.Get(id)
		assert.True(t, ok, "sandbox %s should exist", id)
	}

	// Add one more, should evict LRU (sandbox-1)
	sandbox4 := &mgr.Sandbox{
		ID:           "sandbox-4",
		TeamID:       "team-1",
		InternalAddr: "http://10.0.0.1:8080",
	}
	c.Set(sandbox4.ID, sandbox4)

	// sandbox-1 should be evicted
	_, ok := c.Get("sandbox-1")
	assert.False(t, ok)

	// Others should still exist
	_, ok = c.Get("sandbox-2")
	assert.True(t, ok)
	_, ok = c.Get("sandbox-3")
	assert.True(t, ok)
	_, ok = c.Get("sandbox-4")
	assert.True(t, ok)
}
