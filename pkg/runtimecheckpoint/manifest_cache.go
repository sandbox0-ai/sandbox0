package runtimecheckpoint

import "sync"

const (
	manifestCacheEntries = 16
	manifestCacheBytes   = 8 << 20
)

// manifestCache retains only manifests read from regional storage and checked
// against an exact committed reference. It is not publication or execution
// authority. In particular, peer plans cannot populate it, and VerifyLocal
// still reads and hashes every local image chunk on every invocation.
//
// Immutable strings prevent a caller from modifying cached metadata. FIFO
// eviction bounds both payload bytes and entry count independently of node
// uptime or the number of completed migrations. Restart simply loses the cache.
type manifestCache struct {
	mu      sync.Mutex
	entries [manifestCacheEntries]manifestCacheEntry
	next    int
	bytes   int
}

type manifestCacheEntry struct {
	reference Reference
	payload   string
}

func (c *manifestCache) get(ref Reference) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries {
		if entry.payload != "" && entry.reference == ref {
			return entry.payload, true
		}
	}
	return "", false
}

func (c *manifestCache) put(ref Reference, payload string) {
	if len(payload) == 0 || len(payload) > MaxManifestBytes || len(payload) > manifestCacheBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries {
		if entry.payload != "" && entry.reference == ref {
			return
		}
	}
	// Always evict the next ring slot; additional slots are removed only when
	// the byte budget requires it. Empty slots cost no payload capacity.
	for {
		c.bytes -= len(c.entries[c.next].payload)
		c.entries[c.next] = manifestCacheEntry{}
		if c.bytes+len(payload) <= manifestCacheBytes {
			break
		}
		c.next = (c.next + 1) % len(c.entries)
	}
	c.entries[c.next] = manifestCacheEntry{reference: ref, payload: payload}
	c.bytes += len(payload)
	c.next = (c.next + 1) % len(c.entries)
}
