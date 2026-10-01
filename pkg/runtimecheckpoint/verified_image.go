package runtimecheckpoint

import (
	"sync"
	"time"
)

const verifiedImageEntries = 16
const verifiedImageLifetime = 30 * time.Second

// verifiedImageCache is disposable, single-use evidence for the private files
// just materialized by this Store. It is never serialized or accepted from a
// peer/journal. Kernel watches and exact inode metadata must still be intact.
// Capacity and timers bound idle watches even if preparation is never restored.
type verifiedImageCache struct {
	mu      sync.Mutex
	entries [verifiedImageEntries]*verifiedImageEntry
	next    int
	closed  bool
}

type verifiedImageEntry struct {
	reference Reference
	directory string
	guard     *imageVerificationGuard
	timer     *time.Timer
}

func (c *verifiedImageCache) put(ref Reference, directory string, guard *imageVerificationGuard) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		guard.close()
		return
	}
	if old := c.entries[c.next]; old != nil {
		old.timer.Stop()
		old.guard.close()
	}
	entry := &verifiedImageEntry{reference: ref, directory: directory, guard: guard}
	index := c.next
	c.entries[index] = entry
	c.next = (index + 1) % len(c.entries)
	entry.timer = time.AfterFunc(verifiedImageLifetime, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.entries[index] == entry {
			c.entries[index] = nil
			entry.guard.close()
		}
	})
}

func (c *verifiedImageCache) take(ref Reference, directory string) *imageVerificationGuard {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, entry := range c.entries {
		if entry != nil && entry.reference == ref && entry.directory == directory {
			c.entries[i] = nil
			entry.timer.Stop()
			return entry.guard
		}
	}
	return nil
}

func (c *verifiedImageCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for i, entry := range c.entries {
		if entry != nil {
			entry.timer.Stop()
			entry.guard.close()
			c.entries[i] = nil
		}
	}
}

// Close releases image watches and retained-cache handles. Cached files remain
// disposable across restart. Regional storage and shared chunk cache stay open.
func (s *Store) Close() {
	if s != nil {
		s.verifiedImages.close()
		if c := s.retainedCache(); c != nil {
			c.close()
		}
	}
}
