package objectstore

import (
	"container/list"
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"hash/fnv"
	"strings"
	"sync"
)

// EncryptedHeaderCacheConfig bounds an immutable encrypted store's private LRU.
// MaxBytes charges the serialized header, key, and a fixed allowance for parsed
// crypto state per entry; ciphertext and plaintext are never retained. Both
// limits must be positive to enable caching. Header loads are also bounded to
// min(MaxEntries, 32), each subject to maxEncryptedObjectHeaderBytes.
type EncryptedHeaderCacheConfig struct {
	MaxEntries int
	MaxBytes   int64
	// MaxPrefixBytes optionally co-reads a bounded offset-zero demand with a
	// cold header. Ciphertext belongs only to the initiating read, never the LRU
	// or other waiters. Zero disables this; values are capped at 1 MiB. Header
	// loads keep their existing 32-loader admission. Active returned readers
	// own their buffers separately from the cache's memory budget.
	MaxPrefixBytes int64
	// MaxParallelReadBytes optionally overlaps a cold nonzero-offset demand
	// with its header load. Hinted ciphertext is private to the caller and
	// must match the validated stored layout before use. Zero disables this;
	// at most eight probes of at most 1 MiB may be outstanding per wrapper.
	MaxParallelReadBytes int64
}

type encryptedObjectMetadata struct {
	header    encryptedObjectHeader
	headerEnd int64
	aead      cipher.AEAD
	identity  [sha256.Size]byte
}

type encryptedHeaderEntry struct {
	key      string
	metadata *encryptedObjectMetadata
	bytes    int64
}

type encryptedHeaderFlight struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	waiters  int
	epoch    uint64
	metadata *encryptedObjectMetadata
	err      error
}

type encryptedHeaderCache struct {
	mu      sync.Mutex
	cfg     EncryptedHeaderCacheConfig
	entries map[string]*list.Element
	lru     list.List
	bytes   int64
	flights map[string]*encryptedHeaderFlight
	changed chan struct{}
	fences  [256]encryptedHeaderFence
}

type encryptedHeaderFence struct {
	epoch     uint64
	mutations int
}

var errEncryptedHeaderInvalidated = errors.New("encrypted object header invalidated during load")

func (c *encryptedHeaderCache) contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[key] != nil
}

func newEncryptedHeaderCache(cfg EncryptedHeaderCacheConfig) *encryptedHeaderCache {
	if cfg.MaxEntries <= 0 || cfg.MaxBytes <= 0 {
		return nil
	}
	return &encryptedHeaderCache{
		cfg: cfg, entries: make(map[string]*list.Element),
		flights: make(map[string]*encryptedHeaderFlight), changed: make(chan struct{}),
	}
}

// get coalesces header parsing and key unwrap, while leaving range data reads
// independent. A caller's deadline only cancels its wait. The shared load is
// canceled when its last waiter leaves, and retains its slot until it exits so
// even a provider that ignores cancellation cannot create unbounded loaders.
func (c *encryptedHeaderCache) get(ctx context.Context, key string, load func(context.Context) (*encryptedObjectMetadata, error)) (*encryptedObjectMetadata, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if element := c.entries[key]; element != nil {
			c.lru.MoveToFront(element)
			metadata := element.Value.(encryptedHeaderEntry).metadata
			c.mu.Unlock()
			return metadata, nil
		}
		flight := c.flights[key]
		if flight != nil && (flight.waiters == 0 || flight.epoch != c.fence(key).epoch) {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-flight.done:
				continue
			}
		}
		if flight == nil {
			if len(c.flights) >= min(c.cfg.MaxEntries, 32) {
				changed := c.changed
				c.mu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-changed:
					continue
				}
			}
			loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			flight = &encryptedHeaderFlight{ctx: loadCtx, cancel: cancel, done: make(chan struct{}), epoch: c.fence(key).epoch}
			c.flights[key] = flight
			go c.run(key, flight, load)
		}
		flight.waiters++
		c.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-flight.done:
		}
		c.mu.Lock()
		flight.waiters--
		if flight.waiters == 0 {
			flight.cancel()
		}
		c.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errors.Is(flight.err, errEncryptedHeaderInvalidated) {
			continue
		}
		return flight.metadata, flight.err
	}
}

func (c *encryptedHeaderCache) run(key string, flight *encryptedHeaderFlight, load func(context.Context) (*encryptedObjectMetadata, error)) {
	metadata, err := load(flight.ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	defer flight.cancel()
	fence := c.fence(key)
	if flight.epoch != fence.epoch {
		metadata, err = nil, errEncryptedHeaderInvalidated
	} else if flight.ctx.Err() != nil {
		metadata, err = nil, flight.ctx.Err()
	}
	if err == nil && metadata != nil && fence.mutations == 0 {
		// This deliberately overcharges discarded wrapped-key bytes and reserves
		// space for the AES/ChaCha state, map/list bookkeeping and parsed fields.
		weight := metadata.headerEnd + int64(len(key)) + 2048
		if weight <= c.cfg.MaxBytes {
			for len(c.entries) >= c.cfg.MaxEntries || c.bytes > c.cfg.MaxBytes-weight {
				c.remove(c.lru.Back())
			}
			c.entries[key] = c.lru.PushFront(encryptedHeaderEntry{key: key, metadata: metadata, bytes: weight})
			c.bytes += weight
		}
	}
	flight.metadata, flight.err = metadata, err
	delete(c.flights, key)
	close(flight.done)
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *encryptedHeaderCache) remove(element *list.Element) {
	entry := element.Value.(encryptedHeaderEntry)
	delete(c.entries, entry.key)
	c.bytes -= entry.bytes
	c.lru.Remove(element)
}

// forget does not let a late failing reader evict a newer load of the same key.
func (c *encryptedHeaderCache) forget(key string, metadata *encryptedObjectMetadata) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil && element.Value.(encryptedHeaderEntry).metadata == metadata {
		c.remove(element)
	}
}

// Invalidate only the mutated key, including aliases normalized by the built-in
// stores. Fixed hash stripes bound mutation bookkeeping regardless of write
// concurrency. A collision may fence an unrelated in-flight load but never
// evicts an unrelated cached header. No per-key tombstones survive deletion.
func (c *encryptedHeaderCache) beginMutation(key string) func() {
	c.mu.Lock()
	c.fence(key).mutations++
	c.invalidate(key)
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.fence(key).mutations--
		c.invalidate(key)
	}
}

func (c *encryptedHeaderCache) invalidate(key string) {
	c.fence(key).epoch++
	key = encryptedHeaderMutationKey(key)
	for cachedKey, element := range c.entries {
		if encryptedHeaderMutationKey(cachedKey) == key {
			c.remove(element)
		}
	}
}

func (c *encryptedHeaderCache) fence(key string) *encryptedHeaderFence {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(encryptedHeaderMutationKey(key)))
	return &c.fences[hash.Sum64()%uint64(len(c.fences))]
}

func encryptedHeaderMutationKey(key string) string {
	return strings.TrimLeft(strings.TrimSpace(key), "/")
}
