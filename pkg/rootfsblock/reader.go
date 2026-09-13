package rootfsblock

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

const DefaultReadCacheBytes int64 = 128 << 20

const maxConcurrentSourceReads = 8

// RangeSource performs one exact immutable object range read. Implementations
// must not issue a preceding HEAD request.
type RangeSource interface {
	Get(key string, offset, length int64) (io.ReadCloser, error)
}

// Reader exposes one immutable S3-materialized generation as an io.ReaderAt.
// Composite tails use a separate replay path and are rejected until their
// ordering format has been attached explicitly.
type Reader struct {
	source     RangeSource
	lifetime   context.Context
	descriptor Descriptor
	root       MappingPage
	tail       map[uint64][]byte
	cache      *ReadCache
}

func NewReader(source RangeSource, descriptor Descriptor, cacheBytes int64) (*Reader, error) {
	cache, err := NewReadCache(cacheBytes)
	if err != nil {
		return nil, err
	}
	return NewReaderWithCache(source, descriptor, cache)
}

// NewReaderWithCache opens a generation using a cache shared by all readers
// on the node. Cached payloads are inserted only after checksum validation.
// This constructor retains the legacy Get transport contract; mounted node
// readers should explicitly bind their I/O lifetime with the context variant.
func NewReaderWithCache(source RangeSource, descriptor Descriptor, cache *ReadCache) (*Reader, error) {
	return newReaderWithCache(nil, source, descriptor, cache)
}

// NewReaderWithCacheContext binds source admission and context-aware transport
// to the Reader's entire I/O lifetime, not just this constructor. A mounted
// generation must use its device/node lifetime, never a short-lived claim HTTP
// context. Legacy sources without GetContext cannot cancel an active Get.
// Shared misses retain singleflight semantics and the initiating Reader's
// transport lifetime; this is not independent per-caller cancellation.
func NewReaderWithCacheContext(lifetime context.Context, source RangeSource, descriptor Descriptor, cache *ReadCache) (*Reader, error) {
	if lifetime == nil {
		return nil, fmt.Errorf("reader lifetime is required")
	}
	if err := lifetime.Err(); err != nil {
		return nil, err
	}
	return newReaderWithCache(lifetime, source, descriptor, cache)
}

func newReaderWithCache(lifetime context.Context, source RangeSource, descriptor Descriptor, cache *ReadCache) (*Reader, error) {
	if source == nil {
		return nil, fmt.Errorf("range source is required")
	}
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	if cache == nil {
		return nil, fmt.Errorf("read cache is required")
	}
	reader := &Reader{source: source, lifetime: lifetime, descriptor: descriptor, cache: cache}
	root, err := reader.readMappingPage(descriptor.MappingRoot.Object)
	if err != nil {
		return nil, fmt.Errorf("read root mapping page: %w", err)
	}
	if root.formatVersion() != descriptor.MappingRoot.Version {
		return nil, fmt.Errorf("root mapping format does not match descriptor")
	}
	if descriptor.MappingRoot.Object.Checksum != descriptor.MappingRoot.RootDigest {
		return nil, fmt.Errorf("root mapping digest does not match its descriptor")
	}
	expectedBlocks := uint64(descriptor.LogicalSizeBytes / descriptor.BlockSizeBytes)
	if root.StartBlock != 0 || root.BlockCount != expectedBlocks {
		return nil, fmt.Errorf("root mapping page does not cover the logical device")
	}
	reader.root = root
	if descriptor.CompositeTail != nil {
		records, _, err := DecodeCompositeTail(*descriptor.CompositeTail, expectedBlocks)
		if err != nil {
			return nil, err
		}
		reader.tail = make(map[uint64][]byte, len(records))
		for _, record := range records {
			reader.tail[record.Block] = record.Data
		}
	}
	return reader, nil
}

func (r *Reader) Size() int64 { return r.descriptor.LogicalSizeBytes }

func (r *Reader) ReadAt(target []byte, offset int64) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	if offset < 0 {
		return 0, fmt.Errorf("read offset must be non-negative")
	}
	if offset >= r.Size() {
		return 0, io.EOF
	}
	wanted := len(target)
	if remaining := r.Size() - offset; int64(wanted) > remaining {
		wanted = int(remaining)
	}
	written := 0
	demand := dataReadDemand{offset: offset, bytes: int64(wanted), coalesce: wanted >= bulkReadThreshold}
	var activeStart uint64
	var activePayload []byte
	for written < wanted {
		absolute := offset + int64(written)
		block := uint64(absolute / LogicalBlockSize)
		inBlock := int(absolute % LogicalBlockSize)
		chunk := min(wanted-written, LogicalBlockSize-inBlock)
		if tail, ok := r.tail[block]; ok {
			copy(target[written:written+chunk], tail[inBlock:inBlock+chunk])
			written += chunk
			continue
		}
		if activePayload == nil || block < activeStart || block >= activeStart+uint64(len(activePayload)/LogicalBlockSize) {
			entry, leaf, found, err := r.resolve(r.root, block)
			if err != nil {
				return written, err
			}
			if !found {
				clear(target[written : written+chunk])
				written += chunk
				continue
			}
			activeStart, activePayload, err = r.readDataRange(entry, leaf, &demand)
			if err != nil {
				return written, err
			}
		}
		// Retain this verified range for the duration of the caller's demand.
		// Concurrent LRU eviction (or a disabled cache) must not refetch the
		// same range for every 4 KiB block of one read. Tail overrides above
		// still win even when they interrupt an otherwise contiguous extent.
		entryOffset := int((block-activeStart)*LogicalBlockSize) + inBlock
		copy(target[written:written+chunk], activePayload[entryOffset:entryOffset+chunk])
		written += chunk
	}
	if written < len(target) {
		return written, io.EOF
	}
	return written, nil
}

func (r *Reader) resolve(page MappingPage, block uint64) (MappingEntry, MappingPage, bool, error) {
	entry, found := page.entryFor(block)
	if !found {
		return MappingEntry{}, MappingPage{}, false, nil
	}
	if entry.Kind == MappingEntryData {
		return entry, page, true, nil
	}
	child, err := r.readMappingChildGroup(page, entry.Object)
	if err != nil {
		return MappingEntry{}, MappingPage{}, false, fmt.Errorf("read mapping child: %w", err)
	}
	if child.formatVersion() != page.formatVersion() || child.Level+1 != page.Level || child.StartBlock != entry.LogicalStart || child.BlockCount != uint64(entry.BlockCount) {
		return MappingEntry{}, MappingPage{}, false, fmt.Errorf("mapping child does not match its parent entry")
	}
	return r.resolve(child, block)
}

// readMappingPage shares only checksum-verified, fully decoded immutable pages.
// The caller must still validate the exact parent binding: identical page bytes
// cached by another reader do not prove this descriptor's tree is well formed.
func (r *Reader) readMappingPage(object ObjectRange) (MappingPage, error) {
	return r.readMappingPageUsing(object, nil)
}

func (r *Reader) readMappingPageUsing(object ObjectRange, load func() ([]byte, error)) (MappingPage, error) {
	key := rangeCacheKey(object)
	if page, ok := r.cache.getPage(key); ok {
		return page, nil
	}
	value, err, _ := r.cache.pageRequests.Do(key.flightKey(), func() (any, error) {
		if page, ok := r.cache.getPage(key); ok {
			return page, nil
		}
		var payload []byte
		var err error
		if load == nil {
			payload, err = r.readRange(object)
		} else {
			payload, err = load()
		}
		if err != nil {
			return MappingPage{}, err
		}
		if load != nil {
			release, err := r.acquireSourceSlot()
			if err != nil {
				return MappingPage{}, err
			}
			defer release()
		}
		page, err := DecodeMappingPage(payload)
		if err != nil {
			return MappingPage{}, fmt.Errorf("decode mapping page: %w", err)
		}
		r.cache.decodes.Add(1)
		r.cache.addPage(key, payload, page)
		return page, nil
	})
	if err != nil {
		return MappingPage{}, err
	}
	return value.(MappingPage), nil
}

type readCacheKey struct {
	checksum string
	length   int64
}

func rangeCacheKey(object ObjectRange) readCacheKey {
	return readCacheKey{checksum: object.Checksum, length: object.Length}
}

func (key readCacheKey) flightKey() string {
	return fmt.Sprintf("%s/%d", key.checksum, key.length)
}

// readRange returns verified immutable bytes shared with the cache and other
// readers. Callers must not mutate or recycle the returned backing array.
func (r *Reader) readRange(object ObjectRange) ([]byte, error) {
	if err := object.Validate(MaxMappingRootBytes); err != nil {
		return nil, err
	}
	cacheKey := rangeCacheKey(object)
	if cached, ok := r.cache.get(cacheKey); ok {
		return cached, nil
	}
	value, err, _ := r.cache.requests.Do(cacheKey.flightKey(), func() (any, error) {
		if cached, ok := r.cache.get(cacheKey); ok {
			return cached, nil
		}
		release, err := r.acquireSourceSlot()
		if err != nil {
			return nil, err
		}
		defer release()
		if cached, ok := r.cache.get(cacheKey); ok {
			return cached, nil
		}
		payload, err := r.readSourceRange(object.Key, object.Offset, object.StoredLength())
		if err != nil {
			return nil, err
		}
		payload, err = decodeRangePayload(r.ioLifetime(), object, payload)
		if err != nil {
			return nil, err
		}
		r.cache.addVerified(cacheKey, payload)
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

// acquireSourceSlot belongs inside singleflight and remains held through
// checksum verification and cache admission, bounding distinct in-flight
// source buffers. A source may issue multiple HTTP requests for one load.
// Waiters for the same flight take no slot.
func (r *Reader) acquireSourceSlot() (func(), error) {
	return r.cache.sourceSlots.acquire(r.ioLifetime(), r)
}

func (r *Reader) ioLifetime() context.Context {
	if r.lifetime != nil {
		return r.lifetime
	}
	return context.Background()
}

// readSourceRange only enforces bounded transport reads. Its callers hold a
// source slot and verify their immutable checksums before exposing/caching bytes.
func (r *Reader) readSourceRange(key string, offset, length int64) ([]byte, error) {
	payload := make([]byte, length+1)
	if err := r.readSourceRangeInto(key, offset, payload); err != nil {
		return nil, err
	}
	return payload[:length], nil
}

// readSourceRangeInto fills an owned, unverified buffer whose last byte is
// reserved for excess-body detection. Coalescing can read into a subspan of its
// bounded output buffer without allocating a second bulk payload. Callers hold
// a source slot and must authenticate every exposed byte after this returns.
func (r *Reader) readSourceRangeInto(key string, offset int64, payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("object range buffer must include payload and excess byte")
	}
	length := int64(len(payload) - 1)
	ctx := r.ioLifetime()
	if err := ctx.Err(); err != nil {
		return err
	}
	var body io.ReadCloser
	var err error
	if source, ok := r.source.(interface {
		GetContext(context.Context, string, int64, int64) (io.ReadCloser, error)
	}); ok && r.lifetime != nil {
		body, err = source.GetContext(ctx, key, offset, length)
	} else {
		body, err = r.source.Get(key, offset, length)
	}
	if err != nil {
		return err
	}
	defer body.Close()
	// Mapping validation or the bounded coalescing plan checked this length.
	// Preserve terminal errors even when Read supplies its final byte with an
	// error; ReadFull would discard that error after filling the destination.
	read := 0
	for read < len(payload) {
		n, readErr := body.Read(payload[read:])
		read += n
		if readErr != nil {
			if readErr != io.EOF {
				return readErr
			}
			break
		}
	}
	if int64(read) != length {
		return fmt.Errorf("object range returned %d bytes, expected %d", read, length)
	}
	return nil
}

// ReadCache is checksum-verified and shared across immutable generation readers.
// A bounded segment protects decoded mapping pages from data scans; remaining
// entries use LRU eviction within the same total budget. It coalesces misses by
// content and bounds distinct source loads across all Readers, even with no LRU
// storage. Waiters rotate by Reader, not by tenant or object identity.
type ReadCache struct {
	sourceSlots   sourceReadAdmission
	mu            sync.Mutex
	maxBytes      int64
	bytes         int64
	items         map[readCacheKey]*list.Element
	order         *list.List
	mappingOrder  *list.List
	mappingBudget int64
	mappingBytes  int64
	requests      singleflight.Group
	pageRequests  singleflight.Group
	decodes       atomic.Uint64
}

type rangeCacheEntry struct {
	key       readCacheKey
	payload   []byte
	page      *MappingPage
	protected bool
	bytes     int64
}

func NewReadCache(maxBytes int64) (*ReadCache, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("cache size must be non-negative")
	}
	cache := &ReadCache{maxBytes: maxBytes, items: make(map[readCacheKey]*list.Element), order: list.New()}
	cache.mappingOrder = list.New()
	cache.mappingBudget = min(maxBytes/8, int64(16<<20))
	return cache, nil
}

func (c *ReadCache) get(key readCacheKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.touchLocked(element)
	return element.Value.(rangeCacheEntry).payload, true
}

// addVerified retains a verified immutable buffer without copying it. Its
// backing array must remain immutable even after eviction: active reads and
// singleflight recipients may still hold it independently of the LRU.
func (c *ReadCache) addVerified(key readCacheKey, payload []byte) {
	entryBytes := rangeCacheBytes(key, payload)
	if c.maxBytes == 0 || entryBytes > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.items[key]; ok {
		c.touchLocked(existing)
		return
	}
	element := c.order.PushFront(rangeCacheEntry{key: key, payload: payload, bytes: entryBytes})
	c.items[key] = element
	c.bytes += entryBytes
	c.evictLocked()
}

func (c *ReadCache) getPage(key readCacheKey) (MappingPage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok || element.Value.(rangeCacheEntry).page == nil {
		return MappingPage{}, false
	}
	c.touchLocked(element)
	return *element.Value.(rangeCacheEntry).page, true
}

// addPage retains immutable verified bytes and decoded entries. The raw entry
// may have been evicted during decoding; neither that eviction nor this one
// invalidates a reader's independently retained reference.
func (c *ReadCache) addPage(key readCacheKey, payload []byte, page MappingPage) {
	// Account for decoded slice/entry/string storage, not just the encoded
	// payload. Conservative fixed overheads cover headers on supported arches.
	decodedBytes := int64(128 + cap(page.Entries)*128)
	for _, entry := range page.Entries {
		decodedBytes += int64(len(entry.Object.Key) + len(entry.Object.Checksum))
	}
	entryBytes := rangeCacheBytes(key, payload) + decodedBytes
	if c.maxBytes == 0 || entryBytes > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, exists := c.items[key]; exists {
		entry := element.Value.(rangeCacheEntry)
		if entry.page == nil {
			c.bytes += entryBytes - entry.bytes
			entry.page, entry.bytes = &page, entryBytes
			element.Value = entry
		}
		c.touchLocked(element)
	} else {
		entry := rangeCacheEntry{key: key, payload: payload, page: &page, bytes: entryBytes}
		c.items[key] = c.order.PushFront(entry)
		c.touchLocked(c.items[key])
		c.bytes += entryBytes
	}
	c.evictLocked()
}

func rangeCacheBytes(key readCacheKey, payload []byte) int64 {
	// Include the checksum and conservative map/list/entry overhead so many
	// small mapping objects cannot turn the payload budget into an unbounded
	// metadata cache. Charge buffer capacity, including the excess-detection
	// byte retained by a range read, rather than only its visible payload.
	return int64(cap(payload)+len(key.checksum)) + 192
}

func (c *ReadCache) evictLocked() {
	for c.bytes > c.maxBytes {
		oldest := c.order.Back()
		if oldest == nil {
			oldest = c.mappingOrder.Back()
		}
		entry := oldest.Value.(rangeCacheEntry)
		delete(c.items, entry.key)
		if entry.protected {
			c.mappingOrder.Remove(oldest)
			c.mappingBytes -= entry.bytes
		} else {
			c.order.Remove(oldest)
		}
		c.bytes -= entry.bytes
	}
}
