package rootfsblock

import (
	"container/list"
	"fmt"
	"io"
	"sync"

	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/singleflight"
)

const DefaultReadCacheBytes int64 = 128 << 20

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
func NewReaderWithCache(source RangeSource, descriptor Descriptor, cache *ReadCache) (*Reader, error) {
	if source == nil {
		return nil, fmt.Errorf("range source is required")
	}
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	if cache == nil {
		return nil, fmt.Errorf("read cache is required")
	}
	reader := &Reader{source: source, descriptor: descriptor, cache: cache}
	payload, err := reader.readRange(descriptor.MappingRoot.Object)
	if err != nil {
		return nil, fmt.Errorf("read root mapping page: %w", err)
	}
	if digest.FromBytes(payload).String() != descriptor.MappingRoot.RootDigest {
		return nil, fmt.Errorf("root mapping digest does not match its descriptor")
	}
	root, err := DecodeMappingPage(payload)
	if err != nil {
		return nil, fmt.Errorf("decode root mapping page: %w", err)
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
		entry, found, err := r.resolve(r.root, block)
		if err != nil {
			return written, err
		}
		if !found {
			clear(target[written : written+chunk])
		} else {
			payload, err := r.readRange(entry.Object)
			if err != nil {
				return written, err
			}
			entryOffset := int((block-entry.LogicalStart)*LogicalBlockSize) + inBlock
			copy(target[written:written+chunk], payload[entryOffset:entryOffset+chunk])
		}
		written += chunk
	}
	if written < len(target) {
		return written, io.EOF
	}
	return written, nil
}

func (r *Reader) resolve(page MappingPage, block uint64) (MappingEntry, bool, error) {
	entry, found := page.entryFor(block)
	if !found {
		return MappingEntry{}, false, nil
	}
	if entry.Kind == MappingEntryData {
		return entry, true, nil
	}
	payload, err := r.readRange(entry.Object)
	if err != nil {
		return MappingEntry{}, false, fmt.Errorf("read mapping child: %w", err)
	}
	child, err := DecodeMappingPage(payload)
	if err != nil {
		return MappingEntry{}, false, fmt.Errorf("decode mapping child: %w", err)
	}
	if child.Level+1 != page.Level || child.StartBlock != entry.LogicalStart || child.BlockCount != uint64(entry.BlockCount) {
		return MappingEntry{}, false, fmt.Errorf("mapping child does not match its parent entry")
	}
	return r.resolve(child, block)
}

func (r *Reader) readRange(object ObjectRange) ([]byte, error) {
	cacheKey := fmt.Sprintf("%s/%d", object.Checksum, object.Length)
	if cached, ok := r.cache.get(cacheKey); ok {
		return cached, nil
	}
	value, err, _ := r.cache.requests.Do(cacheKey, func() (any, error) {
		if cached, ok := r.cache.get(cacheKey); ok {
			return cached, nil
		}
		body, err := r.source.Get(object.Key, object.Offset, object.Length)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		payload, err := io.ReadAll(io.LimitReader(body, object.Length+1))
		if err != nil {
			return nil, err
		}
		if int64(len(payload)) != object.Length {
			return nil, fmt.Errorf("object range returned %d bytes, expected %d", len(payload), object.Length)
		}
		if digest.FromBytes(payload).String() != object.Checksum {
			return nil, fmt.Errorf("object range checksum mismatch")
		}
		r.cache.add(cacheKey, payload)
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

// ReadCache is a checksum-verified, bounded LRU shared across immutable
// generation readers. The cache also coalesces concurrent misses by content.
type ReadCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	items    map[string]*list.Element
	order    *list.List
	requests singleflight.Group
}

type rangeCacheEntry struct {
	key     string
	payload []byte
}

func NewReadCache(maxBytes int64) (*ReadCache, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("cache size must be non-negative")
	}
	return &ReadCache{maxBytes: maxBytes, items: make(map[string]*list.Element), order: list.New()}, nil
}

func (c *ReadCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(element)
	return element.Value.(rangeCacheEntry).payload, true
}

func (c *ReadCache) add(key string, payload []byte) {
	if c.maxBytes == 0 || int64(len(payload)) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.items[key]; ok {
		c.order.MoveToFront(existing)
		return
	}
	clone := append([]byte(nil), payload...)
	element := c.order.PushFront(rangeCacheEntry{key: key, payload: clone})
	c.items[key] = element
	c.bytes += int64(len(clone))
	for c.bytes > c.maxBytes {
		oldest := c.order.Back()
		entry := oldest.Value.(rangeCacheEntry)
		delete(c.items, entry.key)
		c.order.Remove(oldest)
		c.bytes -= int64(len(entry.payload))
	}
}
