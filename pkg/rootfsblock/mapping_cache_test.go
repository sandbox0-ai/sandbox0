package rootfsblock

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMappingCacheSharesDecodedPagesAcrossConcurrentReaders(t *testing.T) {
	store, descriptor, leaf := mappingCacheFixture(t, 32)
	cache, err := NewReadCache(DefaultReadCacheBytes)
	require.NoError(t, err)
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			reader, openErr := NewReaderWithCache(store, descriptor, cache)
			require.NoError(t, openErr)
			for block := range 32 {
				payload := make([]byte, LogicalBlockSize)
				_, readErr := reader.ReadAt(payload, int64(block*LogicalBlockSize))
				require.NoError(t, readErr)
				require.Equal(t, byte(block+1), payload[0])
			}
		}()
	}
	wait.Wait()
	require.Equal(t, uint64(2), cache.decodes.Load(), "root and leaf must each be decoded once")
	require.Equal(t, 1, store.count(leaf.Key))
	require.LessOrEqual(t, cache.bytes, cache.maxBytes)
}

func TestMappingCacheHitStillValidatesTheExactParent(t *testing.T) {
	store, descriptor, leaf := mappingCacheFixture(t, 2)
	cache, err := NewReadCache(DefaultReadCacheBytes)
	require.NoError(t, err)
	reader, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.NoError(t, err)
	_, cached := cache.getPage(rangeCacheKey(leaf))
	require.True(t, cached)
	badParent, err := EncodeMappingPage(MappingPage{
		Level: 1, StartBlock: 0, BlockCount: 2,
		Entries: []MappingEntry{{LogicalStart: 0, BlockCount: 1, Kind: MappingEntryChild, Object: leaf}},
	})
	require.NoError(t, err)
	root := store.put("maps/bad-parent", badParent)
	badReader, err := NewReaderWithCache(store, testReaderDescriptor(root, 2), cache)
	require.NoError(t, err)
	_, err = badReader.ReadAt(make([]byte, 1), 0)
	require.ErrorContains(t, err, "mapping child does not match")
	require.Equal(t, 1, store.count(leaf.Key), "a cache hit must not bypass the parent binding check")
}

func TestMappingCacheChargesDecodedPagesAndEvictsWithinSharedBudget(t *testing.T) {
	store, descriptor, leaf := mappingCacheFixture(t, 16)
	cache, err := NewReadCache(16384)
	require.NoError(t, err)
	reader, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.NoError(t, err)
	page, cached := cache.getPage(rangeCacheKey(leaf))
	require.True(t, cached)
	require.Len(t, page.Entries, 16)
	entry := cache.items[rangeCacheKey(leaf)].Value.(rangeCacheEntry)
	require.Greater(t, entry.bytes, int64(len(entry.payload)), "decoded entries and their strings must be charged")
	cache.addVerified(readCacheKey{checksum: "other-data"}, make([]byte, 16000))
	_, cached = cache.getPage(rangeCacheKey(leaf))
	require.False(t, cached)
	require.LessOrEqual(t, cache.bytes, cache.maxBytes)
	// A root already held by a live reader stays usable after its LRU eviction.
	_, err = reader.ReadAt(make([]byte, 1), LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, 2, store.count(leaf.Key))
	require.LessOrEqual(t, cache.bytes, cache.maxBytes)
}

func TestMappingCacheDoesNotRetainOversizedDecodedPages(t *testing.T) {
	store, descriptor, leaf := mappingCacheFixture(t, 16)
	cache, err := NewReadCache(3000)
	require.NoError(t, err)
	reader, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.NoError(t, err)
	_, cached := cache.getPage(rangeCacheKey(leaf))
	require.False(t, cached)
	require.LessOrEqual(t, cache.bytes, cache.maxBytes)
}

func TestMappingCacheRejectsMalformedVerifiedPages(t *testing.T) {
	store := newRangeTestStore()
	object := store.put("maps/malformed", bytes.Repeat([]byte{1}, 64))
	cache, err := NewReadCache(4096)
	require.NoError(t, err)
	for range 2 {
		_, err = NewReaderWithCache(store, testReaderDescriptor(object, 1), cache)
		require.ErrorContains(t, err, "mapping page magic is invalid")
		_, cached := cache.getPage(rangeCacheKey(object))
		require.False(t, cached)
	}
	require.Zero(t, cache.decodes.Load())
}

func TestMappingCacheReinsertsDecodedPageAfterRawEviction(t *testing.T) {
	store, _, leaf := mappingCacheFixture(t, 16)
	key := rangeCacheKey(leaf)
	cache, err := NewReadCache(16384)
	require.NoError(t, err)
	reader := &Reader{source: store, cache: cache}
	payload, err := reader.readRange(leaf)
	require.NoError(t, err)
	page, err := DecodeMappingPage(payload)
	require.NoError(t, err)

	other := readCacheKey{checksum: "other-data"}
	cache.addVerified(other, make([]byte, 16000))
	_, found := cache.get(key)
	require.False(t, found)
	cache.addPage(key, payload, page)
	cached, found := cache.get(key)
	require.True(t, found)
	require.Same(t, &payload[0], &cached[0], "page reinsertion must retain the immutable verified buffer")
	decoded, found := cache.getPage(key)
	require.True(t, found)
	require.Equal(t, page, decoded)
	_, found = cache.get(other)
	require.False(t, found, "decoded page admission must still evict within the shared budget")
	entry := cache.items[key].Value.(rangeCacheEntry)
	require.Greater(t, entry.bytes, rangeCacheBytes(key, payload), "decoded entries must remain charged")
	require.Equal(t, entry.bytes, cache.bytes)
	require.LessOrEqual(t, cache.bytes, cache.maxBytes)
	require.Equal(t, 1, store.count(leaf.Key))
}

func mappingCacheFixture(t testing.TB, blocks int) (*rangeTestStore, Descriptor, ObjectRange) {
	t.Helper()
	store := newRangeTestStore()
	entries := make([]MappingEntry, blocks)
	for block := range blocks {
		object := store.put(fmt.Sprintf("packs/%d", block), bytes.Repeat([]byte{byte(block + 1)}, LogicalBlockSize))
		entries[block] = MappingEntry{LogicalStart: uint64(block), BlockCount: 1, Kind: MappingEntryData, Object: object}
	}
	leafPayload, err := EncodeMappingPage(MappingPage{StartBlock: 0, BlockCount: uint64(blocks), Entries: entries})
	require.NoError(t, err)
	leaf := store.put("maps/leaf", leafPayload)
	rootPayload, err := EncodeMappingPage(MappingPage{
		Level: 1, StartBlock: 0, BlockCount: uint64(blocks),
		Entries: []MappingEntry{{LogicalStart: 0, BlockCount: uint32(blocks), Kind: MappingEntryChild, Object: leaf}},
	})
	require.NoError(t, err)
	root := store.put("maps/root", rootPayload)
	return store, testReaderDescriptor(root, int64(blocks)), leaf
}
