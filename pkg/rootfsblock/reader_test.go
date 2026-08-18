package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestReaderReadsDataZeroesAndNestedPages(t *testing.T) {
	store := newRangeTestStore()
	data := bytes.Repeat([]byte{0x5a}, 2*LogicalBlockSize)
	dataRange := store.put("packs/data", data)
	leafPayload, err := EncodeMappingPage(MappingPage{
		Level: 0, StartBlock: 2, BlockCount: 2,
		Entries: []MappingEntry{{LogicalStart: 2, BlockCount: 2, Kind: MappingEntryData, Object: dataRange}},
	})
	require.NoError(t, err)
	leafRange := store.put("maps/leaf", leafPayload)
	rootPayload, err := EncodeMappingPage(MappingPage{
		Level: 1, StartBlock: 0, BlockCount: 4,
		Entries: []MappingEntry{{LogicalStart: 2, BlockCount: 2, Kind: MappingEntryChild, Object: leafRange}},
	})
	require.NoError(t, err)
	rootRange := store.put("maps/root", rootPayload)
	reader, err := NewReader(store, testReaderDescriptor(rootRange, 4), DefaultReadCacheBytes)
	require.NoError(t, err)

	payload := make([]byte, 4*LogicalBlockSize)
	n, err := reader.ReadAt(payload, 0)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, make([]byte, 2*LogicalBlockSize), payload[:2*LogicalBlockSize])
	require.Equal(t, data, payload[2*LogicalBlockSize:])
}

func TestReaderRejectsCorruptRangeAndWrongChild(t *testing.T) {
	store := newRangeTestStore()
	rootPayload, err := EncodeMappingPage(MappingPage{StartBlock: 0, BlockCount: 1})
	require.NoError(t, err)
	rootRange := store.put("maps/root", rootPayload)
	rootRange.Checksum = digest.FromString("wrong").String()
	_, err = NewReader(store, testReaderDescriptor(rootRange, 1), DefaultReadCacheBytes)
	require.ErrorContains(t, err, "checksum mismatch")

	leafPayload, err := EncodeMappingPage(MappingPage{StartBlock: 1, BlockCount: 1})
	require.NoError(t, err)
	leafRange := store.put("maps/leaf", leafPayload)
	rootPayload, err = EncodeMappingPage(MappingPage{
		Level: 1, StartBlock: 0, BlockCount: 1,
		Entries: []MappingEntry{{LogicalStart: 0, BlockCount: 1, Kind: MappingEntryChild, Object: leafRange}},
	})
	require.NoError(t, err)
	rootRange = store.put("maps/root-2", rootPayload)
	reader, err := NewReader(store, testReaderDescriptor(rootRange, 1), DefaultReadCacheBytes)
	require.NoError(t, err)
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.ErrorContains(t, err, "does not match")
}

func TestReaderCachesAndSingleflightsImmutableRanges(t *testing.T) {
	store := newRangeTestStore()
	data := bytes.Repeat([]byte{0x7b}, LogicalBlockSize)
	dataRange := store.put("packs/data", data)
	rootPayload, err := EncodeMappingPage(MappingPage{
		StartBlock: 0, BlockCount: 1,
		Entries: []MappingEntry{{LogicalStart: 0, BlockCount: 1, Kind: MappingEntryData, Object: dataRange}},
	})
	require.NoError(t, err)
	rootRange := store.put("maps/root", rootPayload)
	reader, err := NewReader(store, testReaderDescriptor(rootRange, 1), DefaultReadCacheBytes)
	require.NoError(t, err)

	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			payload := make([]byte, LogicalBlockSize)
			_, readErr := reader.ReadAt(payload, 0)
			require.NoError(t, readErr)
			require.Equal(t, data, payload)
		}()
	}
	wait.Wait()
	require.Equal(t, 1, store.count("packs/data"))
}

func TestReadersShareVerifiedImmutableRanges(t *testing.T) {
	store := newRangeTestStore()
	data := bytes.Repeat([]byte{0x6c}, LogicalBlockSize)
	dataRange := store.put("packs/shared-data", data)
	rootPayload, err := EncodeMappingPage(MappingPage{
		StartBlock: 0, BlockCount: 1,
		Entries: []MappingEntry{{LogicalStart: 0, BlockCount: 1, Kind: MappingEntryData, Object: dataRange}},
	})
	require.NoError(t, err)
	rootRange := store.put("maps/shared-root", rootPayload)
	cache, err := NewReadCache(DefaultReadCacheBytes)
	require.NoError(t, err)

	readers := make([]*Reader, 32)
	for i := range readers {
		readers[i], err = NewReaderWithCache(store, testReaderDescriptor(rootRange, 1), cache)
		require.NoError(t, err)
	}
	var wait sync.WaitGroup
	for _, reader := range readers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			payload := make([]byte, LogicalBlockSize)
			_, readErr := reader.ReadAt(payload, 0)
			require.NoError(t, readErr)
			require.Equal(t, data, payload)
		}()
	}
	wait.Wait()
	require.Equal(t, 1, store.count("maps/shared-root"))
	require.Equal(t, 1, store.count("packs/shared-data"))
}

func TestReadCacheRejectsInvalidConfiguration(t *testing.T) {
	_, err := NewReadCache(-1)
	require.ErrorContains(t, err, "non-negative")

	store := newRangeTestStore()
	rootPayload, err := EncodeMappingPage(MappingPage{StartBlock: 0, BlockCount: 1})
	require.NoError(t, err)
	rootRange := store.put("maps/root", rootPayload)
	_, err = NewReaderWithCache(store, testReaderDescriptor(rootRange, 1), nil)
	require.ErrorContains(t, err, "read cache is required")
}

func TestReaderReturnsPartialEOFAndReplaysCompositeTail(t *testing.T) {
	store := newRangeTestStore()
	rootPayload, err := EncodeMappingPage(MappingPage{StartBlock: 0, BlockCount: 1})
	require.NoError(t, err)
	rootRange := store.put("maps/root", rootPayload)
	descriptor := testReaderDescriptor(rootRange, 1)
	reader, err := NewReader(store, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	n, err := reader.ReadAt(make([]byte, 2), LogicalBlockSize-1)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, n)

	tail, err := EncodeCompositeTail([]BlockUpdate{{
		Block: 0, Data: bytes.Repeat([]byte{0x44}, LogicalBlockSize),
	}}, 1)
	require.NoError(t, err)
	descriptor.CompositeTail = &tail
	reader, err = NewReader(store, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, LogicalBlockSize)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x44}, LogicalBlockSize), actual)
}

func testReaderDescriptor(root ObjectRange, blocks int64) Descriptor {
	return Descriptor{
		Version: DescriptorVersion, LogicalSizeBytes: blocks * LogicalBlockSize, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{Version: MappingPageVersion, RootDigest: root.Checksum, Object: root},
	}
}

type rangeTestStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	reads   map[string]int
}

func newRangeTestStore() *rangeTestStore {
	return &rangeTestStore{objects: make(map[string][]byte), reads: make(map[string]int)}
}

func (s *rangeTestStore) put(key string, payload []byte) ObjectRange {
	s.objects[key] = append([]byte(nil), payload...)
	return ObjectRange{Key: key, Length: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
}

func (s *rangeTestStore) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.objects[key]
	if !ok || offset < 0 || length < 0 || offset+length > int64(len(payload)) {
		return nil, fmt.Errorf("range not found")
	}
	s.reads[key]++
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func (s *rangeTestStore) count(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[key]
}
