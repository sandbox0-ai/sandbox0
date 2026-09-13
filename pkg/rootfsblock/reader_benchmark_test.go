package rootfsblock

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Models the repeated 4 KiB reads issued by Branch against a nested mapping
// page after object bytes are cached. It excludes network and cold-page I/O.
func BenchmarkReaderNestedMappingWarm4KiB(b *testing.B) {
	store := newRangeTestStore()
	data := store.put("packs/shared", bytes.Repeat([]byte{0x53}, LogicalBlockSize))
	entries := make([]MappingEntry, 1024)
	for index := range entries {
		entries[index] = MappingEntry{LogicalStart: uint64(index), BlockCount: 1, Kind: MappingEntryData, Object: data}
	}
	leafPayload, err := EncodeMappingPage(MappingPage{StartBlock: 0, BlockCount: 1024, Entries: entries})
	require.NoError(b, err)
	leaf := store.put("maps/leaf", leafPayload)
	rootPayload, err := EncodeMappingPage(MappingPage{
		Level: 1, StartBlock: 0, BlockCount: 1024,
		Entries: []MappingEntry{{LogicalStart: 0, BlockCount: 1024, Kind: MappingEntryChild, Object: leaf}},
	})
	require.NoError(b, err)
	root := store.put("maps/root", rootPayload)
	reader, err := NewReader(store, testReaderDescriptor(root, 1024), DefaultReadCacheBytes)
	require.NoError(b, err)
	payload := make([]byte, LogicalBlockSize)
	_, err = reader.ReadAt(payload, 0)
	require.NoError(b, err)
	b.ReportAllocs()
	b.SetBytes(LogicalBlockSize)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := reader.ReadAt(payload, int64(index%1024)*LogicalBlockSize)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Measures cold verified-range buffering and cache admission, not object-store
// latency or end-to-end startup. Every operation gets a new empty read cache.
func BenchmarkReaderColdVerifiedRange8MiB(b *testing.B) {
	for _, cacheBytes := range []int64{0, DefaultReadCacheBytes} {
		name := "cache-disabled"
		if cacheBytes != 0 {
			name = "cache-enabled"
		}
		b.Run(name, func(b *testing.B) {
			store := newRangeTestStore()
			object := store.put("packs/cold-range", bytes.Repeat([]byte{0x73}, DefaultDataRangeBytes))
			b.ReportAllocs()
			b.SetBytes(object.Length)
			b.ResetTimer()
			for range b.N {
				cache, err := NewReadCache(cacheBytes)
				if err != nil {
					b.Fatal(err)
				}
				reader := &Reader{source: store, cache: cache}
				payload, err := reader.readRange(object)
				if err != nil || int64(len(payload)) != object.Length {
					b.Fatalf("read verified range: length=%d, err=%v", len(payload), err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(store.count(object.Key))/float64(b.N), "gets/op")
		})
	}
}
