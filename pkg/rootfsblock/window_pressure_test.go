package rootfsblock

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// These fixtures measure decoded coverage and request counts, not S3 latency.
// Every range has distinct content, including across different generations.
// Highly compressible payloads keep this cache-pressure test inexpensive.
func windowPressurePayload(owner int, target []byte, offset int64) {
	for len(target) > 0 {
		block := offset / CompressedDataRangeBytes
		within := int(offset % CompressedDataRangeBytes)
		n := min(len(target), CompressedDataRangeBytes-within)
		for i := range n {
			target[i] = byte(block + 1)
		}
		if within < 8 {
			var identity [8]byte
			binary.LittleEndian.PutUint64(identity[:], uint64(owner+1)<<32|uint64(block+1))
			copy(target[:n], identity[within:])
		}
		target = target[n:]
		offset += int64(n)
	}
}

type windowPressureCounts struct {
	Requests      int   `json:"data_requests"`
	EncodedBytes  int64 `json:"encoded_source_bytes"`
	DecodedBytes  int64 `json:"source_covered_decoded_bytes"`
	LargestRead   int64 `json:"largest_encoded_read_bytes"`
	LargestWindow int64 `json:"largest_decoded_coverage_bytes"`
}

type windowPressureSource struct {
	RangeSource
	mu     sync.Mutex
	ranges map[string][]ObjectRange
	counts windowPressureCounts
}

func (s *windowPressureSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	if ranges, found := s.ranges[key]; found {
		var covered int64
		for _, object := range ranges {
			if object.Offset >= offset && object.Offset+object.StoredLength() <= offset+length {
				covered += object.Length
			}
		}
		s.counts.Requests++
		s.counts.EncodedBytes += length
		s.counts.DecodedBytes += covered
		s.counts.LargestRead = max(s.counts.LargestRead, length)
		s.counts.LargestWindow = max(s.counts.LargestWindow, covered)
	}
	s.mu.Unlock()
	return s.RangeSource.Get(key, offset, length)
}

func TestCoalescedWindowInterleavedCachePressure(t *testing.T) {
	const owners, imageBytes = 18, 16 << 20
	store := newBuildTestStore()
	descriptors := make([]Descriptor, owners)
	for owner := range owners {
		payload := make([]byte, imageBytes)
		windowPressurePayload(owner, payload, 0)
		built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), imageBytes, store,
			BuildOptions{FormatVersion: CompressedFormatVersion, PageEntries: 1024})
		require.NoError(t, err)
		descriptors[owner] = built.Descriptor
	}
	for _, test := range []struct {
		name                  string
		readers, read, stride int
	}{
		{"dense-single", 1, bulkReadThreshold, bulkReadThreshold},
		{"small-interleaved", owners, 512, 2 << 20},
		{"sparse-bulk-interleaved", owners, bulkReadThreshold, 2 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache, err := NewReadCache(DefaultReadCacheBytes)
			require.NoError(t, err)
			source := &windowPressureSource{RangeSource: store, ranges: make(map[string][]ObjectRange)}
			var previous windowPressureCounts
			for round := range 2 {
				// Recreate generation readers while retaining only the node cache.
				// Round-robin demand is deterministic interleaving, not a claim or
				// concurrent runtime benchmark.
				readers := make([]*Reader, test.readers)
				for owner := range readers {
					readers[owner], err = NewReaderWithCache(source, descriptors[owner], cache)
					require.NoError(t, err)
					if round == 0 {
						for _, entry := range readers[owner].root.Entries {
							source.ranges[entry.Object.Key] = append(source.ranges[entry.Object.Key], entry.Object)
						}
					}
				}
				actual, expected := make([]byte, test.read), make([]byte, test.read)
				var demanded int64
				for offset := 0; offset < imageBytes; offset += test.stride {
					for owner, reader := range readers {
						n, err := reader.ReadAt(actual, int64(offset))
						require.NoError(t, err)
						require.Equal(t, len(actual), n)
						windowPressurePayload(owner, expected, int64(offset))
						require.Equal(t, expected, actual)
						demanded += int64(n)
						require.LessOrEqual(t, cache.bytes, DefaultReadCacheBytes)
					}
				}
				current := source.counts
				delta := current
				delta.Requests -= previous.Requests
				delta.EncodedBytes -= previous.EncodedBytes
				delta.DecodedBytes -= previous.DecodedBytes
				if test.name == "small-interleaved" {
					require.LessOrEqual(t, current.LargestWindow, int64(CompressedDataRangeBytes), "small demand must not expand to a bulk window")
				}
				if round == 1 && test.name != "sparse-bulk-interleaved" {
					require.Zero(t, delta.Requests, "a fitting verified working set must remain reusable by new readers")
				}
				require.LessOrEqual(t, current.LargestWindow, int64(coalescedReadBytes))
				require.LessOrEqual(t, current.LargestRead, int64(coalescedReadBytes))
				encoded, err := json.Marshal(struct {
					Pattern     string               `json:"pattern"`
					Round       int                  `json:"round"`
					Readers     int                  `json:"readers"`
					WindowBytes int                  `json:"window_bytes"`
					DemandBytes int64                `json:"demand_bytes"`
					CacheBytes  int64                `json:"cache_used_bytes"`
					Counts      windowPressureCounts `json:"counts"`
				}{test.name, round, test.readers, coalescedReadBytes, demanded, cache.bytes, delta})
				require.NoError(t, err)
				t.Log(string(encoded))
				previous = current
			}
		})
	}
}
