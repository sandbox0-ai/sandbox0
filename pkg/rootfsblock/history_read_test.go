package rootfsblock

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type historyReadSource struct {
	*memoryObjects
	dataKeys  map[string]bool
	mapKeys   map[string]bool
	dataGets  int
	mapGets   int
	dataBytes int64
	mapBytes  int64
}

func (s *historyReadSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if s.dataKeys[key] {
		s.dataGets++
		s.dataBytes += length
	} else if s.mapKeys[key] {
		s.mapGets++
		s.mapBytes += length
	} else {
		return nil, fmt.Errorf("unclassified history fixture object %q", key)
	}
	return s.memoryObjects.Get(key, offset, length)
}

func (s *historyReadSource) remember(result BuildResult) {
	for _, ref := range result.References {
		if ref.Kind == ObjectKindDataPack {
			s.dataKeys[ref.Key] = true
		} else {
			s.mapKeys[ref.Key] = true
		}
	}
}

func (s *historyReadSource) reset() {
	s.dataGets, s.mapGets, s.dataBytes, s.mapBytes = 0, 0, 0, 0
}

// This is a fully backed 64 MiB block-map fixture with 256 sequential writable
// generations, not a populated XFS image or a claim/command latency benchmark.
// Distinct 64 KiB ranges contain repeated random 16 KiB quarters to exercise
// compressed views without deduplicating all extents to one artificial object.
// Keep history count separate from changes to the current demand's working set.
func TestHistoryReadUsesCurrentRootWithoutAncestryReplay(t *testing.T) {
	const imageBytes = 64 << 20
	const demandBytes = 128 << 10
	const generations = 256
	for _, pattern := range []string{"same-block", "fragmented-demand", "outside-demand"} {
		t.Run(pattern, func(t *testing.T) {
			source := &historyReadSource{memoryObjects: newMemoryObjects(), dataKeys: make(map[string]bool), mapKeys: make(map[string]bool)}
			expected := make([]byte, imageBytes)
			random := rand.New(rand.NewSource(711))
			quarter := make([]byte, CompressedDataRangeBytes/4)
			for start := 0; start < len(expected); start += CompressedDataRangeBytes {
				_, err := random.Read(quarter)
				require.NoError(t, err)
				for part := range 4 {
					copy(expected[start+part*len(quarter):], quarter)
				}
			}
			options := BuildOptions{FormatVersion: CompressedFormatVersion}
			built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(expected), imageBytes, source, options)
			require.NoError(t, err)
			source.remember(built)
			for generation := 0; generation <= generations; generation++ {
				if generation > 0 {
					block := uint64(1)
					switch pattern {
					case "fragmented-demand":
						block = uint64(1 + 2*((generation-1)%16))
					case "outside-demand":
						unit := (generation - 1) % ((imageBytes - demandBytes) / CompressedDataRangeBytes)
						block = uint64(demandBytes/LogicalBlockSize + unit*(CompressedDataRangeBytes/LogicalBlockSize))
					}
					data := make([]byte, LogicalBlockSize)
					_, err := random.Read(data)
					require.NoError(t, err)
					updates := []BlockUpdate{{Block: block, Data: data}}
					// Both durable publication routes must yield a complete current
					// root, not add a read-time dependency on every prior generation.
					if generation%2 == 0 {
						composite, _, err := BuildCompositeGeneration(built.Descriptor, updates)
						require.NoError(t, err)
						batch, err := BuildIncrementalGenerationsBatch(t.Context(), source,
							[]BatchIncrementalInput{{ID: "history", Descriptor: composite}}, source, options)
						require.NoError(t, err)
						built = batch.Results["history"]
					} else {
						built, err = BuildIncrementalGeneration(t.Context(), source, built.Descriptor, updates, source, options)
						require.NoError(t, err)
					}
					source.remember(built)
					copy(expected[int(block)*LogicalBlockSize:], data)
				}
				if generation != 0 && generation != 1 && generation != 16 && generation != 64 && generation != generations {
					continue
				}
				for _, cacheBytes := range []int64{0, DefaultReadCacheBytes} {
					t.Run(fmt.Sprintf("generation-%d-cache-%d", generation, cacheBytes), func(t *testing.T) {
						cache, err := NewReadCache(cacheBytes)
						require.NoError(t, err)
						for identity := range 2 {
							source.reset()
							reader, err := NewReaderWithCache(source, built.Descriptor, cache)
							require.NoError(t, err)
							openGets := source.mapGets
							if generation > 0 {
								require.Positive(t, reader.root.Level, "history must exercise a split, multilevel mapping tree")
							}
							require.Zero(t, source.dataGets, "opening a current root must not fetch payload or ancestors")
							if identity == 0 || cacheBytes == 0 {
								require.Equal(t, 1, openGets, "one current root, independent of generation count")
							} else {
								require.Zero(t, openGets, "new Reader reuses verified immutable mapping cache")
							}
							source.reset()
							actual := make([]byte, demandBytes)
							n, err := reader.ReadAt(actual, 0)
							require.NoError(t, err)
							require.Equal(t, len(actual), n)
							require.Equal(t, expected[:demandBytes], actual)
							if identity == 1 && cacheBytes > 0 {
								require.Zero(t, source.dataGets, "second identity must reuse the demanded immutable data")
								require.Zero(t, source.mapGets)
							}
							if identity == 0 && cacheBytes > 0 {
								require.LessOrEqual(t, source.mapGets, 2, "only current demand's leaf paths, not all historical roots")
								require.LessOrEqual(t, source.dataGets, 18, "at most sixteen final edits and two unchanged range objects, independent of history count")
								if pattern == "outside-demand" {
									require.Equal(t, 1, source.dataGets, "unrelated history must not add demanded payload requests")
								}
							}
							t.Logf("history=%d pattern=%s cache_bytes=%d identity=%d root_level=%d open_mapping_gets=%d demand_bytes=%d mapping_gets=%d mapping_bytes=%d data_gets=%d data_bytes=%d",
								generation, pattern, cacheBytes, identity, reader.root.Level, openGets, demandBytes,
								source.mapGets, source.mapBytes, source.dataGets, source.dataBytes)
						}
					})
				}
				// Verify every current block separately from the measured prefix.
				// This reader/cache cannot warm any of the next checkpoint's caches.
				whole, err := NewReader(source, built.Descriptor, DefaultReadCacheBytes)
				require.NoError(t, err)
				actual := make([]byte, len(expected))
				n, err := whole.ReadAt(actual, 0)
				require.NoError(t, err)
				require.Equal(t, len(actual), n)
				require.Equal(t, sha256.Sum256(expected), sha256.Sum256(actual), "complete generation %d", generation)
			}
		})
	}
}
