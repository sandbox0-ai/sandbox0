package rootfsblock

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

const (
	adaptiveRangeBytes   = 64 << 10
	adaptiveWindowRanges = coalescedReadBytes / adaptiveRangeBytes
)

type adaptiveGet struct{ offset, length int64 }
type adaptiveSource struct {
	RangeSource
	pack      string
	mu        sync.Mutex
	reads     []adaptiveGet
	bulkError error
}

func (s *adaptiveSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if key == s.pack {
		s.mu.Lock()
		s.reads = append(s.reads, adaptiveGet{offset, length})
		s.mu.Unlock()
		if length > adaptiveRangeBytes && s.bulkError != nil {
			return nil, s.bulkError
		}
	}
	return s.RangeSource.Get(key, offset, length)
}
func (s *adaptiveSource) requests() []adaptiveGet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]adaptiveGet(nil), s.reads...)
}

func adaptiveFixture(t testing.TB, size, pageEntries int) (*adaptiveSource, *buildTestStore, Descriptor, []byte) {
	t.Helper()
	payload := make([]byte, size)
	// Exercise Format2's incompressible raw-range path with stable bytes.
	_, _ = rand.New(rand.NewSource(7)).Read(payload)
	// Cache and flight identities use verified content, not physical offsets.
	// Keep ranges unique beyond 256 entries so wide fixtures really exercise
	// independent source loads instead of accidentally deduplicating them.
	for offset := 0; offset+8 <= len(payload); offset += adaptiveRangeBytes {
		binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(offset/adaptiveRangeBytes+1))
	}
	store := newBuildTestStore()
	built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(size), store,
		BuildOptions{DataRangeBytes: adaptiveRangeBytes, PageEntries: pageEntries})
	require.NoError(t, err)
	var pack string
	for _, ref := range built.References {
		if ref.Kind == ObjectKindDataPack {
			require.Empty(t, pack)
			pack = ref.Key
		}
	}
	require.NotEmpty(t, pack)
	return &adaptiveSource{RangeSource: store, pack: pack}, store, built.Descriptor, payload
}

func TestAdaptiveFixtureKeepsDistantRangesDistinct(t *testing.T) {
	const distantOffset = 256 * adaptiveRangeBytes
	source, _, descriptor, expected := adaptiveFixture(t, distantOffset+adaptiveRangeBytes, 1024)
	require.NotEqual(t, expected[:adaptiveRangeBytes], expected[distantOffset:])
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	for _, offset := range []int64{0, distantOffset} {
		actual := make([]byte, 512)
		_, err := reader.ReadAt(actual, offset)
		require.NoError(t, err)
		require.Equal(t, expected[offset:offset+512], actual)
	}
	require.Equal(t, []adaptiveGet{{0, adaptiveRangeBytes}, {distantOffset, adaptiveRangeBytes}}, source.requests())
}

func TestAdaptiveReadSmallDemandDoesNotReadAhead(t *testing.T) {
	source, _, descriptor, expected := adaptiveFixture(t, 2*coalescedReadBytes, 1024)
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, 512)
	for _, offset := range []int64{4096, coalescedReadBytes + 4096} {
		_, err = reader.ReadAt(actual, offset)
		require.NoError(t, err)
		require.Equal(t, expected[offset:offset+512], actual)
	}
	require.Equal(t, []adaptiveGet{{0, adaptiveRangeBytes}, {coalescedReadBytes, adaptiveRangeBytes}}, source.requests())
}

func TestAdaptiveReadBulkWindowsKeepSourceRequestsBounded(t *testing.T) {
	for _, cacheBytes := range []int64{0, 1 << 20, DefaultReadCacheBytes} {
		t.Run(fmt.Sprint(cacheBytes), func(t *testing.T) {
			source, _, descriptor, expected := adaptiveFixture(t, 2*coalescedReadBytes, 1024)
			reader, err := NewReader(source, descriptor, cacheBytes)
			require.NoError(t, err)
			actual := make([]byte, bulkReadThreshold)
			for offset := 0; offset < len(expected); offset += len(actual) {
				_, err = reader.ReadAt(actual, int64(offset))
				require.NoError(t, err)
				require.Equal(t, expected[offset:offset+len(actual)], actual)
			}
			reads := source.requests()
			if cacheBytes < 2*coalescedReadBytes {
				require.Len(t, reads, len(expected)/bulkReadThreshold)
				for _, read := range reads {
					require.EqualValues(t, bulkReadThreshold, read.length)
				}
			} else {
				require.Equal(t, []adaptiveGet{{0, coalescedReadBytes}, {coalescedReadBytes, coalescedReadBytes}}, reads)
			}
			require.LessOrEqual(t, reader.cache.bytes, cacheBytes)
		})
	}
}

func TestAdaptiveReadRetainsBulkSpanWithoutCache(t *testing.T) {
	source, _, descriptor, expected := adaptiveFixture(t, 2*coalescedReadBytes, 1024)
	reader, err := NewReader(source, descriptor, 0)
	require.NoError(t, err)
	actual := make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, []adaptiveGet{{0, coalescedReadBytes}, {coalescedReadBytes, coalescedReadBytes}}, source.requests())
}

func TestAdaptiveReadDoesNotSpeculateAcrossMappingLeaves(t *testing.T) {
	source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 2)
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, bulkReadThreshold)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected[:len(actual)], actual)
	require.Equal(t, []adaptiveGet{{0, bulkReadThreshold}}, source.requests())
}

func TestAdaptiveReadCorruptionNeverEscapesAndDoesNotPoisonHealthyNeighbors(t *testing.T) {
	for _, corruptIndex := range []int{0, 2, adaptiveWindowRanges - 1} {
		t.Run(fmt.Sprint(corruptIndex), func(t *testing.T) {
			source, store, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
			store.objects[source.pack][corruptIndex*adaptiveRangeBytes] ^= 1
			reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			// Demand starts after the optional corrupt prefix; the suffix is
			// speculative. Neither may prevent a healthy demanded read.
			actual := bytes.Repeat([]byte{0xee}, bulkReadThreshold)
			if corruptIndex != 2 {
				n, err := reader.ReadAt(actual, adaptiveRangeBytes)
				require.NoError(t, err)
				require.Equal(t, len(actual), n)
				require.Equal(t, expected[adaptiveRangeBytes:adaptiveRangeBytes+len(actual)], actual)
			} else {
				n, err := reader.ReadAt(actual, adaptiveRangeBytes)
				require.ErrorContains(t, err, "checksum mismatch")
				require.Equal(t, adaptiveRangeBytes, n)
				require.Equal(t, expected[adaptiveRangeBytes:2*adaptiveRangeBytes], actual[:n])
				require.Equal(t, bytes.Repeat([]byte{0xee}, adaptiveRangeBytes), actual[n:])
			}
			bad, _, found, err := reader.resolve(reader.root, uint64(corruptIndex*adaptiveRangeBytes/LogicalBlockSize))
			require.NoError(t, err)
			require.True(t, found)
			_, cached := reader.cache.get(rangeCacheKey(bad.Object))
			require.False(t, cached)
			require.LessOrEqual(t, reader.cache.bytes, reader.cache.maxBytes)
		})
	}
}

func TestAdaptiveReadSourceFailureFallsBackToExactDemand(t *testing.T) {
	source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
	source.bulkError = errors.New("speculative range unavailable")
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, bulkReadThreshold)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected[:len(actual)], actual)
	require.Equal(t, []adaptiveGet{{0, coalescedReadBytes}, {0, adaptiveRangeBytes}, {adaptiveRangeBytes, adaptiveRangeBytes}}, source.requests())
}

func TestAdaptiveReadCompositeTailOverridesCoalescedBytes(t *testing.T) {
	source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
	dirty := bytes.Repeat([]byte{0xe9}, LogicalBlockSize)
	tail, err := EncodeCompositeTail([]BlockUpdate{{Block: 19, Data: dirty}}, uint64(len(expected)/LogicalBlockSize))
	require.NoError(t, err)
	descriptor.CompositeTail = &tail
	copy(expected[19*LogicalBlockSize:], dirty)
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, []adaptiveGet{{0, coalescedReadBytes}}, source.requests())
}

func TestAdaptiveReadConcurrentBulkMissesShareOneSourceSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
		source := &readerAdmissionSource{RangeSource: base, pack: base.pack, started: make(chan int64, 64), finish: make(chan struct{})}
		reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		var readers sync.WaitGroup
		t.Cleanup(func() { close(source.finish); readers.Wait() })
		for index := range 64 {
			readers.Add(1)
			go func() {
				defer readers.Done()
				offset := (index % (coalescedReadBytes / bulkReadThreshold)) * bulkReadThreshold
				actual := make([]byte, bulkReadThreshold)
				_, err := reader.ReadAt(actual, int64(offset))
				require.NoError(t, err)
				require.Equal(t, expected[offset:offset+len(actual)], actual)
			}()
		}
		synctest.Wait()
		require.Len(t, source.started, 1, "one source flight despite more waiters than admission slots")
		source.finish <- struct{}{}
		readers.Wait()
		require.Equal(t, []adaptiveGet{{0, coalescedReadBytes}}, base.requests())
	})
}

func TestAdaptiveReadIndependentWindowsRespectSourceAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const windows = maxConcurrentSourceReads + 1
		base, _, descriptor, expected := adaptiveFixture(t, windows*coalescedReadBytes, 1024)
		source := &readerAdmissionSource{RangeSource: base, pack: base.pack, started: make(chan int64, windows), finish: make(chan struct{}, windows)}
		reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		var readers sync.WaitGroup
		t.Cleanup(func() { close(source.finish); readers.Wait() })
		for index := range windows {
			readers.Add(1)
			go func() {
				defer readers.Done()
				offset := index * coalescedReadBytes
				actual := make([]byte, bulkReadThreshold)
				_, err := reader.ReadAt(actual, int64(offset))
				require.NoError(t, err)
				require.Equal(t, expected[offset:offset+len(actual)], actual)
			}()
		}
		synctest.Wait()
		require.Len(t, source.started, maxConcurrentSourceReads)
		for range maxConcurrentSourceReads {
			<-source.started
		}
		source.finish <- struct{}{}
		synctest.Wait()
		require.Len(t, source.started, 1)
		for range maxConcurrentSourceReads {
			source.finish <- struct{}{}
		}
		readers.Wait()
		require.Len(t, base.requests(), windows)
	})
}

// This measures bytes and GETs, not latency. Both layers must be fine-grained:
// small mapping entries alone still fetch an entire legacy encrypted frame.
func TestAdaptiveReadEncryptedFrameGeometry(t *testing.T) {
	for _, frameBytes := range []int64{1 << 20, 64 << 10} {
		for _, bulk := range []bool{false, true} {
			t.Run(fmt.Sprintf("frame-%d/bulk-%t", frameBytes, bulk), func(t *testing.T) {
				_, plain, descriptor, expected := adaptiveFixture(t, 2*coalescedReadBytes, 1024)
				base := &diagnosticCountingStore{Store: objectstore.NewMemoryStore(t.Name())}
				source := objectstore.EncryptingImmutable(base, objectstore.EncryptionConfig{Enabled: true, KeyEncryptor: diagnosticKeyWrapper{}, ChunkSize: frameBytes}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 16, MaxBytes: 1 << 20})
				for key, payload := range plain.objects {
					require.NoError(t, source.Put(key, bytes.NewReader(payload)))
				}
				reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
				require.NoError(t, err)
				base.calls, base.bytes = 0, 0
				var dataBytes int64
				if bulk {
					for offset := 0; offset < len(expected); offset += bulkReadThreshold {
						actual := make([]byte, bulkReadThreshold)
						_, err := reader.ReadAt(actual, int64(offset))
						require.NoError(t, err)
						require.Equal(t, expected[offset:offset+len(actual)], actual)
					}
					dataBytes = int64(len(expected))
				} else {
					for _, offset := range []int64{0, coalescedReadBytes} {
						actual := make([]byte, 512)
						_, err := reader.ReadAt(actual, offset)
						require.NoError(t, err)
						require.Equal(t, expected[offset:offset+512], actual)
					}
					dataBytes = 2 * frameBytes
				}
				require.Equal(t, 3, base.calls, "one header GET and two data GETs for either workload")
				require.GreaterOrEqual(t, base.bytes, dataBytes)
				require.Less(t, base.bytes, dataBytes+4096)
				t.Logf("data_bytes=%d fetched_ciphertext_bytes=%d gets=%d", dataBytes, base.bytes, base.calls)
			})
		}
	}
}

func TestAdaptiveReadPlanStopsAtHolesAndPhysicalBoundaries(t *testing.T) {
	for _, boundary := range []string{"hole", "object", "offset", "coarse"} {
		t.Run(boundary, func(t *testing.T) {
			source, _, descriptor, _ := adaptiveFixture(t, coalescedReadBytes, 1024)
			reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			leaf := reader.root
			leaf.Entries = append([]MappingEntry(nil), leaf.Entries...)
			switch boundary {
			case "hole":
				leaf.Entries = append(leaf.Entries[:2], leaf.Entries[3:]...)
			case "object":
				leaf.Entries[2].Object.Key = "another-pack"
			case "offset":
				leaf.Entries[2].Object.Offset++
			case "coarse":
				leaf.Entries[2].Object.Length = bulkReadThreshold
				leaf.Entries[2].BlockCount *= 2
				leaf.Entries = append(leaf.Entries[:3], leaf.Entries[4:]...)
			}
			require.NoError(t, leaf.Validate())
			entries := reader.coalescedEntries(leaf.Entries[0], leaf, 0, bulkReadThreshold)
			require.Len(t, entries, 2)
		})
	}
}

func TestAdaptiveReadCacheOwnsOnlyEachVerifiedFragment(t *testing.T) {
	source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, bulkReadThreshold)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	clear(actual)
	for _, entry := range reader.root.Entries {
		payload, ok := reader.cache.get(rangeCacheKey(entry.Object))
		require.True(t, ok)
		require.Less(t, cap(payload), 2*adaptiveRangeBytes, "no hidden retained bulk array")
		require.Equal(t, entry.Object.Checksum, digest.FromBytes(payload).String())
	}
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected[:len(actual)], actual)
}

func TestAdaptiveReadUnalignedBulkHolesAndEOF(t *testing.T) {
	const size = 4 * coalescedReadBytes
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*17 + i/adaptiveRangeBytes + 1)
	}
	clear(payload[3*adaptiveRangeBytes : 5*adaptiveRangeBytes])
	clear(payload[2*coalescedReadBytes : 2*coalescedReadBytes+adaptiveRangeBytes])
	for _, pageEntries := range []int{2, 1024} {
		store := newBuildTestStore()
		built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), size, store,
			BuildOptions{DataRangeBytes: adaptiveRangeBytes, PageEntries: pageEntries})
		require.NoError(t, err)
		for _, cacheBytes := range []int64{0, 1 << 20, DefaultReadCacheBytes} {
			for _, offset := range []int64{0, 1, adaptiveRangeBytes - 1, 3*adaptiveRangeBytes - 7,
				coalescedReadBytes - 17, 2*coalescedReadBytes - 4097, 3*coalescedReadBytes + 13, size - 1023, size, size + 1} {
				for _, length := range []int{512, bulkReadThreshold - 1, bulkReadThreshold, coalescedReadBytes + 37} {
					t.Run(fmt.Sprintf("page%d/cache%d/offset%d/length%d", pageEntries, cacheBytes, offset, length), func(t *testing.T) {
						reader, err := NewReader(store, built.Descriptor, cacheBytes)
						require.NoError(t, err)
						actual := bytes.Repeat([]byte{0xee}, length)
						expected := bytes.Clone(actual)
						wantN, wantErr := bytes.NewReader(payload).ReadAt(expected, offset)
						n, err := reader.ReadAt(actual, offset)
						require.Equal(t, wantN, n)
						require.ErrorIs(t, err, wantErr)
						require.Equal(t, expected, actual)
						require.LessOrEqual(t, reader.cache.bytes, cacheBytes)
					})
				}
			}
		}
	}
}
