package rootfsblock

import (
	"bytes"
	"context"
	"encoding/binary"
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

func compressedFixture(t *testing.T, pages int) (*buildTestStore, BuildResult, []byte, string) {
	t.Helper()
	payload := make([]byte, coalescedReadBytes)
	for index := range len(payload) / CompressedDataRangeBytes {
		copy(payload[index*CompressedDataRangeBytes:], bytes.Repeat([]byte{byte(index + 1)}, CompressedDataRangeBytes))
	}
	// One incompressible unit exercises mixed raw/encoded coalesced windows.
	_, err := rand.New(rand.NewSource(19)).Read(payload[5*CompressedDataRangeBytes : 6*CompressedDataRangeBytes])
	require.NoError(t, err)
	store := newBuildTestStore()
	built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), store,
		BuildOptions{FormatVersion: CompressedFormatVersion, PageEntries: pages})
	require.NoError(t, err)
	var pack string
	for _, ref := range built.References {
		stored, found := store.payload(ref.Key)
		require.True(t, found)
		require.Equal(t, int64(len(stored)), ref.Size)
		require.Equal(t, digest.FromBytes(stored).String(), ref.Checksum)
		if ref.Kind == ObjectKindDataPack {
			require.Empty(t, pack)
			pack = ref.Key
		}
	}
	require.NotEmpty(t, pack)
	require.Less(t, built.Bytes, int64(len(payload)/4))
	return store, built, payload, pack
}

func TestCompressedReadersShareNodeSourceAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const width = 64
		store := newRangeTestStore()
		cache, err := NewReadCache(0)
		require.NoError(t, err)
		source := &sharedAdmissionSource{RangeSource: store, started: make(chan int64, width), finish: make(chan struct{})}
		readers := make([]*Reader, width)
		for index := range width {
			encoded, object, err := encodeRangePayload(t.Context(), bytes.Repeat([]byte{byte(index + 1)}, CompressedDataRangeBytes))
			require.NoError(t, err)
			object.Key = store.put(fmt.Sprintf("packs/shared-admission/%d", index), encoded).Key
			payload, err := EncodeMappingPage(MappingPage{Version: CompressedFormatVersion, BlockCount: 16,
				Entries: []MappingEntry{{Kind: MappingEntryData, BlockCount: 16, Object: object}}})
			require.NoError(t, err)
			root := store.put(fmt.Sprintf("maps/compressed-admission/%d", index), payload)
			descriptor := testReaderDescriptor(root, 16)
			descriptor.Version, descriptor.MappingRoot.Version = CompressedFormatVersion, CompressedFormatVersion
			readers[index], err = NewReaderWithCache(source, descriptor, cache)
			require.NoError(t, err)
		}
		var wg sync.WaitGroup
		var release sync.Once
		defer func() { release.Do(func() { close(source.finish) }); wg.Wait() }()
		done := make(chan error, width)
		for index, reader := range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				actual := make([]byte, 1)
				_, err := reader.ReadAt(actual, 0)
				if err == nil && actual[0] != byte(index+1) {
					err = fmt.Errorf("generation content differs")
				}
				done <- err
			}()
		}
		synctest.Wait()
		require.Len(t, source.started, 8)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, width-8, width-8)
		release.Do(func() { close(source.finish) })
		wg.Wait()
		for range width {
			require.NoError(t, <-done)
		}
		requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
	})
}

func TestCompressedRangesUseExistingEncryptedContextStore(t *testing.T) {
	plain, built, expected, _ := compressedFixture(t, 2)
	base := objectstore.NewMemoryStore(t.Name())
	store := objectstore.EncryptingImmutable(base, objectstore.EncryptionConfig{
		Enabled: true, KeyEncryptor: diagnosticKeyWrapper{}, ChunkSize: CompressedDataRangeBytes,
	}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 32, MaxBytes: 1 << 20})
	for key, payload := range plain.objects {
		require.NoError(t, store.Put(key, bytes.NewReader(payload)))
	}
	cache, err := NewReadCache(DefaultReadCacheBytes)
	require.NoError(t, err)
	reader, err := NewReaderWithCacheContext(t.Context(), store, built.Descriptor, cache)
	require.NoError(t, err)
	actual := make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
}

// Measure real envelope bytes on a synthetic read pattern, not network time or
// a node -v trace. A small encoded demand can still expand to an encryption
// frame; keeping these domains separate prevents overstating compression gains.
func TestCompressedRangeEncryptedReadAmplification(t *testing.T) {
	_, _, expected, _ := compressedFixture(t, 1024)
	observed := make(map[string]int64)
	for _, version := range []int{DescriptorVersion, CompressedFormatVersion} {
		for _, frameBytes := range []int64{64 << 10, 16 << 10} {
			t.Run(fmt.Sprintf("format%d/frame%d", version, frameBytes), func(t *testing.T) {
				plain := newBuildTestStore()
				built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(expected), int64(len(expected)), plain,
					BuildOptions{FormatVersion: version, DataRangeBytes: CompressedDataRangeBytes})
				require.NoError(t, err)
				base := &diagnosticCountingStore{Store: objectstore.NewMemoryStore(t.Name())}
				store := objectstore.EncryptingImmutable(base, objectstore.EncryptionConfig{
					Enabled: true, KeyEncryptor: diagnosticKeyWrapper{}, ChunkSize: frameBytes,
				}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 32, MaxBytes: 1 << 20})
				for key, payload := range plain.objects {
					require.NoError(t, store.Put(key, bytes.NewReader(payload)))
				}
				reader, err := NewReader(store, built.Descriptor, 0)
				require.NoError(t, err)
				base.calls, base.bytes = 0, 0
				for _, index := range []int64{0, 2, 4, 6, 8, 10, 12, 14} {
					actual := make([]byte, 512)
					offset := index*CompressedDataRangeBytes + 4096
					_, err := reader.ReadAt(actual, offset)
					require.NoError(t, err)
					require.Equal(t, expected[offset:offset+512], actual)
				}
				require.Equal(t, 9, base.calls, "one cold envelope header GET plus eight demanded data GETs; no codec index")
				observed[fmt.Sprintf("%d/%d", version, frameBytes)] = base.bytes
				t.Logf("synthetic_read_count=8 demanded_bytes=4096 format=%d frame_bytes=%d fetched_ciphertext_bytes=%d gets=%d", version, frameBytes, base.bytes, base.calls)
			})
		}
	}
	require.Less(t, observed["2/65536"], observed["1/65536"])
	require.Less(t, observed["2/16384"], observed["2/65536"], "compression does not remove encrypted-frame amplification")
}

type corruptEncodedSource struct {
	RangeSource
	pack   string
	offset int64
}

func (s *corruptEncodedSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	body, err := s.RangeSource.Get(key, offset, length)
	if err != nil || key != s.pack || s.offset < offset || s.offset >= offset+length {
		return body, err
	}
	defer body.Close()
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	payload[s.offset-offset] ^= 0xff
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func TestCompressedRangesRejectCorruptNeighborWithoutPoisoningCache(t *testing.T) {
	store, built, expected, pack := compressedFixture(t, 1024)
	source := &corruptEncodedSource{RangeSource: store, pack: pack, offset: -1}
	reader, err := NewReader(source, built.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	bad := reader.root.Entries[10].Object
	source.offset = bad.Offset
	actual := make([]byte, bulkReadThreshold)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err, "bad speculative neighbor must not fail healthy demanded bytes")
	require.Equal(t, expected[:len(actual)], actual)
	_, err = reader.ReadAt(make([]byte, 1), 10*CompressedDataRangeBytes)
	require.Error(t, err)
	_, present := reader.cache.get(rangeCacheKey(bad))
	require.False(t, present)
	requireReadAdmissionUsage(t, &reader.cache.sourceSlots, 0, 0, 0)
	source.offset = -1
	one := make([]byte, 1)
	_, err = reader.ReadAt(one, 10*CompressedDataRangeBytes)
	require.NoError(t, err)
	require.Equal(t, expected[10*CompressedDataRangeBytes:10*CompressedDataRangeBytes+1], one)
}

func TestCompressedRangesReadDemandWithoutPackIndex(t *testing.T) {
	for _, cacheBytes := range []int64{0, DefaultReadCacheBytes} {
		t.Run(fmt.Sprint(cacheBytes), func(t *testing.T) {
			store, built, expected, pack := compressedFixture(t, 1024)
			source := &adaptiveSource{RangeSource: store, pack: pack}
			reader, err := NewReader(source, built.Descriptor, cacheBytes)
			require.NoError(t, err)
			require.Equal(t, CompressedFormatVersion, reader.root.formatVersion())
			require.Equal(t, RangeEncodingZstd, reader.root.Entries[0].Object.Encoding)
			require.Empty(t, reader.root.Entries[5].Object.Encoding)
			for _, index := range []int{0, 5, len(expected)/CompressedDataRangeBytes - 1} {
				actual := make([]byte, 512)
				_, err := reader.ReadAt(actual, int64(index*CompressedDataRangeBytes+4096))
				require.NoError(t, err)
				require.Equal(t, expected[index*CompressedDataRangeBytes+4096:index*CompressedDataRangeBytes+4608], actual)
				entry := reader.root.Entries[index].Object
				got := source.requests()
				require.Equal(t, adaptiveGet{entry.Offset, entry.StoredLength()}, got[len(got)-1], "demand must fetch only its physical range, without a separate codec index")
			}
			require.Len(t, source.requests(), 3)
		})
	}
}

func TestCompressedRangesCoalesceMixedBulkReadsAndTrimCachedEdges(t *testing.T) {
	for _, cacheBytes := range []int64{0, DefaultReadCacheBytes} {
		for _, prime := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache%d/prime%t", cacheBytes, prime), func(t *testing.T) {
				store, built, expected, pack := compressedFixture(t, 1024)
				source := &adaptiveSource{RangeSource: store, pack: pack}
				reader, err := NewReader(source, built.Descriptor, cacheBytes)
				require.NoError(t, err)
				require.Len(t, reader.root.Entries, len(expected)/CompressedDataRangeBytes, "the complete test window must be populated")
				lastEntry := len(reader.root.Entries) - 1
				if prime {
					for _, index := range []int{0, lastEntry} {
						_, err := reader.ReadAt(make([]byte, 1), int64(index*CompressedDataRangeBytes))
						require.NoError(t, err)
					}
				}
				before := len(source.requests())
				actual := make([]byte, len(expected))
				_, err = reader.ReadAt(actual, 0)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				gets := source.requests()[before:]
				require.Len(t, gets, 1, "bulk demand retains one physical GET rather than one per compressed block")
				first, last := 0, lastEntry
				if prime && cacheBytes > 0 {
					first, last = 1, lastEntry-1
				}
				start, end := reader.root.Entries[first].Object, reader.root.Entries[last].Object
				require.Equal(t, adaptiveGet{start.Offset, end.Offset + end.StoredLength() - start.Offset}, gets[0])
				requireReadAdmissionUsage(t, &reader.cache.sourceSlots, 0, 0, 0)
			})
		}
	}
}

func TestCompressedIncrementalAndBatchPreserveDecodedSubranges(t *testing.T) {
	store, base, expected, _ := compressedFixture(t, 2)
	for step, block := range []uint64{1, 3, 15, 31} {
		data := bytes.Repeat([]byte{byte(0xb0 + step)}, LogicalBlockSize)
		next, err := BuildIncrementalGeneration(t.Context(), store, base.Descriptor,
			[]BlockUpdate{{Block: block, Data: data}}, store, BuildOptions{PageEntries: 2})
		require.NoError(t, err)
		require.Equal(t, CompressedFormatVersion, next.Descriptor.Version)
		changed, err := ChangedBlocks(t.Context(), store, base.Descriptor, next.Descriptor, 8)
		require.NoError(t, err)
		require.Equal(t, []uint64{block}, changed, "splitting an encoded range must not mark unchanged decoded views dirty")
		copy(expected[int(block)*LogicalBlockSize:], data)
		reader, err := NewReader(store, next.Descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		actual := make([]byte, len(expected))
		_, err = reader.ReadAt(actual, 0)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		base = next
	}
	inputs := make([]BatchIncrementalInput, 2)
	for index := range inputs {
		descriptor, _, err := BuildCompositeGeneration(base.Descriptor, []BlockUpdate{{Block: uint64(7 + index), Data: bytes.Repeat([]byte{byte(0xd0 + index)}, LogicalBlockSize)}})
		require.NoError(t, err)
		inputs[index] = BatchIncrementalInput{ID: fmt.Sprint(index), Descriptor: descriptor}
	}
	batch, err := BuildIncrementalGenerationsBatch(t.Context(), store, inputs, store, BuildOptions{PageEntries: 2})
	require.NoError(t, err)
	for index := range inputs {
		result := batch.Results[fmt.Sprint(index)]
		require.Equal(t, CompressedFormatVersion, result.Descriptor.Version)
		require.Nil(t, result.Descriptor.CompositeTail)
		reader, err := NewReader(store, result.Descriptor, 0)
		require.NoError(t, err)
		actual := make([]byte, len(expected))
		_, err = reader.ReadAt(actual, 0)
		require.NoError(t, err)
		want := bytes.Clone(expected)
		copy(want[(7+index)*LogicalBlockSize:], bytes.Repeat([]byte{byte(0xd0 + index)}, LogicalBlockSize))
		require.Equal(t, want, actual)
	}
}

func TestCompressedRangeIntegrityAndDecodeBounds(t *testing.T) {
	plain := bytes.Repeat([]byte{0x41}, CompressedDataRangeBytes)
	encoded, object, err := encodeRangePayload(t.Context(), plain)
	require.NoError(t, err)
	object.Key = "packs/encoded"
	actual, err := decodeRangePayload(t.Context(), object, encoded)
	require.NoError(t, err)
	require.Equal(t, plain, actual)
	for _, mutation := range []string{"checksum", "decoded-short", "decoded-long", "encoded-short", "encoded-excess", "corrupt", "codec", "raw-with-encoded-length"} {
		t.Run(mutation, func(t *testing.T) {
			bad, payload := object, bytes.Clone(encoded)
			switch mutation {
			case "checksum":
				bad.Checksum = digest.FromString("wrong").String()
			case "decoded-short":
				bad.Length--
			case "decoded-long":
				bad.Length++
			case "encoded-short":
				payload = payload[:len(payload)-1]
			case "encoded-excess":
				payload = append(payload, 0)
			case "corrupt":
				payload[len(payload)/2] ^= 0xff
			case "codec":
				bad.Encoding = "gzip"
			case "raw-with-encoded-length":
				bad.Encoding = ""
			}
			_, err := decodeRangePayload(t.Context(), bad, payload)
			require.Error(t, err)
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = decodeRangePayload(ctx, object, encoded)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCompressedMappingVersionAndViewBounds(t *testing.T) {
	_, object, err := encodeRangePayload(t.Context(), bytes.Repeat([]byte{1}, CompressedDataRangeBytes))
	require.NoError(t, err)
	object.Key = "packs/encoded"
	page := MappingPage{Version: CompressedFormatVersion, BlockCount: 16, Entries: []MappingEntry{{Kind: MappingEntryData, BlockCount: 1, DataOffset: 4096, Object: object}}}
	payload, err := EncodeMappingPage(page)
	require.NoError(t, err)
	decoded, err := DecodeMappingPage(payload)
	require.NoError(t, err)
	require.Equal(t, page, decoded)
	legacy := page
	legacy.Version = 0
	_, err = EncodeMappingPage(legacy)
	require.Error(t, err)
	for _, offset := range []uint32{1, CompressedDataRangeBytes} {
		bad := page
		bad.Entries = append([]MappingEntry(nil), page.Entries...)
		bad.Entries[0].DataOffset = offset
		require.Error(t, bad.Validate())
	}
	for _, at := range []int{mappingPageHeaderBytes + 65, mappingPageHeaderBytes + 76} {
		bad := bytes.Clone(payload)
		bad[at] = 1
		_, err := DecodeMappingPage(bad)
		require.Error(t, err)
	}
	bad := bytes.Clone(payload)
	binary.BigEndian.PutUint16(bad[8:10], MappingPageVersion)
	_, err = DecodeMappingPage(bad)
	require.Error(t, err)
}
