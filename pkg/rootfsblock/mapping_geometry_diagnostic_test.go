package rootfsblock

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// This exercises the real builder and verified reader without materializing a
// multi-GiB data image. It measures mapping bytes and dependent source calls,
// not encryption, network latency, NBD, or an end-to-end startup SLO. Entry
// count and key length approximate the observed 64 KiB import; data is synthetic.
func TestMappingGeometryDiagnosticDependentReads(t *testing.T) {
	const entryCount = 75 * 1024
	const dataBytes = 64 << 10
	const blocksPerEntry = dataBytes / LogicalBlockSize
	const logicalBytes = 16 << 30
	const prefix = "rootfs/v1/diagnostic-range-64k-frames64k-PHBKs8"
	checksum := digest.FromString("synthetic-data-not-published").String()
	entries := make([]MappingEntry, entryCount)
	for index := range entries {
		entries[index] = MappingEntry{
			LogicalStart: uint64(index * blocksPerEntry), BlockCount: blocksPerEntry, Kind: MappingEntryData,
			Object: ObjectRange{
				Key:    fmt.Sprintf("%s/packs/sha256/%064x", prefix, index/1024),
				Offset: int64(index%1024) * dataBytes, Length: dataBytes, Checksum: checksum,
			},
		}
	}
	require.Len(t, entries[0].Object.Key, 125)
	for _, test := range []struct {
		fanout           int
		level            uint8
		pages            int
		coldMappingGets  int
		coldMappingBytes int64
	}{
		{1024, 1, 76, 2, 9702},
		{512, 1, 151, 2, 14045},
		{256, 2, 303, 3, 22415},
	} {
		t.Run(fmt.Sprintf("entries-%d", test.fanout), func(t *testing.T) {
			store := newBuildTestStore()
			options, err := NormalizeBuildOptions(BuildOptions{DataRangeBytes: dataBytes, PageEntries: test.fanout, ObjectPrefix: prefix})
			require.NoError(t, err)
			builder := generationBuilder{
				ctx: t.Context(), publisher: store, options: options,
				references: make(map[string]ObjectReference),
			}
			root, rootPayload, err := builder.publishMappingTree(entries, logicalBytes/LogicalBlockSize)
			require.NoError(t, err)
			page, err := DecodeMappingPage(rootPayload)
			require.NoError(t, err)
			require.Equal(t, test.level, page.Level)
			require.Equal(t, test.pages, builder.objects)
			source := &mappingGeometrySource{store: store}
			reader, err := NewReader(source, testReaderDescriptor(root, logicalBytes/LogicalBlockSize), DefaultReadCacheBytes)
			require.NoError(t, err)
			entry, _, found, err := reader.resolve(reader.root, 0)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, entries[0], entry)
			require.Equal(t, test.coldMappingGets, source.calls)
			require.Equal(t, test.coldMappingBytes, source.bytes)
			t.Logf("synthetic_entries=%d page_entries=%d root_level=%d published_mapping_pages=%d cold_mapping_gets=%d cold_mapping_bytes=%d root_bytes=%d",
				entryCount, test.fanout, page.Level, builder.objects, source.calls, source.bytes, len(rootPayload))

			// A repeat uses only verified mapping cache. This is not a second
			// sandbox claim or a measurement of cached end-to-end startup.
			source.calls, source.bytes = 0, 0
			_, _, found, err = reader.resolve(reader.root, 0)
			require.NoError(t, err)
			require.True(t, found)
			require.Zero(t, source.calls)
			require.Zero(t, source.bytes)
			last, _, found, err := reader.resolve(reader.root, entries[len(entries)-1].LogicalStart)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, entries[len(entries)-1], last)
			_, _, found, err = reader.resolve(reader.root, logicalBytes/LogicalBlockSize-1)
			require.NoError(t, err)
			require.False(t, found, "implicit zero space must remain unmapped")
		})
	}
}

type mappingGeometrySource struct {
	store *buildTestStore
	calls int
	bytes int64
}

func (s *mappingGeometrySource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if !strings.Contains(key, "/maps/sha256/") {
		return nil, fmt.Errorf("geometry diagnostic must not read data packs")
	}
	body, err := s.store.Get(key, offset, length)
	if err == nil {
		s.calls++
		s.bytes += length
	}
	return body, err
}
