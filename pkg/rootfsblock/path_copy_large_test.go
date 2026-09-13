package rootfsblock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// All 16,777,216 extents are mapped, with actual checksums for every mapping
// page. Leaf bodies are generated on demand and repeated nonzero data shares
// one compressed object. This is a block-map algorithm fixture, NOT a populated
// XFS image, a real 1TiB transfer, or a cold-start/RSS benchmark.
type generatedDenseMappingSource struct {
	*memoryObjects
	leafPrototype []byte
	stride        int
	reads         int
	readBytes     int64
	dataReads     int
}

func (s *generatedDenseMappingSource) fillLeaf(payload []byte, start uint64) {
	binary.BigEndian.PutUint64(payload[16:24], start)
	for index := range DefaultPageEntries {
		offset := mappingPageHeaderBytes + index*s.stride
		binary.BigEndian.PutUint64(payload[offset:offset+8], start+uint64(index*CompressedDataRangeBytes/LogicalBlockSize))
	}
}

func (s *generatedDenseMappingSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.reads++
	s.readBytes += length
	if strings.Contains(key, "/packs/") || key == "packs/repeated" {
		s.dataReads++
	}
	if !strings.HasPrefix(key, "maps/generated-leaf/") {
		return s.memoryObjects.Get(key, offset, length)
	}
	start, err := strconv.ParseUint(strings.TrimPrefix(key, "maps/generated-leaf/"), 10, 64)
	if err != nil || offset < 0 || length < 0 || offset+length > int64(len(s.leafPrototype)) {
		return nil, fmt.Errorf("invalid generated leaf request")
	}
	payload := bytes.Clone(s.leafPrototype)
	s.fillLeaf(payload, start)
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func TestPathCopyOneTiBFullyMappedBlockModel(t *testing.T) {
	const logicalBytes = int64(1 << 40)
	const totalBlocks = uint64(logicalBytes / LogicalBlockSize)
	const leafBlocks = uint64(DefaultPageEntries * CompressedDataRangeBytes / LogicalBlockSize)
	const leafCount = int(totalBlocks / leafBlocks)
	source := &generatedDenseMappingSource{memoryObjects: newMemoryObjects()}
	encoded, data, err := encodeRangePayload(t.Context(), bytes.Repeat([]byte{0x53}, CompressedDataRangeBytes))
	require.NoError(t, err)
	data.Key = "packs/repeated"
	require.NoError(t, source.PutImmutable(t.Context(), data.Key, encoded))
	leaf := MappingPage{Version: CompressedFormatVersion, BlockCount: leafBlocks, Entries: make([]MappingEntry, DefaultPageEntries)}
	for index := range leaf.Entries {
		leaf.Entries[index] = MappingEntry{
			LogicalStart: uint64(index * CompressedDataRangeBytes / LogicalBlockSize),
			BlockCount:   CompressedDataRangeBytes / LogicalBlockSize, Kind: MappingEntryData, Object: data,
		}
	}
	source.leafPrototype, err = EncodeMappingPage(leaf)
	require.NoError(t, err)
	source.stride = compressedMappingEntryBytes + len(data.Key)
	leaves := make([]publishedPage, leafCount)
	buffer := bytes.Clone(source.leafPrototype)
	for index := range leaves {
		start := uint64(index) * leafBlocks
		source.fillLeaf(buffer, start)
		leaves[index] = publishedPage{start: start, count: leafBlocks, object: ObjectRange{
			Key: fmt.Sprintf("maps/generated-leaf/%d", start), Length: int64(len(buffer)), Checksum: digest.FromBytes(buffer).String(),
		}}
	}
	options, err := NormalizeBuildOptions(BuildOptions{FormatVersion: CompressedFormatVersion})
	require.NoError(t, err)
	builder := generationBuilder{ctx: t.Context(), publisher: source, options: options}
	parents := make([]publishedPage, 0, leafCount/DefaultPageEntries)
	for index := 0; index < len(leaves); index += DefaultPageEntries {
		parent, err := builder.publishInternalPage(leaves[index:index+DefaultPageEntries], 1, false, totalBlocks)
		require.NoError(t, err)
		parents = append(parents, parent)
	}
	root, err := builder.publishInternalPage(parents, 2, true, totalBlocks)
	require.NoError(t, err)
	base := Descriptor{Version: CompressedFormatVersion, LogicalSizeBytes: logicalBytes, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{Version: CompressedFormatVersion, RootDigest: root.object.Checksum, Object: root.object}}
	updates := []BlockUpdate{
		{Block: 1, Data: bytes.Repeat([]byte{0xa1}, LogicalBlockSize)},
		{Block: totalBlocks - 2, Data: bytes.Repeat([]byte{0xb2}, LogicalBlockSize)},
	}
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch-%t", batch), func(t *testing.T) {
			source.reads, source.readBytes, source.dataReads = 0, 0, 0
			var result BuildResult
			if batch {
				composite, _, err := BuildCompositeGeneration(base, updates)
				require.NoError(t, err)
				built, err := BuildIncrementalGenerationsBatch(t.Context(), source, []BatchIncrementalInput{{ID: "dense", Descriptor: composite}}, source, options)
				require.NoError(t, err)
				result = built.Results["dense"]
			} else {
				result, err = BuildIncrementalGeneration(t.Context(), source, base, updates, source, options)
				require.NoError(t, err)
			}
			require.Equal(t, 5, source.reads, "root plus two disjoint two-page paths")
			require.Zero(t, source.dataReads, "decoded views do not download unchanged compressed ranges")
			require.Less(t, source.readBytes, int64(1<<20))
			t.Logf("logical_bytes=%d mapped_extents=%d edited_blocks=2 source_gets=%d source_bytes=%d source_data_gets=%d", logicalBytes, leafCount*DefaultPageEntries, source.reads, source.readBytes, source.dataReads)
			reader, err := NewReader(source, result.Descriptor, 0)
			require.NoError(t, err)
			for _, block := range []uint64{0, 1, 2, leafBlocks, totalBlocks / 2, totalBlocks - 2, totalBlocks - 1} {
				actual := make([]byte, LogicalBlockSize)
				_, err := reader.ReadAt(actual, int64(block)*LogicalBlockSize)
				require.NoError(t, err)
				value := byte(0x53)
				if block == 1 {
					value = 0xa1
				}
				if block == totalBlocks-2 {
					value = 0xb2
				}
				require.Equal(t, bytes.Repeat([]byte{value}, LogicalBlockSize), actual)
			}
		})
	}
}
