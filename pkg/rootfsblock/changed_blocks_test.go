package rootfsblock

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestChangedBlocksSkipsIdenticalMappingRoot(t *testing.T) {
	objects := &countingRangeSource{objects: make(map[string][]byte)}
	descriptor := changedBlocksDescriptor(t, objects, "same", 8, nil)

	changed, err := ChangedBlocks(t.Context(), objects, descriptor, descriptor, 8)
	require.NoError(t, err)
	require.Empty(t, changed)
	require.Empty(t, objects.reads)
}

func TestChangedBlocksFindsDataAndHoleChangesWithoutReadingPayloads(t *testing.T) {
	objects := &countingRangeSource{objects: make(map[string][]byte)}
	shared := ObjectRange{
		Key: "rootfs/packs/shared", Offset: 4096, Length: 2 * LogicalBlockSize,
		Checksum: digest.FromString("shared-two-block-range").String(),
	}
	oldDescriptor := changedBlocksDescriptor(t, objects, "old-data-hole", 4, []MappingEntry{
		{LogicalStart: 0, BlockCount: 2, Kind: MappingEntryData, Object: shared},
	})
	currentDescriptor := changedBlocksDescriptor(t, objects, "current-data-hole", 4, []MappingEntry{
		{
			LogicalStart: 0, BlockCount: 1, Kind: MappingEntryData,
			Object: ObjectRange{
				Key: shared.Key, Offset: shared.Offset, Length: LogicalBlockSize,
				Checksum: digest.FromString("shared-first-block").String(),
			},
		},
		{
			LogicalStart: 2, BlockCount: 1, Kind: MappingEntryData,
			Object: ObjectRange{
				Key: "rootfs/packs/new", Length: LogicalBlockSize,
				Checksum: digest.FromString("new-third-block").String(),
			},
		},
	})

	changed, err := ChangedBlocks(t.Context(), objects, oldDescriptor, currentDescriptor, 4)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, changed)
	require.ElementsMatch(t, []string{"rootfs/maps/old-data-hole", "rootfs/maps/current-data-hole"}, objects.reads)
}

func TestChangedBlocksIncludesCompositeTailOverrides(t *testing.T) {
	objects := &countingRangeSource{objects: make(map[string][]byte)}
	base := changedBlocksDescriptor(t, objects, "tail-base", 4, nil)
	current, _, err := BuildCompositeGeneration(base, []BlockUpdate{
		{Block: 3, Data: bytes.Repeat([]byte{0x41}, LogicalBlockSize)},
		{Block: 3, Data: bytes.Repeat([]byte{0x42}, LogicalBlockSize)},
		{Block: 1, Data: bytes.Repeat([]byte{0x43}, LogicalBlockSize)},
	})
	require.NoError(t, err)

	changed, err := ChangedBlocks(t.Context(), objects, base, current, 4)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3}, changed)
	require.Empty(t, objects.reads, "identical immutable roots must not be fetched for tail-only changes")
}

func TestChangedBlocksEnforcesAdmissionLimit(t *testing.T) {
	objects := &countingRangeSource{objects: make(map[string][]byte)}
	oldDescriptor := changedBlocksDescriptor(t, objects, "limit-old", 4, nil)
	currentDescriptor := changedBlocksDescriptor(t, objects, "limit-current", 4, []MappingEntry{
		{
			LogicalStart: 0, BlockCount: 4, Kind: MappingEntryData,
			Object: ObjectRange{
				Key: "rootfs/packs/all", Length: 4 * LogicalBlockSize,
				Checksum: digest.FromString("all-blocks").String(),
			},
		},
	})

	_, err := ChangedBlocks(t.Context(), objects, oldDescriptor, currentDescriptor, 3)
	var limitErr *ChangedBlockLimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, 3, limitErr.Limit)
	require.GreaterOrEqual(t, limitErr.RequiredAtLeast, uint64(4))
}

func TestChangedBlocksTraversesNestedIncrementalMappingAndReusesSplitRanges(t *testing.T) {
	payload := make([]byte, 16*LogicalBlockSize)
	for block := range 16 {
		copy(payload[block*LogicalBlockSize:], bytes.Repeat([]byte{byte(block + 1)}, LogicalBlockSize))
	}
	store := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), store, BuildOptions{
		DataRangeBytes: 2 * LogicalBlockSize, PackBytes: 4 * LogicalBlockSize,
		PageEntries: 2, ObjectPrefix: "nested-changed-blocks",
	})
	require.NoError(t, err)
	next, err := BuildIncrementalGeneration(t.Context(), store, base.Descriptor, []BlockUpdate{
		{Block: 7, Data: bytes.Repeat([]byte{0xee}, LogicalBlockSize)},
	}, store, BuildOptions{
		DataRangeBytes: 2 * LogicalBlockSize, PackBytes: 4 * LogicalBlockSize,
		PageEntries: 2, ObjectPrefix: "nested-changed-blocks",
	})
	require.NoError(t, err)
	objects := &countingRangeSource{objects: store.values, rejectPacks: true}

	changed, err := ChangedBlocks(t.Context(), objects, base.Descriptor, next.Descriptor, 16)
	require.NoError(t, err)
	require.Equal(t, []uint64{7}, changed)
	require.NotEmpty(t, objects.reads)
}

func TestChangedBlocksHandlesHundredGiBSparseMappingWithMetadataOnly(t *testing.T) {
	objects := &countingRangeSource{objects: make(map[string][]byte), rejectPacks: true}
	const logicalSize = int64(100 << 30)
	totalBlocks := uint64(logicalSize / LogicalBlockSize)
	dirtyBlock := totalBlocks - 2
	oldDescriptor := changedBlocksDescriptor(t, objects, "sparse-old", totalBlocks, nil)
	currentDescriptor := changedBlocksDescriptor(t, objects, "sparse-current", totalBlocks, []MappingEntry{
		{
			LogicalStart: dirtyBlock, BlockCount: 1, Kind: MappingEntryData,
			Object: ObjectRange{
				Key: "rootfs/packs/sparse-data", Length: LogicalBlockSize,
				Checksum: digest.FromString("sparse-data").String(),
			},
		},
	})

	changed, err := ChangedBlocks(t.Context(), objects, oldDescriptor, currentDescriptor, 1)
	require.NoError(t, err)
	require.Equal(t, []uint64{dirtyBlock}, changed)
	require.Len(t, objects.reads, 2)
}

func changedBlocksDescriptor(
	t *testing.T,
	objects *countingRangeSource,
	suffix string,
	totalBlocks uint64,
	entries []MappingEntry,
) Descriptor {
	t.Helper()
	payload, err := EncodeMappingPage(MappingPage{
		StartBlock: 0, BlockCount: totalBlocks, Entries: entries,
	})
	require.NoError(t, err)
	key := "rootfs/maps/" + suffix
	objects.objects[key] = payload
	checksum := digest.FromBytes(payload).String()
	return Descriptor{
		Version: DescriptorVersion, LogicalSizeBytes: int64(totalBlocks) * LogicalBlockSize,
		BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{
			Version: MappingPageVersion, RootDigest: checksum,
			Object: ObjectRange{Key: key, Length: int64(len(payload)), Checksum: checksum},
		},
	}
}

type countingRangeSource struct {
	objects     map[string][]byte
	reads       []string
	rejectPacks bool
}

func (s *countingRangeSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if s.rejectPacks && len(key) >= len("rootfs/packs/") && key[:len("rootfs/packs/")] == "rootfs/packs/" {
		return nil, fmt.Errorf("data payload %s must not be read", key)
	}
	payload, ok := s.objects[key]
	if !ok || offset < 0 || length < 0 || offset > int64(len(payload))-length {
		return nil, fmt.Errorf("object range %s/%d/%d is missing", key, offset, length)
	}
	s.reads = append(s.reads, key)
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}
