package rootfsblock

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderRetainsVerifiedRangeForOneDemandWithoutCache(t *testing.T) {
	store, descriptor, expected, pack := coalescingFixture(t)
	reader, err := NewReader(store, descriptor, 0)
	require.NoError(t, err)
	actual := make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, 2, store.count(pack), "one data GET per demanded range, not per 4 KiB block")
}

func TestReaderRetainedRangeRespectsCompositeTail(t *testing.T) {
	store, descriptor, expected, pack := coalescingFixture(t)
	tailPayload := bytes.Repeat([]byte{0x73}, LogicalBlockSize)
	tail, err := EncodeCompositeTail([]BlockUpdate{{Block: 17, Data: tailPayload}}, uint64(len(expected)/LogicalBlockSize))
	require.NoError(t, err)
	descriptor.CompositeTail = &tail
	copy(expected[17*LogicalBlockSize:], tailPayload)
	reader, err := NewReader(store, descriptor, 0)
	require.NoError(t, err)
	actual := make([]byte, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	require.Equal(t, 2, store.count(pack))
}

func TestBranchCoalescesVerifiedCleanSpansAndStopsAtJournalOverrides(t *testing.T) {
	store, descriptor, expected, pack := coalescingFixture(t)
	reader, err := NewReader(store, descriptor, 0)
	require.NoError(t, err)
	branch, err := OpenBranch(filepath.Join(t.TempDir(), "branch"), testBranchIdentity(int64(len(expected))), reader)
	require.NoError(t, err)
	defer branch.Close()
	dirty := bytes.Repeat([]byte{0x72}, LogicalBlockSize)
	_, err = branch.WriteAt(dirty, 8*LogicalBlockSize)
	require.NoError(t, err)
	copy(expected[8*LogicalBlockSize:], dirty)
	actual := make([]byte, len(expected)+1)
	n, err := branch.ReadAt(actual, 1)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, len(expected)-1, n)
	require.Equal(t, expected[1:], actual[:n])
	require.Equal(t, 3, store.count(pack), "the dirty block splits the first data range into two demanded clean spans")
}

func TestBranchCoalescedReadPreservesHolesAndCorruptionBoundary(t *testing.T) {
	store := newRangeTestStore()
	first := store.put("packs/first", bytes.Repeat([]byte{1}, 2*LogicalBlockSize))
	last := store.put("packs/last", bytes.Repeat([]byte{2}, 2*LogicalBlockSize))
	rootPayload, err := EncodeMappingPage(MappingPage{
		StartBlock: 0, BlockCount: 6,
		Entries: []MappingEntry{
			{LogicalStart: 0, BlockCount: 2, Kind: MappingEntryData, Object: first},
			{LogicalStart: 4, BlockCount: 2, Kind: MappingEntryData, Object: last},
		},
	})
	require.NoError(t, err)
	root := store.put("maps/root", rootPayload)
	reader, err := NewReader(store, testReaderDescriptor(root, 6), 0)
	require.NoError(t, err)
	branch, err := OpenBranch(filepath.Join(t.TempDir(), "branch"), testBranchIdentity(6*LogicalBlockSize), reader)
	require.NoError(t, err)
	defer branch.Close()
	store.objects[last.Key][0] ^= 1
	actual := bytes.Repeat([]byte{0xaa}, 6*LogicalBlockSize)
	n, err := branch.ReadAt(actual, 0)
	require.ErrorContains(t, err, "checksum mismatch")
	require.Equal(t, 4*LogicalBlockSize, n)
	require.Equal(t, bytes.Repeat([]byte{1}, 2*LogicalBlockSize), actual[:2*LogicalBlockSize])
	require.Equal(t, make([]byte, 2*LogicalBlockSize), actual[2*LogicalBlockSize:4*LogicalBlockSize])
	require.Equal(t, bytes.Repeat([]byte{0xaa}, 2*LogicalBlockSize), actual[4*LogicalBlockSize:], "unverified bytes must never be exposed")
}

func coalescingFixture(t *testing.T) (*rangeTestStore, Descriptor, []byte, string) {
	t.Helper()
	const rangeBytes = 32 * LogicalBlockSize
	logical := append(bytes.Repeat([]byte{1}, rangeBytes), bytes.Repeat([]byte{2}, rangeBytes)...)
	builder := newBuildTestStore()
	built, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(logical), int64(len(logical)), builder, BuildOptions{
		DataRangeBytes: rangeBytes, PageEntries: 2,
	})
	require.NoError(t, err)
	store := newRangeTestStore()
	var pack string
	for _, reference := range built.References {
		store.put(reference.Key, builder.objects[reference.Key])
		if reference.Kind == ObjectKindDataPack {
			pack = reference.Key
		}
	}
	require.NotEmpty(t, pack)
	return store, built.Descriptor, logical, pack
}
