package rootfsblock

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSparseGrowthReusesDataAndPreservesOldGeneration(t *testing.T) {
	for _, entries := range []int{2, 1024} {
		t.Run(strconv.Itoa(entries), func(t *testing.T) {
			objects := newMemoryObjects()
			payload := bytes.Repeat([]byte{0xab}, 8*LogicalBlockSize)
			options := BuildOptions{DataRangeBytes: LogicalBlockSize, PageEntries: entries}
			base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), objects, options)
			require.NoError(t, err)
			objects.resetPublished()
			grown, err := GrowSparseGeneration(t.Context(), objects, base.Descriptor, 64<<40, objects, options)
			require.NoError(t, err)
			require.Less(t, grown.Bytes, int64(4096))
			for _, reference := range grown.References {
				require.Equal(t, ObjectKindMappingPage, reference.Kind)
			}
			reader, err := NewReader(objects, grown.Descriptor, 0)
			require.NoError(t, err)
			actual := make([]byte, len(payload))
			_, err = reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, payload, actual)
			last := bytes.Repeat([]byte{0xff}, LogicalBlockSize)
			_, err = reader.ReadAt(last, reader.Size()-LogicalBlockSize)
			require.NoError(t, err)
			require.Equal(t, make([]byte, LogicalBlockSize), last)
			old, err := NewReader(objects, base.Descriptor, 0)
			require.NoError(t, err)
			_, err = old.ReadAt(last, old.Size())
			require.ErrorIs(t, err, io.EOF)
			updated, err := BuildIncrementalGeneration(t.Context(), objects, grown.Descriptor, []BlockUpdate{{Block: uint64(reader.Size()/LogicalBlockSize - 1), Data: payload[:LogicalBlockSize]}}, objects, options)
			require.NoError(t, err)
			next, err := NewReader(objects, updated.Descriptor, 0)
			require.NoError(t, err)
			_, err = next.ReadAt(last, next.Size()-LogicalBlockSize)
			require.NoError(t, err)
			require.Equal(t, payload[:LogicalBlockSize], last)
		})
	}
}

func TestSparseGrowthRejectsShrinkAndMisalignment(t *testing.T) {
	objects := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(make([]byte, 2*LogicalBlockSize)), 2*LogicalBlockSize, objects, BuildOptions{})
	require.NoError(t, err)
	objects.resetPublished()
	for _, size := range []int64{-LogicalBlockSize, LogicalBlockSize, 2*LogicalBlockSize + 1} {
		_, err = GrowSparseGeneration(t.Context(), objects, base.Descriptor, size, objects, BuildOptions{})
		require.Error(t, err)
	}
	require.Zero(t, objects.publishedBytes)
}

func TestSparseDeviceEditsAcrossSixteenTiBChildBoundaries(t *testing.T) {
	objects := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(make([]byte, 2*LogicalBlockSize)), 2*LogicalBlockSize, objects, BuildOptions{})
	require.NoError(t, err)
	sparse, err := GrowSparseGeneration(t.Context(), objects, base.Descriptor, 64<<40, objects, BuildOptions{})
	require.NoError(t, err)
	updates := make([]BlockUpdate, 0, DefaultPageEntries+32)
	for band := range 4 {
		for index := range 264 {
			block := uint64((int64(band)*(16<<40))/LogicalBlockSize) + uint64(2*index)
			updates = append(updates, BlockUpdate{Block: block, Data: bytes.Repeat([]byte{byte(1 + index%250)}, LogicalBlockSize)})
		}
	}
	updates = append(updates, BlockUpdate{Block: uint64((64<<40)/LogicalBlockSize - 1), Data: bytes.Repeat([]byte{0x72}, LogicalBlockSize)})
	first, err := BuildIncrementalGeneration(t.Context(), objects, sparse.Descriptor, updates, objects, BuildOptions{})
	require.NoError(t, err)
	reader, err := NewReader(objects, first.Descriptor, 0)
	require.NoError(t, err)
	for _, update := range updates {
		var actual [LogicalBlockSize]byte
		_, err = reader.ReadAt(actual[:], int64(update.Block)*LogicalBlockSize)
		require.NoError(t, err)
		require.Equal(t, update.Data, actual[:])
	}
	// Exercise gap path creation in the new internal root, along with an
	// overwrite and an explicit zero mask of data from its immutable base.
	changed := []BlockUpdate{
		{Block: uint64((32<<40)/LogicalBlockSize) - 1, Data: bytes.Repeat([]byte{0x31}, LogicalBlockSize)},
		{Block: updates[len(updates)-1].Block, Data: bytes.Repeat([]byte{0x19}, LogicalBlockSize)},
		{Block: updates[700].Block, Data: make([]byte, LogicalBlockSize)},
	}
	second, err := BuildIncrementalGeneration(t.Context(), objects, first.Descriptor, changed, objects, BuildOptions{})
	require.NoError(t, err)
	newReader, err := NewReader(objects, second.Descriptor, 0)
	require.NoError(t, err)
	for _, update := range changed {
		var actual [LogicalBlockSize]byte
		_, err = newReader.ReadAt(actual[:], int64(update.Block)*LogicalBlockSize)
		require.NoError(t, err)
		require.Equal(t, update.Data, actual[:])
	}
	var original [LogicalBlockSize]byte
	_, err = reader.ReadAt(original[:], int64(updates[700].Block)*LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, updates[700].Data, original[:], "earlier snapshot must remain immutable")
}
