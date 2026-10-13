package rootfsblock

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaterializedSparseGeometryScansOnlyOwnedInput(t *testing.T) {
	input := bytes.Repeat([]byte{0x73}, 3*CompressedDataRangeBytes+LogicalBlockSize)
	for _, withLayout := range []bool{false, true} {
		var layout *DataRangeLayout
		if withLayout {
			var err error
			layout, err = NewDataRangeLayout(int64(len(input)), CompressedDataRangeBytes,
				[]DataRangeSpan{{Start: LogicalBlockSize, End: LogicalBlockSize + 2*CompressedDataRangeBytes}})
			require.NoError(t, err)
		}
		store := newBuildTestStore()
		counter := &sparseCountReader{ReaderAt: bytes.NewReader(input)}
		built, err := buildMaterializedGenerationGeometry(t.Context(), counter, int64(len(input)), 64<<40, store, BuildOptions{}, layout)
		require.NoError(t, err)
		require.Equal(t, int64(len(input)), counter.bytes)
		require.Less(t, built.Bytes, int64(1<<20))
		reader, err := NewReader(store, built.Descriptor, 0)
		require.NoError(t, err)
		actual := make([]byte, len(input))
		_, err = reader.ReadAt(actual, 0)
		require.NoError(t, err)
		require.Equal(t, input, actual)
		tail := make([]byte, LogicalBlockSize)
		_, err = reader.ReadAt(tail, (64<<40)-LogicalBlockSize)
		require.NoError(t, err)
		require.Equal(t, make([]byte, LogicalBlockSize), tail)
	}
}

func TestMaterializedSparseFileMatchesExpandedDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned-image")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(3*CompressedDataRangeBytes+LogicalBlockSize))
	_, err = file.WriteAt(bytes.Repeat([]byte{0x49}, LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	file, err = os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.Seek(17, 0)
	require.NoError(t, err)
	store := newBuildTestStore()
	base, err := BuildMaterializedFileGeneration(t.Context(), file, 3*CompressedDataRangeBytes+LogicalBlockSize, store, BuildOptions{}, nil)
	require.NoError(t, err)
	expanded, err := GrowSparseGeneration(t.Context(), store, base.Descriptor, 64<<40, store, BuildOptions{})
	require.NoError(t, err)
	sparse, err := BuildMaterializedSparseFileGeneration(t.Context(), file, 64<<40, store, BuildOptions{}, nil)
	require.NoError(t, err)
	require.Equal(t, expanded.Descriptor, sparse.Descriptor)
	position, err := file.Seek(0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(17), position)
	_, err = BuildMaterializedSparseFileGeneration(t.Context(), file, LogicalBlockSize, store, BuildOptions{}, nil)
	require.Error(t, err)
}
