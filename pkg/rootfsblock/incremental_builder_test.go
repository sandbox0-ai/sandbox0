package rootfsblock

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestBuildIncrementalGenerationReusesUnchangedRanges(t *testing.T) {
	basePayload := make([]byte, 8*LogicalBlockSize)
	for block := range 8 {
		copy(basePayload[block*LogicalBlockSize:], bytes.Repeat([]byte{byte(block + 1)}, LogicalBlockSize))
	}
	objects := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(basePayload), int64(len(basePayload)), objects, BuildOptions{
		DataRangeBytes: 4 * LogicalBlockSize, PackBytes: 8 * LogicalBlockSize, PageEntries: 4, ObjectPrefix: "test",
	})
	require.NoError(t, err)
	objects.resetPublished()

	blockTwo := bytes.Repeat([]byte{0xaa}, LogicalBlockSize)
	blockSix := bytes.Repeat([]byte{0xbb}, LogicalBlockSize)
	next, err := BuildIncrementalGeneration(t.Context(), objects, base.Descriptor, []BlockUpdate{
		{Block: 2, Data: blockTwo}, {Block: 4, Data: make([]byte, LogicalBlockSize)}, {Block: 6, Data: blockSix},
	}, objects, BuildOptions{DataRangeBytes: 4 * LogicalBlockSize, PackBytes: 8 * LogicalBlockSize, PageEntries: 4, ObjectPrefix: "test"})
	require.NoError(t, err)
	require.Less(t, objects.publishedBytes, int64(len(basePayload)))
	require.NotEmpty(t, next.References)
	for _, reference := range next.References {
		payload, found := objects.values[reference.Key]
		require.True(t, found)
		require.Equal(t, int64(len(payload)), reference.Size)
		require.Equal(t, digest.FromBytes(payload).String(), reference.Checksum)
	}

	reader, err := NewReader(objects, next.Descriptor, 1<<20)
	require.NoError(t, err)
	actual := make([]byte, len(basePayload))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	expected := append([]byte(nil), basePayload...)
	copy(expected[2*LogicalBlockSize:], blockTwo)
	clear(expected[4*LogicalBlockSize : 5*LogicalBlockSize])
	copy(expected[6*LogicalBlockSize:], blockSix)
	require.Equal(t, expected, actual)
}

func TestBuildIncrementalGenerationRejectsDuplicateUpdates(t *testing.T) {
	objects := newMemoryObjects()
	basePayload := bytes.Repeat([]byte{1}, 2*LogicalBlockSize)
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(basePayload), int64(len(basePayload)), objects, BuildOptions{})
	require.NoError(t, err)

	_, err = BuildIncrementalGeneration(t.Context(), objects, base.Descriptor, []BlockUpdate{
		{Block: 0, Data: make([]byte, LogicalBlockSize)}, {Block: 0, Data: make([]byte, LogicalBlockSize)},
	}, objects, BuildOptions{})
	require.ErrorContains(t, err, "duplicates")
}

func TestBuildIncrementalGenerationStreamsBlockReaderOnce(t *testing.T) {
	basePayload := bytes.Repeat([]byte{0x11}, 4*LogicalBlockSize)
	objects := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(basePayload), int64(len(basePayload)), objects, BuildOptions{})
	require.NoError(t, err)
	updates := &countingBlockUpdateReader{
		blocks: []uint64{3, 0, 2},
		values: map[uint64][]byte{
			0: bytes.Repeat([]byte{0x20}, LogicalBlockSize),
			2: make([]byte, LogicalBlockSize),
			3: bytes.Repeat([]byte{0x23}, LogicalBlockSize),
		},
		reads: make(map[uint64]int),
	}

	next, err := BuildIncrementalGenerationFromBlockReader(
		t.Context(), objects, base.Descriptor, updates, objects,
		BuildOptions{DataRangeBytes: 2 * LogicalBlockSize, PackBytes: 4 * LogicalBlockSize},
	)
	require.NoError(t, err)
	require.Equal(t, map[uint64]int{0: 1, 2: 1, 3: 1}, updates.reads)
	reader, err := NewReader(objects, next.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(basePayload))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, byte(0x20), actual[0])
	require.Equal(t, byte(0x11), actual[LogicalBlockSize])
	require.Equal(t, byte(0), actual[2*LogicalBlockSize])
	require.Equal(t, byte(0x23), actual[3*LogicalBlockSize])
}

func TestBuildIncrementalGenerationValidatesBlockReaderBeforePublishing(t *testing.T) {
	objects := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(make([]byte, 2*LogicalBlockSize)), 2*LogicalBlockSize, objects, BuildOptions{})
	require.NoError(t, err)
	objects.resetPublished()
	updates := &countingBlockUpdateReader{
		blocks: []uint64{1, 1}, values: map[uint64][]byte{1: make([]byte, LogicalBlockSize)}, reads: make(map[uint64]int),
	}
	_, err = BuildIncrementalGenerationFromBlockReader(t.Context(), objects, base.Descriptor, updates, objects, BuildOptions{})
	require.ErrorContains(t, err, "duplicated")
	require.Zero(t, objects.publishedBytes)
	require.Empty(t, updates.reads)
}

func TestReaderRejectsMismatchedLogicalRootDigest(t *testing.T) {
	objects := newMemoryObjects()
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(make([]byte, LogicalBlockSize)), LogicalBlockSize, objects, BuildOptions{})
	require.NoError(t, err)
	base.Descriptor.MappingRoot.RootDigest = digest.FromString("different-root").String()
	_, err = NewReader(objects, base.Descriptor, 0)
	require.ErrorContains(t, err, "root mapping digest")
}

type memoryObjects struct {
	values         map[string][]byte
	publishedBytes int64
}

func newMemoryObjects() *memoryObjects { return &memoryObjects{values: make(map[string][]byte)} }

func (m *memoryObjects) PutImmutable(_ context.Context, key string, payload []byte) error {
	if existing, ok := m.values[key]; ok {
		if !bytes.Equal(existing, payload) {
			return bytes.ErrTooLarge
		}
		return nil
	}
	m.values[key] = append([]byte(nil), payload...)
	m.publishedBytes += int64(len(payload))
	return nil
}

func (m *memoryObjects) Get(key string, offset, length int64) (io.ReadCloser, error) {
	payload, ok := m.values[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if offset < 0 || length < 0 || offset+length > int64(len(payload)) {
		return nil, io.ErrUnexpectedEOF
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func (m *memoryObjects) resetPublished() { m.publishedBytes = 0 }

type countingBlockUpdateReader struct {
	blocks []uint64
	values map[uint64][]byte
	reads  map[uint64]int
}

func (r *countingBlockUpdateReader) Blocks() ([]uint64, error) {
	return append([]uint64(nil), r.blocks...), nil
}

func (r *countingBlockUpdateReader) ReadBlock(block uint64, target []byte) (int, error) {
	payload, ok := r.values[block]
	if !ok {
		return 0, os.ErrNotExist
	}
	r.reads[block]++
	return copy(target, payload), nil
}
