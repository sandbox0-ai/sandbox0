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
