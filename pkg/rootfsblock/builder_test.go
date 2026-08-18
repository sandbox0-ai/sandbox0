package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildMaterializedGenerationRoundTripAndSkipsZeroRanges(t *testing.T) {
	logical := append(bytes.Repeat([]byte{0x11}, LogicalBlockSize), make([]byte, LogicalBlockSize)...)
	logical = append(logical, bytes.Repeat([]byte{0x22}, 2*LogicalBlockSize)...)
	store := newBuildTestStore()
	result, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(logical), int64(len(logical)), store, BuildOptions{
		DataRangeBytes: LogicalBlockSize, PackBytes: 2 * LogicalBlockSize, PageEntries: 4,
	})
	require.NoError(t, err)
	require.NoError(t, result.Descriptor.Validate())
	require.NotEmpty(t, result.Payload)
	require.Equal(t, 3, result.Objects)

	reader, err := NewReader(store, result.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(logical))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, logical, actual)
	require.Equal(t, 2, store.packObjects())
}

func TestBuildMaterializedGenerationCreatesMultiLevelTree(t *testing.T) {
	logical := bytes.Repeat([]byte{0x31}, 5*LogicalBlockSize)
	store := newBuildTestStore()
	result, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(logical), int64(len(logical)), store, BuildOptions{
		DataRangeBytes: LogicalBlockSize, PackBytes: LogicalBlockSize, PageEntries: 2,
	})
	require.NoError(t, err)
	reader, err := NewReader(store, result.Descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(logical))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, logical, actual)
}

func TestBuildMaterializedGenerationRejectsShortInputAndConflictingObject(t *testing.T) {
	store := newBuildTestStore()
	_, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(make([]byte, LogicalBlockSize-1)), LogicalBlockSize, store, BuildOptions{
		DataRangeBytes: LogicalBlockSize, PackBytes: LogicalBlockSize,
	})
	require.ErrorContains(t, err, "ended")

	store.conflict = true
	_, err = BuildMaterializedGeneration(t.Context(), bytes.NewReader(bytes.Repeat([]byte{1}, LogicalBlockSize)), LogicalBlockSize, store, BuildOptions{
		DataRangeBytes: LogicalBlockSize, PackBytes: LogicalBlockSize,
	})
	require.ErrorContains(t, err, "immutable object conflict")
}

type buildTestStore struct {
	mu       sync.Mutex
	objects  map[string][]byte
	conflict bool
}

func newBuildTestStore() *buildTestStore { return &buildTestStore{objects: make(map[string][]byte)} }

func (s *buildTestStore) PutImmutable(_ context.Context, key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conflict {
		return fmt.Errorf("immutable object conflict")
	}
	if existing, ok := s.objects[key]; ok && !bytes.Equal(existing, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	s.objects[key] = append([]byte(nil), payload...)
	return nil
}

func (s *buildTestStore) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.objects[key]
	if !ok || offset < 0 || length < 0 || offset+length > int64(len(payload)) {
		return nil, fmt.Errorf("range not found")
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func (s *buildTestStore) packObjects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for key := range s.objects {
		if strings.Contains(key, "/packs/") {
			count++
		}
	}
	return count
}
