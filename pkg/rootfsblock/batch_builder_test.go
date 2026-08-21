package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildIncrementalGenerationsBatchSharesDataAndMappingPacks(t *testing.T) {
	objects := newBatchTestObjects()
	base, err := BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, 4*LogicalBlockSize)),
		4*LogicalBlockSize, objects, BuildOptions{},
	)
	require.NoError(t, err)
	inputs := make([]BatchIncrementalInput, 1000)
	expected := make(map[string][]byte, len(inputs))
	for index := range inputs {
		value := bytes.Repeat([]byte{byte(index%251 + 1)}, LogicalBlockSize)
		_, payload, buildErr := BuildCompositeGeneration(base.Descriptor, []BlockUpdate{{
			Sequence: 1, Block: uint64(index % 4), Data: value,
		}})
		require.NoError(t, buildErr)
		id := fmt.Sprintf("generation-%04d", index)
		inputs[index] = BatchIncrementalInput{ID: id, Descriptor: mustDecodeBatchDescriptor(t, payload)}
		image := make([]byte, 4*LogicalBlockSize)
		copy(image[(index%4)*LogicalBlockSize:], value)
		expected[id] = image
	}

	before := objects.count()
	result, err := BuildIncrementalGenerationsBatch(
		t.Context(), objects, inputs, objects,
		BuildOptions{DataRangeBytes: LogicalBlockSize, PackBytes: 8 << 20},
	)
	require.NoError(t, err)
	require.Len(t, result.Results, len(inputs))
	require.Equal(t, 2, result.Objects, "1000 tiny generations should share one data and one mapping pack")
	require.Equal(t, before+2, objects.count())
	for id, built := range result.Results {
		reader, readErr := NewReader(objects, built.Descriptor, DefaultReadCacheBytes)
		require.NoError(t, readErr)
		actual := make([]byte, 4*LogicalBlockSize)
		_, readErr = reader.ReadAt(actual, 0)
		require.NoError(t, readErr)
		require.Equal(t, expected[id], actual)
		require.Len(t, built.References, 2)
	}
}

func TestBuildIncrementalGenerationsBatchIsDeterministicAndPackBounded(t *testing.T) {
	objects := newBatchTestObjects()
	base, err := BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, LogicalBlockSize)),
		LogicalBlockSize, objects, BuildOptions{},
	)
	require.NoError(t, err)
	inputs := make([]BatchIncrementalInput, 17)
	for index := range inputs {
		_, payload, buildErr := BuildCompositeGeneration(base.Descriptor, []BlockUpdate{{
			Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{byte(index + 1)}, LogicalBlockSize),
		}})
		require.NoError(t, buildErr)
		inputs[index] = BatchIncrementalInput{
			ID: fmt.Sprintf("generation-%02d", index), Descriptor: mustDecodeBatchDescriptor(t, payload),
		}
	}
	options := BuildOptions{DataRangeBytes: LogicalBlockSize, PackBytes: 8 * LogicalBlockSize}
	first, err := BuildIncrementalGenerationsBatch(t.Context(), objects, inputs, objects, options)
	require.NoError(t, err)
	firstCount := objects.count()
	second, err := BuildIncrementalGenerationsBatch(t.Context(), objects, inputs, objects, options)
	require.NoError(t, err)
	require.Equal(t, first.References, second.References)
	require.Equal(t, firstCount, objects.count(), "an exact retry must not create new content-addressed objects")
	for id := range first.Results {
		require.Equal(t, first.Results[id].Payload, second.Results[id].Payload)
	}
	for _, size := range objects.sizes() {
		require.LessOrEqual(t, size, options.PackBytes)
	}
}

func TestBuildIncrementalGenerationsBatchRejectsInvalidInputsBeforePublishing(t *testing.T) {
	objects := newBatchTestObjects()
	_, err := BuildIncrementalGenerationsBatch(
		t.Context(), objects,
		[]BatchIncrementalInput{{ID: "same"}, {ID: "same"}},
		objects, BuildOptions{},
	)
	require.Error(t, err)
	require.Zero(t, objects.count())
}

func mustDecodeBatchDescriptor(t *testing.T, payload []byte) Descriptor {
	t.Helper()
	descriptor, err := DecodeDescriptor(payload)
	require.NoError(t, err)
	return descriptor
}

type batchTestObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newBatchTestObjects() *batchTestObjects {
	return &batchTestObjects{objects: make(map[string][]byte)}
}

func (s *batchTestObjects) PutImmutable(_ context.Context, key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, found := s.objects[key]; found && !bytes.Equal(current, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	s.objects[key] = append([]byte(nil), payload...)
	return nil
}

func (s *batchTestObjects) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, found := s.objects[key]
	if !found || offset < 0 || length < 0 || offset > int64(len(payload))-length {
		return nil, fmt.Errorf("object range not found")
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func (s *batchTestObjects) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

func (s *batchTestObjects) sizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]int, 0, len(s.objects))
	for _, payload := range s.objects {
		result = append(result, len(payload))
	}
	return result
}
