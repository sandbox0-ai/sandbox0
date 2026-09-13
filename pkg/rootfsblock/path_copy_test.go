package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type pathOnlySource struct {
	RangeSource
	allowed map[string]bool
	reads   int
}

type canceledEditSource struct {
	RangeSource
	cancel context.CancelFunc
	calls  int
}

func (s *canceledEditSource) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.calls++
	if s.calls == 2 {
		s.cancel()
		return nil, ctx.Err()
	}
	return s.RangeSource.Get(key, offset, length)
}

type editRecordingPublisher struct {
	ImmutableObjectPublisher
	writes int
}

func (p *editRecordingPublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	p.writes++
	return p.ImmutableObjectPublisher.PutImmutable(ctx, key, payload)
}

func TestPathCopyPreparationCancellationDoesNotPublish(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch-%t", batch), func(t *testing.T) {
			store, base, _, _ := compressedFixture(t, 2)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := &canceledEditSource{RangeSource: store, cancel: cancel}
			publisher := &editRecordingPublisher{ImmutableObjectPublisher: store}
			updates := []BlockUpdate{{Block: 1, Data: bytes.Repeat([]byte{0x31}, LogicalBlockSize)}}
			var err error
			if batch {
				composite, _, buildErr := BuildCompositeGeneration(base.Descriptor, updates)
				require.NoError(t, buildErr)
				_, err = BuildIncrementalGenerationsBatch(ctx, source, []BatchIncrementalInput{{ID: "canceled", Descriptor: composite}}, publisher, BuildOptions{})
			} else {
				_, err = BuildIncrementalGeneration(ctx, source, base.Descriptor, updates, publisher, BuildOptions{})
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 2, source.calls)
			require.Zero(t, publisher.writes)
		})
	}
}

func TestPathCopyGenerationsPreserveSparseAndDenseImages(t *testing.T) {
	for _, version := range []int{DescriptorVersion, CompressedFormatVersion} {
		for _, fanout := range []int{2, 4} {
			t.Run(fmt.Sprintf("format-%d-fanout-%d", version, fanout), func(t *testing.T) {
				store := newMemoryObjects()
				expected := make([]byte, 128*LogicalBlockSize)
				for block := 3; block < 128; block += 7 {
					copy(expected[block*LogicalBlockSize:], bytes.Repeat([]byte{byte(block)}, LogicalBlockSize))
				}
				options := BuildOptions{FormatVersion: version, DataRangeBytes: 4 * LogicalBlockSize, PackBytes: 16 * LogicalBlockSize, PageEntries: fanout}
				base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(expected), int64(len(expected)), store, options)
				require.NoError(t, err)
				random := rand.New(rand.NewSource(23))
				for round := range 16 {
					blocks := random.Perm(128)[:8]
					updates := make([]BlockUpdate, 0, len(blocks))
					for index, block := range blocks {
						value := byte(round + index + 1)
						if index%3 == 0 {
							value = 0
						}
						data := bytes.Repeat([]byte{value}, LogicalBlockSize)
						updates = append(updates, BlockUpdate{Block: uint64(block), Data: data})
						copy(expected[block*LogicalBlockSize:], data)
					}
					if round%2 == 0 {
						base, err = BuildIncrementalGeneration(t.Context(), store, base.Descriptor, updates, store, options)
					} else {
						composite, _, buildErr := BuildCompositeGeneration(base.Descriptor, updates)
						require.NoError(t, buildErr)
						batch, buildErr := BuildIncrementalGenerationsBatch(t.Context(), store, []BatchIncrementalInput{{ID: "one", Descriptor: composite}}, store, options)
						require.NoError(t, buildErr)
						base = batch.Results["one"]
					}
					require.NoError(t, err)
					reader, err := NewReader(store, base.Descriptor, 0)
					require.NoError(t, err)
					actual := make([]byte, len(expected))
					_, err = reader.ReadAt(actual, 0)
					require.NoError(t, err)
					require.Equal(t, expected, actual, "round %d", round)
				}
				updates := make([]BlockUpdate, 128)
				for index := range updates {
					updates[index] = BlockUpdate{Block: uint64(index), Data: make([]byte, LogicalBlockSize)}
				}
				cleared, err := BuildIncrementalGeneration(t.Context(), store, base.Descriptor, updates, store, options)
				require.NoError(t, err)
				reader, err := NewReader(store, cleared.Descriptor, 0)
				require.NoError(t, err)
				require.Zero(t, reader.root.Level)
				require.Empty(t, reader.root.Entries)
			})
		}
	}
}

func TestPathCopyBatchSharesVerifiedMappingReads(t *testing.T) {
	store, base, _, _ := compressedFixture(t, 2)
	reader, err := NewReader(store, base.Descriptor, 0)
	require.NoError(t, err)
	allowed := map[string]bool{base.Descriptor.MappingRoot.Object.Key: true}
	page := reader.root
	for page.Level > 0 {
		entry, ok := page.entryFor(1)
		require.True(t, ok)
		allowed[entry.Object.Key] = true
		page, err = reader.readMappingPage(entry.Object)
		require.NoError(t, err)
	}
	source := &pathOnlySource{RangeSource: store, allowed: allowed}
	inputs := make([]BatchIncrementalInput, 64)
	for index := range inputs {
		composite, _, err := BuildCompositeGeneration(base.Descriptor, []BlockUpdate{{Block: 1, Data: bytes.Repeat([]byte{byte(index + 100)}, LogicalBlockSize)}})
		require.NoError(t, err)
		inputs[index] = BatchIncrementalInput{ID: fmt.Sprint(index), Descriptor: composite}
	}
	result, err := BuildIncrementalGenerationsBatch(t.Context(), source, inputs, store, BuildOptions{PageEntries: 2})
	require.NoError(t, err)
	require.Len(t, result.Results, len(inputs))
	require.Equal(t, len(allowed), source.reads, "one shared bounded cache, not one base mapping download per member")
	for index := range inputs {
		reader, err := NewReader(store, result.Results[fmt.Sprint(index)].Descriptor, 0)
		require.NoError(t, err)
		actual := make([]byte, LogicalBlockSize)
		_, err = reader.ReadAt(actual, LogicalBlockSize)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(index + 100)}, LogicalBlockSize), actual)
	}
}

func (s *pathOnlySource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	if !s.allowed[key] {
		return nil, fmt.Errorf("unaffected mapping or data object was read: %s", key)
	}
	s.reads++
	return s.RangeSource.Get(key, offset, length)
}

func TestIncrementalPublicationOnlyReadsChangedMappingPaths(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch-%t", batch), func(t *testing.T) {
			store, base, expected, _ := compressedFixture(t, 2)
			reader, err := NewReader(store, base.Descriptor, 0)
			require.NoError(t, err)
			allowed := map[string]bool{base.Descriptor.MappingRoot.Object.Key: true}
			page := reader.root
			for page.Level > 0 {
				entry, ok := page.entryFor(1)
				require.True(t, ok)
				allowed[entry.Object.Key] = true
				page, err = reader.readMappingPage(entry.Object)
				require.NoError(t, err)
			}
			source := &pathOnlySource{RangeSource: store, allowed: allowed}
			update := BlockUpdate{Block: 1, Data: bytes.Repeat([]byte{0xe1}, LogicalBlockSize)}
			var result BuildResult
			if batch {
				composite, _, err := BuildCompositeGeneration(base.Descriptor, []BlockUpdate{update})
				require.NoError(t, err)
				built, err := BuildIncrementalGenerationsBatch(t.Context(), source, []BatchIncrementalInput{{ID: "one", Descriptor: composite}}, store, BuildOptions{PageEntries: 2})
				require.NoError(t, err)
				result = built.Results["one"]
			} else {
				result, err = BuildIncrementalGeneration(t.Context(), source, base.Descriptor, []BlockUpdate{update}, store, BuildOptions{PageEntries: 2})
				require.NoError(t, err)
			}
			require.Equal(t, len(allowed), source.reads, "only one root-to-leaf path, no data downloads")
			copy(expected[LogicalBlockSize:], update.Data)
			next, err := NewReader(store, result.Descriptor, 0)
			require.NoError(t, err)
			actual := make([]byte, len(expected))
			_, err = next.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
