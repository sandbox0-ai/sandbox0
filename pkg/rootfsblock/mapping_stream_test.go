package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestMappingStreamBoundedFrontier(t *testing.T) {
	for _, version := range []int{0, CompressedFormatVersion} {
		for _, fanout := range []int{2, 31, DefaultPageEntries} {
			t.Run(fmt.Sprintf("version-%d/fanout-%d", version, fanout), func(t *testing.T) {
				exerciseMappingStreamBound(t, version, fanout, max(32769, fanout*fanout+fanout+1))
			})
		}
	}
}

func TestMappingStreamOneTiBFullyMappedModel(t *testing.T) {
	if os.Getenv("S0_ROOTFS_STREAM_SCALE") != "1" {
		t.Skip("set S0_ROOTFS_STREAM_SCALE=1 for sixteen million generated extents; not a populated image or startup test")
	}
	exerciseMappingStreamBound(t, CompressedFormatVersion, DefaultPageEntries, 1<<24)
}

// The model publishes real mapping pages, but reuses one synthetic data
// locator. It tests the streaming frontier and object inventory, not 1TiB of
// distinct data, whole-image I/O, node RSS, or sandbox startup latency.
func exerciseMappingStreamBound(t *testing.T, version, fanout, count int) {
	t.Helper()
	options, err := NormalizeBuildOptions(BuildOptions{FormatVersion: version, PageEntries: fanout})
	require.NoError(t, err)
	builder := generationBuilder{ctx: t.Context(), publisher: streamDiscardPublisher{}, options: options}
	defer builder.encoder.close()
	const blocksPerEntry = CompressedDataRangeBytes / LogicalBlockSize
	total := uint64(count) * blocksPerEntry
	stream := newMappingStream(&builder, total)
	object := ObjectRange{Key: "rootfs/model/packs/repeated", Length: CompressedDataRangeBytes, Checksum: digest.FromBytes(bytes.Repeat([]byte{3}, CompressedDataRangeBytes)).String()}
	peak := 0
	inspect := func() {
		t.Helper()
		retained := len(stream.leaf)
		require.LessOrEqual(t, cap(stream.leaf), fanout)
		for _, pages := range stream.levels {
			require.LessOrEqual(t, cap(pages), fanout)
			retained += len(pages)
			for _, page := range pages {
				require.Nil(t, page.payload, "published mapping bytes must not be retained in the frontier")
			}
		}
		peak = max(peak, retained)
		require.LessOrEqual(t, retained, fanout*(len(stream.levels)+1))
	}
	for index := 0; index < count; index++ {
		if err := stream.add(MappingEntry{LogicalStart: uint64(index) * blocksPerEntry, BlockCount: blocksPerEntry, Kind: MappingEntryData, Object: object}); err != nil {
			t.Fatal(err)
		}
		if index%fanout == fanout-1 {
			inspect()
		}
	}
	inspect()
	root, payload, err := stream.finish()
	require.NoError(t, err)
	page, err := DecodeMappingPage(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), page.StartBlock)
	require.Equal(t, total, page.BlockCount)
	require.Equal(t, digest.FromBytes(payload).String(), root.Checksum)
	expectedPages, width := 0, count
	for width > fanout {
		width = (width + fanout - 1) / fanout
		expectedPages += width
	}
	expectedPages++
	require.Equal(t, expectedPages, builder.objects)
	require.Len(t, builder.references, expectedPages)
	t.Logf("logical_model_bytes=%d entries=%d fanout=%d root_level=%d peak_observed_frontier_entries=%d mapping_objects=%d inventory_entries=%d", int64(total)*LogicalBlockSize, count, fanout, page.Level, peak, builder.objects, len(builder.references))
}

func TestMappingStreamRejectsInvalidOrderAndReuse(t *testing.T) {
	options, err := NormalizeBuildOptions(BuildOptions{PageEntries: 2})
	require.NoError(t, err)
	builder := generationBuilder{ctx: t.Context(), publisher: streamDiscardPublisher{}, options: options}
	stream := newMappingStream(&builder, 8)
	entry := MappingEntry{LogicalStart: 2, BlockCount: 1, Kind: MappingEntryData, Object: ObjectRange{Key: "packs/data", Length: LogicalBlockSize, Checksum: digest.FromBytes(make([]byte, LogicalBlockSize)).String()}}
	require.NoError(t, stream.add(entry))
	require.ErrorContains(t, stream.add(entry), "unordered")
	bad := entry
	bad.LogicalStart = 8
	require.ErrorContains(t, stream.add(bad), "outside")
	bad.LogicalStart = 3
	bad.BlockCount = 0
	require.ErrorContains(t, stream.add(bad), "outside")
	_, _, err = stream.finish()
	require.NoError(t, err)
	require.ErrorContains(t, stream.add(entry), "finished")
	_, _, err = stream.finish()
	require.ErrorContains(t, err, "finished")
}

func TestMaterializedCancellationAfterFirstMapping(t *testing.T) {
	for _, version := range []int{0, CompressedFormatVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			const size = 40 * LogicalBlockSize
			input := &streamPatternReader{size: size}
			publisher := &streamInspectPublisher{store: newBuildTestStore(), onMap: func() error { cancel(); return nil }}
			result, err := BuildMaterializedGeneration(ctx, input, size, publisher, BuildOptions{FormatVersion: version, DataRangeBytes: LogicalBlockSize, PackBytes: 4 * LogicalBlockSize, PageEntries: 2})
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, result.Payload)
			require.Equal(t, 1, publisher.maps)
			require.Less(t, input.readBytes, int64(size))
		})
	}
}

func TestMaterializedZeroRangesReuseInputBuffer(t *testing.T) {
	for _, size := range []int64{LogicalBlockSize, 1024 * LogicalBlockSize, 1024*LogicalBlockSize + LogicalBlockSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			input := &zeroBufferObserver{streamPatternReader: streamPatternReader{size: size, zero: true}}
			_, err := BuildMaterializedGeneration(t.Context(), input, size, streamDiscardPublisher{}, BuildOptions{DataRangeBytes: 4 * LogicalBlockSize})
			require.NoError(t, err)
			require.Equal(t, size, input.readBytes)
			require.NotNil(t, input.first)
			require.False(t, input.changed, "every zero-range read, including a short final range, must reuse the input buffer")
		})
	}
}

// Buffer identity checks the actual ownership invariant without counting the
// race detector's or runtime's unrelated allocations during a longer scan.
type zeroBufferObserver struct {
	streamPatternReader
	first   *byte
	changed bool
}

func (r *zeroBufferObserver) ReadAt(dst []byte, offset int64) (int, error) {
	if len(dst) > 0 {
		if r.first == nil {
			r.first = &dst[0]
		} else if r.first != &dst[0] {
			r.changed = true
		}
	}
	return r.streamPatternReader.ReadAt(dst, offset)
}

func TestMaterializedConcurrentCodecOwnership(t *testing.T) {
	const width = 8
	payload := bytes.Repeat([]byte{7}, 4<<20)
	options := BuildOptions{FormatVersion: CompressedFormatVersion, PackBytes: 1 << 20, PageEntries: 8}
	results := make([]BuildResult, width)
	errors := make([]error, width)
	var work sync.WaitGroup
	for index := range width {
		work.Add(1)
		go func() {
			defer work.Done()
			results[index], errors[index] = BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), int64(len(payload)), streamDiscardPublisher{}, options)
		}()
	}
	work.Wait()
	for index := range width {
		require.NoError(t, errors[index])
		require.Equal(t, results[0], results[index])
	}
}

func TestRangeEncoderReuseIsIndependentAndReleasesCodec(t *testing.T) {
	var encoder rangeEncoder
	defer encoder.close()
	for _, size := range []int{LogicalBlockSize, CompressedDataRangeBytes, 3 * CompressedDataRangeBytes, LogicalBlockSize} {
		input := make([]byte, size)
		for i := range input {
			input[i] = byte((uint32(i) * 2654435761) >> 24)
		}
		got, object, err := encoder.encode(t.Context(), input)
		require.NoError(t, err)
		codec := encoder.codec
		want, wantObject, err := encodeRangePayload(t.Context(), input)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, wantObject, object)
		object.Key = "packs/codec-reuse"
		decoded, err := decodeRangePayload(t.Context(), object, got)
		require.NoError(t, err)
		require.Equal(t, input, decoded)
		_, _, err = encoder.encode(t.Context(), input)
		require.NoError(t, err)
		require.Same(t, codec, encoder.codec)
	}
	encoder.close()
	require.Nil(t, encoder.codec)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := encoder.encode(ctx, []byte{1})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, encoder.codec)
}
