package rootfsblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type sparseTestExtent struct{ start, end int64 }

func sparseTestSeeker(extents []sparseTestExtent, calls *int) func(int64, bool) (int64, error) {
	return func(offset int64, hole bool) (int64, error) {
		*calls++
		for _, extent := range extents {
			if offset < extent.end {
				if hole {
					if offset < extent.start {
						return offset, nil
					}
					return extent.end, nil
				}
				return max(offset, extent.start), nil
			}
		}
		return 0, io.EOF
	}
}

type sparseCountReader struct {
	io.ReaderAt
	bytes int64
	calls int
}

func (r *sparseCountReader) ReadAt(p []byte, offset int64) (int, error) {
	r.calls++
	n, err := r.ReaderAt.ReadAt(p, offset)
	r.bytes += int64(n)
	return n, err
}

func TestMaterializedSparsePublicationEqualsFullScan(t *testing.T) {
	const size = 19*CompressedDataRangeBytes + LogicalBlockSize
	rng := rand.New(rand.NewSource(88213))
	for iteration := 0; iteration < 24; iteration++ {
		payload := make([]byte, size)
		var extents []sparseTestExtent
		for start := 0; start < size; {
			start += rng.Intn(3 * CompressedDataRangeBytes)
			if start >= size {
				break
			}
			end := min(size, start+1+rng.Intn(CompressedDataRangeBytes))
			// Some allocated data extents contain only zeros. They must still
			// pass through the canonical all-zero and packing logic.
			if iteration%3 != 0 {
				_, err := rng.Read(payload[start:end])
				require.NoError(t, err)
			}
			extents = append(extents, sparseTestExtent{int64(start), int64(end)})
			start = end
		}
		for _, version := range []int{0, 2} {
			for _, shifted := range []bool{false, true} {
				if version == 0 && shifted {
					continue
				}
				t.Run(fmt.Sprintf("iteration-%d/version-%d/shifted-%t", iteration, version, shifted), func(t *testing.T) {
					options := BuildOptions{FormatVersion: version, DataRangeBytes: CompressedDataRangeBytes, PackBytes: 3 * CompressedDataRangeBytes, PageEntries: 3}
					var layout *DataRangeLayout
					if shifted {
						var err error
						layout, err = NewDataRangeLayout(size, CompressedDataRangeBytes, []DataRangeSpan{
							{LogicalBlockSize, LogicalBlockSize + 3*CompressedDataRangeBytes},
							{7*CompressedDataRangeBytes + 2*LogicalBlockSize, 11*CompressedDataRangeBytes + 2*LogicalBlockSize},
						})
						require.NoError(t, err)
					}
					baseline, candidate := newBuildTestStore(), newBuildTestStore()
					want, err := buildMaterializedGeneration(t.Context(), bytes.NewReader(payload), size, baseline, options, layout)
					require.NoError(t, err)
					input := &sparseCountReader{ReaderAt: bytes.NewReader(payload)}
					calls := 0
					reader := &materializedFileReader{ReaderAt: input, size: size, seek: sparseTestSeeker(extents, &calls)}
					got, err := buildMaterializedGeneration(t.Context(), reader, size, candidate, options, layout)
					require.NoError(t, err)
					require.Equal(t, want, got, "descriptor, inventory, sizes and counts are canonical")
					require.Equal(t, baseline.objects, candidate.objects, "every immutable object is byte-identical")
					require.LessOrEqual(t, input.bytes, int64(size))
					require.LessOrEqual(t, calls, 2*len(extents)+1, "discovery retains one extent, not one record per data unit")
					view, err := NewReader(candidate, got.Descriptor, 0)
					require.NoError(t, err)
					actual := make([]byte, size)
					_, err = view.ReadAt(actual, 0)
					require.NoError(t, err)
					require.Equal(t, payload, actual)
				})
			}
		}
	}
}

func TestMaterializedSparseFallbackAndFailures(t *testing.T) {
	const size = 3 * CompressedDataRangeBytes
	payload := bytes.Repeat([]byte{7}, size)
	sentinel := errors.New("injected source failure")
	for _, tc := range []struct {
		name string
		seek func(int64, bool) (int64, error)
		ok   bool
	}{
		{"unsupported-data", func(int64, bool) (int64, error) { return 0, errMaterializedSeekUnsupported }, true},
		{"unsupported-hole", func(offset int64, hole bool) (int64, error) {
			if hole {
				return 0, errMaterializedSeekUnsupported
			}
			return offset, nil
		}, true},
		{"conservative-all-data", func(offset int64, hole bool) (int64, error) {
			if hole {
				return size, nil
			}
			return offset, nil
		}, true},
		{"data-io-error", func(int64, bool) (int64, error) { return 0, sentinel }, false},
		{"data-negative", func(int64, bool) (int64, error) { return -1, nil }, false},
		{"data-at-end", func(int64, bool) (int64, error) { return size, nil }, false},
		{"data-overflow", func(int64, bool) (int64, error) { return math.MaxInt64, nil }, false},
		{"hole-io-error", func(offset int64, hole bool) (int64, error) {
			if hole {
				return 0, sentinel
			}
			return offset, nil
		}, false},
		{"hole-eof-is-not-a-zero-tail", func(offset int64, hole bool) (int64, error) {
			if hole {
				return 0, io.EOF
			}
			return offset, nil
		}, false},
		{"hole-no-progress", func(offset int64, hole bool) (int64, error) { return offset, nil }, false},
		{"hole-outside", func(offset int64, hole bool) (int64, error) {
			if hole {
				return size + 1, nil
			}
			return offset, nil
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plain, sparse := newBuildTestStore(), newBuildTestStore()
			options := BuildOptions{FormatVersion: 2}
			want, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(payload), size, plain, options)
			require.NoError(t, err)
			input := &sparseCountReader{ReaderAt: bytes.NewReader(payload)}
			reader := &materializedFileReader{ReaderAt: input, size: size, seek: tc.seek}
			got, err := BuildMaterializedGeneration(t.Context(), reader, size, sparse, options)
			if tc.ok {
				require.NoError(t, err)
				require.Equal(t, want, got)
				require.Equal(t, plain.objects, sparse.objects)
				require.Equal(t, int64(size), input.bytes)
			} else {
				require.Error(t, err)
				require.Empty(t, got.Payload)
				require.Empty(t, sparse.objects)
				require.Zero(t, input.bytes)
			}
		})
	}
	for _, failure := range []string{"cancel-before-seek", "cancel-during-seek", "short-read", "publish"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			count := 0
			seek := sparseTestSeeker([]sparseTestExtent{{0, size}}, &count)
			input := bytes.NewReader(payload)
			publisher := newBuildTestStore()
			switch failure {
			case "cancel-before-seek":
				cancel()
			case "cancel-during-seek":
				seek = func(int64, bool) (int64, error) { cancel(); return 0, io.EOF }
			case "short-read":
				input = bytes.NewReader(payload[:10])
			case "publish":
				publisher.conflict = true
			}
			got, err := BuildMaterializedGeneration(ctx, &materializedFileReader{ReaderAt: input, size: size, seek: seek}, size, publisher, BuildOptions{FormatVersion: 2})
			require.Error(t, err)
			require.Empty(t, got.Payload)
			if failure == "cancel-before-seek" {
				require.Zero(t, count)
			}
		})
	}
}

func TestDataRangeSparseJumpMatchesSequentialSegmentation(t *testing.T) {
	const size = 23*CompressedDataRangeBytes + 3*LogicalBlockSize
	layout, err := NewDataRangeLayout(size, CompressedDataRangeBytes, []DataRangeSpan{
		{LogicalBlockSize, 5*CompressedDataRangeBytes + LogicalBlockSize},
		{9*CompressedDataRangeBytes + 2*LogicalBlockSize, 14*CompressedDataRangeBytes + 2*LogicalBlockSize},
	})
	require.NoError(t, err)
	for _, plan := range []*DataRangeLayout{nil, layout} {
		sequential := dataRangeCursor{layout: plan}
		for start := int64(0); start < size; {
			length := sequential.nextLength(start, size, CompressedDataRangeBytes)
			for _, offset := range []int64{start, start + length/2, start + length - 1} {
				jump := dataRangeCursor{layout: plan}
				require.Equal(t, start, jump.rangeStart(offset, CompressedDataRangeBytes))
				require.Equal(t, length, jump.nextLength(start, size, CompressedDataRangeBytes))
			}
			start += length
		}
	}
	const large = math.MaxInt64 / LogicalBlockSize * LogicalBlockSize
	nearEnd := int64(large - 3*CompressedDataRangeBytes + LogicalBlockSize)
	plan, err := NewDataRangeLayout(large, CompressedDataRangeBytes, []DataRangeSpan{{nearEnd, nearEnd + CompressedDataRangeBytes}})
	require.NoError(t, err)
	cursor := dataRangeCursor{layout: plan}
	require.Equal(t, nearEnd, cursor.rangeStart(nearEnd+1, CompressedDataRangeBytes))
	start := cursor.rangeStart(large-1, CompressedDataRangeBytes)
	require.LessOrEqual(t, start, int64(large-1))
	require.Equal(t, int64(large), start+cursor.nextLength(start, large, CompressedDataRangeBytes))
}
