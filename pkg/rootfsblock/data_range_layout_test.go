package rootfsblock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataRangeLayoutCanonicalizationOwnsCompactSpans(t *testing.T) {
	const unit = CompressedDataRangeBytes
	input := []DataRangeSpan{{Start: unit + 4096, End: 3*unit + 4096}, {Start: 4096, End: unit + 4096}}
	before := append([]DataRangeSpan(nil), input...)
	layout, err := NewDataRangeLayout(4*unit, unit, input)
	require.NoError(t, err)
	require.Equal(t, before, input, "constructor must not sort or merge the caller's slice")
	require.Equal(t, []DataRangeSpan{{Start: 4096, End: 3*unit + 4096}}, layout.spans)
	input[0].End = 0
	require.Equal(t, int64(3*unit+4096), layout.spans[0].End)

	const size = int64(1 << 40)
	large, err := NewDataRangeLayout(size, unit, []DataRangeSpan{{Start: 4096, End: size - unit + 4096}})
	require.NoError(t, err)
	require.Len(t, large.spans, 1, "one near-1TiB extent must not allocate sixteen million range records")
	require.Equal(t, 1, cap(large.spans))
}

func TestDataRangeLayoutRejectsMalformedSpans(t *testing.T) {
	const unit = CompressedDataRangeBytes
	for name, spans := range map[string][]DataRangeSpan{
		"negative":       {{Start: -4096, End: unit - 4096}},
		"unaligned":      {{Start: 1, End: unit + 1}},
		"partial":        {{Start: 4096, End: 8192}},
		"empty":          {{Start: 4096, End: 4096}},
		"reversed":       {{Start: unit, End: 0}},
		"outside":        {{Start: 2 * unit, End: 3 * unit}},
		"duplicate":      {{Start: 0, End: unit}, {Start: 0, End: unit}},
		"overlap":        {{Start: 0, End: unit}, {Start: 4096, End: unit + 4096}},
		"overflowed-end": {{Start: math.MaxInt64 - unit, End: math.MinInt64 + unit}},
	} {
		t.Run(name, func(t *testing.T) { _, err := NewDataRangeLayout(2*unit, unit, spans); require.Error(t, err) })
	}
	for _, size := range []int64{-4096, 0, 4097} {
		_, err := NewDataRangeLayout(size, unit, nil)
		require.Error(t, err)
	}
	for _, unit := range []int{-4096, 0, 4097, 128 << 10} {
		_, err := NewDataRangeLayout(1<<20, unit, nil)
		require.Error(t, err)
	}
	_, err := NewDataRangeLayout(1<<40, unit, make([]DataRangeSpan, MaxDataRangeSpans+1))
	require.ErrorContains(t, err, "exceeds")
}

type layoutUnexpectedRead struct{ calls int }

func (r *layoutUnexpectedRead) ReadAt([]byte, int64) (int, error) {
	r.calls++
	return 0, fmt.Errorf("unexpected input read")
}

func TestDataRangeLayoutRejectsUnboundBuildBeforeIO(t *testing.T) {
	const unit = CompressedDataRangeBytes
	layout, err := NewDataRangeLayout(2*unit, unit, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		size    int64
		options BuildOptions
		layout  *DataRangeLayout
	}{
		{"nil", 2 * unit, BuildOptions{FormatVersion: 2}, nil},
		{"zero", 2 * unit, BuildOptions{FormatVersion: 2}, &DataRangeLayout{}},
		{"wrong-size", 3 * unit, BuildOptions{FormatVersion: 2}, layout},
		{"wrong-unit", 2 * unit, BuildOptions{FormatVersion: 2, DataRangeBytes: 4096}, layout},
		{"legacy", 2 * unit, BuildOptions{DataRangeBytes: unit}, layout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &layoutUnexpectedRead{}
			store := newBuildTestStore()
			_, err := BuildMaterializedGenerationWithLayout(t.Context(), reader, tc.size, store, tc.options, tc.layout)
			require.Error(t, err)
			require.Zero(t, reader.calls)
			require.Empty(t, store.objects)
		})
	}
}

func TestDataRangeLayoutEmptyPlanPreservesPublication(t *testing.T) {
	for _, unit := range []int{4096, 16 << 10, 64 << 10} {
		t.Run(fmt.Sprint(unit), func(t *testing.T) {
			data := make([]byte, 13*unit)
			_, err := rand.New(rand.NewSource(101)).Read(data)
			require.NoError(t, err)
			clear(data[3*unit : 8*unit])
			options := BuildOptions{FormatVersion: 2, DataRangeBytes: unit, PackBytes: 2 * unit, PageEntries: 2, ObjectPrefix: "same-identity"}
			baseline := newBuildTestStore()
			before, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(data), int64(len(data)), baseline, options)
			require.NoError(t, err)
			layout, err := NewDataRangeLayout(int64(len(data)), unit, nil)
			require.NoError(t, err)
			store := newBuildTestStore()
			after, err := BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(data), int64(len(data)), store, options, layout)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, baseline.objects, store.objects)
		})
	}
}

// This oracle explicitly enumerates boundaries only for small test images. The
// implementation must not expand an arbitrary-size image this way.
func layoutOracle(size, unit int64, spans []DataRangeSpan) []int64 {
	boundaries := []int64{0, size}
	for off := unit; off < size; off += unit {
		inside := false
		for _, s := range spans {
			inside = inside || off > s.Start && off < s.End
		}
		if !inside {
			boundaries = append(boundaries, off)
		}
	}
	for _, s := range spans {
		for off := s.Start; off <= s.End; off += unit {
			boundaries = append(boundaries, off)
		}
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i] < boundaries[j] })
	unique := boundaries[:0]
	for _, off := range boundaries {
		if len(unique) == 0 || unique[len(unique)-1] != off {
			unique = append(unique, off)
		}
	}
	return unique
}

func TestDataRangeLayoutRandomCoverageAndPublication(t *testing.T) {
	random := rand.New(rand.NewSource(1789))
	for trial := range 24 {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			unit := int64([]int{4096, 16 << 10, 64 << 10}[trial%3])
			size := unit * 24
			var spans []DataRangeSpan
			for off := int64(random.Intn(8)) * 4096; off+unit <= size; {
				end := min(off+int64(1+random.Intn(3))*unit, off+(size-off)/unit*unit)
				spans = append(spans, DataRangeSpan{Start: off, End: end})
				off = end + int64(1+random.Intn(9))*4096
			}
			plan, err := NewDataRangeLayout(size, int(unit), spans)
			require.NoError(t, err)
			oracle := layoutOracle(size, unit, spans)
			cursor := dataRangeCursor{layout: plan}
			for i := 0; i < len(oracle)-1; i++ {
				got := cursor.nextLength(oracle[i], size, int(unit))
				require.Equal(t, oracle[i+1]-oracle[i], got)
				require.Positive(t, got)
				require.LessOrEqual(t, got, unit)
			}
			data := make([]byte, size)
			_, err = random.Read(data)
			require.NoError(t, err)
			// Short zero residuals exercise subsequent growth of a reused input buffer.
			for off := 0; off < len(data); off += 7 * 4096 {
				clear(data[off:min(off+3*4096, len(data))])
			}
			store := newBuildTestStore()
			built, err := BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(data), size, store, BuildOptions{FormatVersion: 2, DataRangeBytes: int(unit), PackBytes: int(2 * unit), PageEntries: 2}, plan)
			require.NoError(t, err)
			r, err := NewReader(store, built.Descriptor, 0)
			require.NoError(t, err)
			actual := make([]byte, size)
			_, err = r.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, data, actual)
			for _, ref := range built.References {
				payload, ok := store.payload(ref.Key)
				require.True(t, ok)
				require.Equal(t, ref.Size, int64(len(payload)))
				if ref.Kind == ObjectKindDataPack {
					require.LessOrEqual(t, ref.Size, 2*unit)
				}
			}
		})
	}
}

func TestDataRangeCursorAvoidsOverflowNearInt64Limit(t *testing.T) {
	size := int64(math.MaxInt64 / 4096 * 4096)
	unit := int64(CompressedDataRangeBytes)
	plan, err := NewDataRangeLayout(size, int(unit), []DataRangeSpan{{Start: size - 8*unit, End: size}})
	require.NoError(t, err)
	cursor := dataRangeCursor{layout: plan}
	for off := size - 8*unit; off < size; {
		length := cursor.nextLength(off, size, int(unit))
		require.Equal(t, unit, length)
		off += length
	}
	empty, err := NewDataRangeLayout(size, int(unit), nil)
	require.NoError(t, err)
	cursor = dataRangeCursor{layout: empty}
	require.Equal(t, int64(4096), cursor.nextLength(size-4096, size, int(unit)))
}

func TestDataRangeLayoutConcurrentBuildsShareImmutablePlan(t *testing.T) {
	const unit = CompressedDataRangeBytes
	data := bytes.Repeat([]byte{0x42}, 9*unit)
	plan, err := NewDataRangeLayout(int64(len(data)), unit, []DataRangeSpan{{Start: 4096, End: 8*unit + 4096}})
	require.NoError(t, err)
	options := BuildOptions{FormatVersion: 2, PackBytes: 2 * unit, PageEntries: 2}
	baseline, err := BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(data), int64(len(data)), newBuildTestStore(), options, plan)
	require.NoError(t, err)
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			got, err := BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(data), int64(len(data)), newBuildTestStore(), options, plan)
			require.NoError(t, err)
			require.Equal(t, baseline, got)
		})
	}
}

func TestDataRangeLayoutPreservesCancellationAndShortReadErrors(t *testing.T) {
	const unit = CompressedDataRangeBytes
	plan, err := NewDataRangeLayout(2*unit, unit, []DataRangeSpan{{Start: 4096, End: unit + 4096}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader := &layoutUnexpectedRead{}
	store := newBuildTestStore()
	_, err = BuildMaterializedGenerationWithLayout(ctx, reader, 2*unit, store, BuildOptions{FormatVersion: 2}, plan)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, reader.calls)
	require.Empty(t, store.objects)
	_, err = BuildMaterializedGenerationWithLayout(t.Context(), bytes.NewReader(make([]byte, 4095)), 2*unit, store, BuildOptions{FormatVersion: 2}, plan)
	require.ErrorContains(t, err, "ended")
	require.Empty(t, store.objects)
	_, err = BuildMaterializedGenerationWithLayout(t.Context(), errorReadAt{}, 2*unit, store, BuildOptions{FormatVersion: 2}, plan)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

type errorReadAt struct{}

func (errorReadAt) ReadAt([]byte, int64) (int, error) { return 0, io.ErrUnexpectedEOF }
