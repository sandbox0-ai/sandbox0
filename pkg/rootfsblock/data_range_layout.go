package rootfsblock

import (
	"context"
	"fmt"
	"io"
	"sort"
)

// MaxDataRangeSpans bounds one layout's retained span storage to 16MiB, separate
// from the caller's input and the builder's pack, codec and inventory budgets.
const MaxDataRangeSpans = 1 << 20

// DataRangeSpan prefers complete data units beginning at Start and continuing
// to End. A span represents an arbitrary number of adjacent units; its device
// start must be block-aligned but need not be aligned to the data-unit size.
// Spans are publication hints over immutable image bytes, not filesystem truth.
type DataRangeSpan struct {
	Start int64
	End   int64
}

// DataRangeLayout is an immutable, size-bound segmentation plan. Each build
// owns its cursor, so one plan can be reused by independent builders. The
// layout deliberately does not own file paths, RootFS identity or a read cache.
type DataRangeLayout struct {
	logicalSize int64
	rangeBytes  int64
	spans       []DataRangeSpan
}

// NewDataRangeLayout copies and canonicalizes complete preferred spans. It
// rejects overlap, including duplicate spans: a filesystem planner must resolve
// hardlinks/shared extents before handing physical ownership to this layer.
func NewDataRangeLayout(logicalSize int64, rangeBytes int, spans []DataRangeSpan) (*DataRangeLayout, error) {
	if logicalSize <= 0 || logicalSize%LogicalBlockSize != 0 {
		return nil, fmt.Errorf("data range layout size must be a positive block multiple")
	}
	if rangeBytes <= 0 || rangeBytes > CompressedDataRangeBytes || rangeBytes%LogicalBlockSize != 0 {
		return nil, fmt.Errorf("data range layout unit must be block-aligned and no greater than %d", CompressedDataRangeBytes)
	}
	if len(spans) > MaxDataRangeSpans {
		return nil, fmt.Errorf("data range layout exceeds %d spans", MaxDataRangeSpans)
	}
	owned := append([]DataRangeSpan(nil), spans...)
	sort.Slice(owned, func(i, j int) bool { return owned[i].Start < owned[j].Start })
	canonical := owned[:0]
	for _, span := range owned {
		if span.Start < 0 || span.Start%LogicalBlockSize != 0 || span.End <= span.Start ||
			span.End > logicalSize || (span.End-span.Start)%int64(rangeBytes) != 0 {
			return nil, fmt.Errorf("data range layout span is unaligned, incomplete or outside the image")
		}
		if len(canonical) > 0 {
			previous := &canonical[len(canonical)-1]
			if span.Start < previous.End {
				return nil, fmt.Errorf("data range layout spans overlap")
			}
			if span.Start == previous.End {
				previous.End = span.End
				continue
			}
		}
		canonical = append(canonical, span)
	}
	return &DataRangeLayout{logicalSize: logicalSize, rangeBytes: int64(rangeBytes), spans: canonical}, nil
}

// BuildMaterializedGenerationWithLayout uses the existing streamed publisher
// with explicit format-two segmentation. Residual bytes return to the global
// grid, and every initial mapping still names a complete raw/encoded payload.
// Callers must bind the segmentation policy to their durable import operation
// and attestation before using this entry point for ready-artifact publication.
// The ordinary builder and importer do not implicitly select this policy.
func BuildMaterializedGenerationWithLayout(
	ctx context.Context,
	reader io.ReaderAt,
	logicalSize int64,
	publisher ImmutableObjectPublisher,
	options BuildOptions,
	layout *DataRangeLayout,
) (BuildResult, error) {
	if layout == nil {
		return BuildResult{}, fmt.Errorf("explicit data range layout is required")
	}
	return buildMaterializedGeneration(ctx, reader, logicalSize, publisher, options, layout)
}

type dataRangeCursor struct {
	layout *DataRangeLayout
	index  int
}

// rangeStart finds the canonical unit containing a monotonically increasing
// byte offset, without walking or materializing units inside a sparse hole.
func (c *dataRangeCursor) rangeStart(offset int64, rangeBytes int) int64 {
	unit := int64(rangeBytes)
	start := offset - offset%unit
	if c.layout == nil {
		return start
	}
	spans := c.layout.spans
	for c.index < len(spans) && spans[c.index].End <= offset {
		c.index++
	}
	if c.index < len(spans) && offset >= spans[c.index].Start {
		return offset - (offset-spans[c.index].Start)%unit
	}
	if c.index > 0 {
		start = max(start, spans[c.index-1].End)
	}
	return start
}

// nextLength handles sequential offsets in O(ranges + spans), without expanding
// a large file span into per-block records or retaining mutable state in a plan.
func (c *dataRangeCursor) nextLength(offset, logicalSize int64, rangeBytes int) int64 {
	unit := int64(rangeBytes)
	length := min(unit, logicalSize-offset)
	if c.layout == nil {
		return length
	}
	length = min(length, unit-offset%unit)
	spans := c.layout.spans
	for c.index < len(spans) && spans[c.index].End <= offset {
		c.index++
	}
	if c.index < len(spans) {
		span := spans[c.index]
		if offset < span.Start {
			length = min(length, span.Start-offset)
		} else {
			length = min(logicalSize-offset, unit-(offset-span.Start)%unit)
		}
	}
	return length
}
