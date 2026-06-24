package s0fs

import "fmt"

func cloneExtents(extents []FileExtent) []FileExtent {
	if len(extents) == 0 {
		return nil
	}
	return append([]FileExtent(nil), extents...)
}

func sliceExtents(extents []FileExtent, start, end uint64) []FileExtent {
	if end <= start || len(extents) == 0 {
		return nil
	}
	var out []FileExtent
	fileOffset := uint64(0)
	for _, extent := range extents {
		extentStart := fileOffset
		extentEnd := fileOffset + extent.Length
		fileOffset = extentEnd
		if end <= extentStart || start >= extentEnd {
			continue
		}
		readStart := maxUint64(start, extentStart)
		readEnd := minUint64(end, extentEnd)
		cloned := extent
		if cloned.SegmentID != "" {
			cloned.Offset += readStart - extentStart
		} else {
			cloned.Offset = 0
		}
		cloned.Length = readEnd - readStart
		out = append(out, cloned)
	}
	return coalesceExtents(out)
}

func coalesceExtents(extents []FileExtent) []FileExtent {
	if len(extents) == 0 {
		return nil
	}
	out := make([]FileExtent, 0, len(extents))
	for _, extent := range extents {
		if extent.Length == 0 {
			continue
		}
		if len(out) == 0 {
			out = append(out, extent)
			continue
		}
		last := &out[len(out)-1]
		if last.SegmentID == "" && extent.SegmentID == "" {
			last.Length += extent.Length
			continue
		}
		if last.SegmentID == extent.SegmentID && last.Offset+last.Length == extent.Offset {
			last.Length += extent.Length
			continue
		}
		out = append(out, extent)
	}
	return out
}

func isInlineSegment(segment *Segment) bool {
	return segment != nil && segment.Key == "" && len(segment.InlineData) > 0
}

func inlineSegmentRange(segment *Segment, off, length uint64) ([]byte, error) {
	if !isInlineSegment(segment) {
		return nil, fmt.Errorf("%w: segment %s is not inline", ErrInvalidInput, segmentIDForError(segment))
	}
	end := off + length
	if end > uint64(len(segment.InlineData)) {
		return nil, fmt.Errorf("%w: inline segment %s range exceeds payload", ErrInvalidInput, segment.ID)
	}
	return segment.InlineData[int(off):int(end)], nil
}

func segmentIDForError(segment *Segment) string {
	if segment == nil {
		return "<nil>"
	}
	return segment.ID
}
