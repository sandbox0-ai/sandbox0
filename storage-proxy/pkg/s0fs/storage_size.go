package s0fs

// StateStorageBytes returns the S3-backed payload bytes referenced by a state.
// Inline data is included because it becomes object storage on the next materialization.
func StateStorageBytes(state *SnapshotState) int64 {
	if state == nil {
		return 0
	}
	var size int64
	for _, payload := range state.Data {
		size += int64(len(payload))
	}
	seenSegments := make(map[string]struct{})
	for _, extents := range state.ColdFiles {
		for _, extent := range extents {
			if extent.SegmentID == "" {
				continue
			}
			if _, seen := seenSegments[extent.SegmentID]; seen {
				continue
			}
			seenSegments[extent.SegmentID] = struct{}{}
			if segment := state.Segments[extent.SegmentID]; segment != nil {
				size += int64(segment.Length)
				continue
			}
			size += int64(extent.Length)
		}
	}
	return size
}
