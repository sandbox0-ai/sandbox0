package s0fs

import (
	"fmt"
	"math"
)

// InitialStateObjectCount is the root node plus its directory-entry map.
const InitialStateObjectCount int64 = 2

// StorageUsage is the quota-relevant logical payload and metadata cardinality
// of an S0FS state.
type StorageUsage struct {
	Bytes   int64
	Objects int64
}

// StateStorageBytes returns the S3-backed payload bytes referenced by a state.
// Inline data is included because it becomes object storage on the next materialization.
func StateStorageBytes(state *SnapshotState) int64 {
	if state == nil {
		return 0
	}
	var size int64
	for _, payload := range state.Data {
		size = saturatingStorageByteAdd(size, uint64(len(payload)))
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
				size = saturatingStorageByteAdd(size, segment.Length)
				continue
			}
			size = saturatingStorageByteAdd(size, extent.Length)
		}
	}
	return size
}

// saturatingStorageByteAdd prevents corrupted or adversarial manifests from
// wrapping a logical byte count below the physical value. Saturation makes
// quota admission conservatively reject the object at any smaller limit.
func saturatingStorageByteAdd(current int64, value uint64) int64 {
	if current == math.MaxInt64 || value > uint64(math.MaxInt64-current) {
		return math.MaxInt64
	}
	return current + int64(value)
}

// StateObjectCount returns the number of independently growing state records.
// It counts both outer collection records and their entries so a team cannot
// exhaust memory or serialized manifests with empty files, directories,
// hardlinks, fragmented extents, or unreferenced segment descriptors.
func StateObjectCount(state *SnapshotState) (int64, error) {
	if state == nil {
		return 0, nil
	}
	var count int64
	add := func(value int) error {
		if value < 0 || int64(value) > math.MaxInt64-count {
			return fmt.Errorf("%w: s0fs object count overflow", ErrInvalidInput)
		}
		count += int64(value)
		return nil
	}
	if err := add(len(state.Nodes)); err != nil {
		return 0, err
	}
	if err := add(len(state.Children)); err != nil {
		return 0, err
	}
	for _, children := range state.Children {
		if err := add(len(children)); err != nil {
			return 0, err
		}
	}
	if err := add(len(state.Data)); err != nil {
		return 0, err
	}
	if err := add(len(state.ColdFiles)); err != nil {
		return 0, err
	}
	for _, extents := range state.ColdFiles {
		if err := add(len(extents)); err != nil {
			return 0, err
		}
	}
	if err := add(len(state.Segments)); err != nil {
		return 0, err
	}
	return count, nil
}

// StateStorageUsage returns the quota-relevant usage of an immutable state.
func StateStorageUsage(state *SnapshotState) (StorageUsage, error) {
	objects, err := StateObjectCount(state)
	if err != nil {
		return StorageUsage{}, err
	}
	return StorageUsage{
		Bytes:   StateStorageBytes(state),
		Objects: objects,
	}, nil
}
