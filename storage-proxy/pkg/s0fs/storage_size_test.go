package s0fs

import "testing"

func TestStateStorageBytesCountsInlineAndReferencedSegments(t *testing.T) {
	state := &SnapshotState{
		Data: map[uint64][]byte{
			2: []byte("hot"),
		},
		ColdFiles: map[uint64][]FileExtent{
			3: {
				{SegmentID: "seg-a", Offset: 0, Length: 4},
				{SegmentID: "seg-a", Offset: 4, Length: 4},
				{SegmentID: "seg-b", Offset: 0, Length: 8},
			},
		},
		Segments: map[string]*Segment{
			"seg-a":  {ID: "seg-a", Length: 8},
			"seg-b":  {ID: "seg-b", Length: 16},
			"unused": {ID: "unused", Length: 32},
		},
	}

	if got, want := StateStorageBytes(state), int64(27); got != want {
		t.Fatalf("StateStorageBytes() = %d, want %d", got, want)
	}
}
