package s0fs

import (
	"context"
	"math"
	"path/filepath"
	"testing"
)

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

func TestStateStorageBytesSaturatesInsteadOfWrapping(t *testing.T) {
	state := &SnapshotState{
		ColdFiles: map[uint64][]FileExtent{
			1: {
				{SegmentID: "huge"},
				{SegmentID: "more"},
			},
		},
		Segments: map[string]*Segment{
			"huge": {ID: "huge", Length: math.MaxUint64},
			"more": {ID: "more", Length: 1},
		},
	}

	if got := StateStorageBytes(state); got != math.MaxInt64 {
		t.Fatalf("StateStorageBytes() = %d, want saturation at %d", got, int64(math.MaxInt64))
	}
}

func TestEngineStorageBytesTracksWritesWithoutCloningState(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "volume-storage-size",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()
	node, err := engine.CreateFile(RootInode, "payload", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	got, err := engine.StorageBytes()
	if err != nil {
		t.Fatalf("StorageBytes() error = %v", err)
	}
	if got != 5 {
		t.Fatalf("StorageBytes() = %d, want 5", got)
	}
}

func TestStateObjectCountCoversEveryGrowingCollection(t *testing.T) {
	state := &SnapshotState{
		Nodes: map[uint64]*Node{
			1: {Inode: 1},
			2: {Inode: 2},
		},
		Children: map[uint64]map[string]uint64{
			1: {"a": 2, "b": 3},
			2: {"c": 4},
		},
		Data: map[uint64][]byte{
			2: nil,
		},
		ColdFiles: map[uint64][]FileExtent{
			3: {
				{SegmentID: "a"},
				{SegmentID: "b"},
				{SegmentID: "c"},
			},
		},
		Segments: map[string]*Segment{
			"a": {ID: "a"},
			"b": {ID: "b"},
			"c": {ID: "c"},
		},
	}

	got, err := StateObjectCount(state)
	if err != nil {
		t.Fatalf("StateObjectCount() error = %v", err)
	}
	if want := int64(15); got != want {
		t.Fatalf("StateObjectCount() = %d, want %d", got, want)
	}
}

func TestEngineStorageUsageCountsWALAndOpenUnlinkedState(t *testing.T) {
	engine, err := Open(context.Background(), Config{
		VolumeID: "volume-storage-objects",
		WALPath:  filepath.Join(t.TempDir(), "volume.wal"),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	usage, err := engine.StorageUsage()
	if err != nil {
		t.Fatalf("StorageUsage(initial) error = %v", err)
	}
	if usage.Objects != InitialStateObjectCount {
		t.Fatalf("StorageUsage(initial).Objects = %d, want %d", usage.Objects, InitialStateObjectCount)
	}

	node, err := engine.CreateFile(RootInode, "payload", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	usage, err = engine.StorageUsage()
	if err != nil {
		t.Fatalf("StorageUsage(after write) error = %v", err)
	}
	if usage.Bytes != 5 || usage.Objects != 7 {
		t.Fatalf("StorageUsage(after write) = %+v, want bytes=5 objects=7", usage)
	}

	if err := engine.Unlink(RootInode, "payload"); err != nil {
		t.Fatalf("Unlink() error = %v", err)
	}
	usage, err = engine.StorageUsage()
	if err != nil {
		t.Fatalf("StorageUsage(after unlink) error = %v", err)
	}
	if usage.Objects != 7 {
		t.Fatalf("StorageUsage(after unlink).Objects = %d, want 7", usage.Objects)
	}
	if err := engine.Forget(node.Inode); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	usage, err = engine.StorageUsage()
	if err != nil {
		t.Fatalf("StorageUsage(after forget) error = %v", err)
	}
	if usage.Bytes != 0 || usage.Objects != 5 {
		t.Fatalf("StorageUsage(after forget) = %+v, want bytes=0 objects=5", usage)
	}
}
