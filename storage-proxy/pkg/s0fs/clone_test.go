package s0fs

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

func TestCloneFilesIntoStateCreatesCOWFile(t *testing.T) {
	ctx := context.Background()
	baseStore := objectstore.NewMemoryStore("clone-cow")
	heads := newMemoryHeadStore()
	resolver := func(volumeID string) (objectstore.Store, error) {
		return objectstore.Prefix(baseStore, volumeID+"/s0fs/"), nil
	}

	sourceStore, _ := resolver("source")
	source, err := Open(ctx, Config{
		VolumeID:             "source",
		WALPath:              filepath.Join(t.TempDir(), "source.wal"),
		ObjectStore:          sourceStore,
		ObjectStoreForVolume: resolver,
		HeadStore:            heads,
	})
	if err != nil {
		t.Fatalf("Open(source) error = %v", err)
	}
	defer source.Close()

	sourceNode, err := source.CreateFile(RootInode, "asset.txt", 0o640)
	if err != nil {
		t.Fatalf("CreateFile(source) error = %v", err)
	}
	if err := source.SetOwner(sourceNode.Inode, 1000, 2000); err != nil {
		t.Fatalf("SetOwner(source) error = %v", err)
	}
	if _, err := source.Write(sourceNode.Inode, 0, []byte("shared-data")); err != nil {
		t.Fatalf("Write(source) error = %v", err)
	}
	if _, err := source.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize(source) error = %v", err)
	}

	targetStore, _ := resolver("target")
	target, err := Open(ctx, Config{
		VolumeID:             "target",
		WALPath:              filepath.Join(t.TempDir(), "target.wal"),
		ObjectStore:          targetStore,
		ObjectStoreForVolume: resolver,
		HeadStore:            heads,
	})
	if err != nil {
		t.Fatalf("Open(target) error = %v", err)
	}
	defer target.Close()

	nextState, results, err := CloneFilesIntoState(target.SnapshotState(), map[string]FileCloneSource{
		"source": {
			VolumeID: "source",
			State:    source.SnapshotState(),
		},
	}, []FileCloneEntry{{
		SourceVolumeID: "source",
		SourcePath:     "/asset.txt",
		TargetPath:     "/mnt/asset.txt",
		CreateParents:  true,
		Mode:           FileCloneModeCOW,
	}})
	if err != nil {
		t.Fatalf("CloneFilesIntoState() error = %v", err)
	}
	if len(results) != 1 || results[0].Mode != FileCloneModeCOW {
		t.Fatalf("clone results = %+v, want one cow result", results)
	}
	if err := target.ReplaceState(nextState); err != nil {
		t.Fatalf("ReplaceState(target) error = %v", err)
	}

	mnt, err := target.Lookup(RootInode, "mnt")
	if err != nil {
		t.Fatalf("Lookup(mnt) error = %v", err)
	}
	cloned, err := target.Lookup(mnt.Inode, "asset.txt")
	if err != nil {
		t.Fatalf("Lookup(cloned) error = %v", err)
	}
	if cloned.Mode != 0o640 || cloned.UID != 1000 || cloned.GID != 2000 {
		t.Fatalf("cloned attrs mode=%o uid=%d gid=%d, want mode=640 uid=1000 gid=2000", cloned.Mode, cloned.UID, cloned.GID)
	}
	data, err := target.Read(cloned.Inode, 0, cloned.Size)
	if err != nil {
		t.Fatalf("Read(cloned) error = %v", err)
	}
	if !bytes.Equal(data, []byte("shared-data")) {
		t.Fatalf("cloned data = %q, want shared-data", data)
	}

	if _, err := target.Write(cloned.Inode, 0, []byte("target")); err != nil {
		t.Fatalf("Write(target clone) error = %v", err)
	}
	sourceData, err := source.Read(sourceNode.Inode, 0, 1024)
	if err != nil {
		t.Fatalf("Read(source after target write) error = %v", err)
	}
	if !bytes.Equal(sourceData, []byte("shared-data")) {
		t.Fatalf("source after target write = %q, want shared-data", sourceData)
	}
}

func TestCloneFilesIntoStateFallsBackToInlineCopy(t *testing.T) {
	nowState := &SnapshotState{
		NextSeq:   1,
		NextInode: RootInode + 2,
		Nodes: map[uint64]*Node{
			RootInode: {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1},
			2:         {Inode: 2, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 4},
		},
		Children: map[uint64]map[string]uint64{RootInode: {"hot.txt": 2}},
		Data:     map[uint64][]byte{2: []byte("data")},
	}
	target := &SnapshotState{
		NextSeq:   1,
		NextInode: RootInode + 1,
		Nodes:     map[uint64]*Node{RootInode: {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1}},
		Children:  map[uint64]map[string]uint64{RootInode: {}},
	}

	next, results, err := CloneFilesIntoState(target, map[string]FileCloneSource{
		"source": {VolumeID: "source", State: nowState},
	}, []FileCloneEntry{{
		SourceVolumeID: "source",
		SourcePath:     "/hot.txt",
		TargetPath:     "/copy.txt",
		Mode:           FileCloneModeCopy,
		Data:           []byte("data"),
	}})
	if err != nil {
		t.Fatalf("CloneFilesIntoState(copy) error = %v", err)
	}
	if len(results) != 1 || results[0].Mode != FileCloneModeCopy {
		t.Fatalf("copy results = %+v, want one copy result", results)
	}
	lookup, err := LookupPath(next, "/copy.txt", false)
	if err != nil {
		t.Fatalf("LookupPath(copy) error = %v", err)
	}
	if got := string(next.Data[lookup.Inode]); got != "data" {
		t.Fatalf("copy data = %q, want data", got)
	}
}

func TestCloneFilesIntoStateIsAtomicOnConflict(t *testing.T) {
	source := &SnapshotState{
		NextSeq:   1,
		NextInode: RootInode + 2,
		Nodes: map[uint64]*Node{
			RootInode: {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1},
			2:         {Inode: 2, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 0},
		},
		Children: map[uint64]map[string]uint64{RootInode: {"empty.txt": 2}},
	}
	target := &SnapshotState{
		NextSeq:   1,
		NextInode: RootInode + 2,
		Nodes: map[uint64]*Node{
			RootInode: {Inode: RootInode, Type: TypeDirectory, Mode: 0o755, Nlink: 1},
			2:         {Inode: 2, Type: TypeFile, Mode: 0o644, Nlink: 1, Size: 0},
		},
		Children: map[uint64]map[string]uint64{RootInode: {"exists.txt": 2}},
	}

	_, _, err := CloneFilesIntoState(target, map[string]FileCloneSource{
		"source": {VolumeID: "source", State: source},
	}, []FileCloneEntry{
		{SourceVolumeID: "source", SourcePath: "/empty.txt", TargetPath: "/created.txt", Mode: FileCloneModeCOW},
		{SourceVolumeID: "source", SourcePath: "/empty.txt", TargetPath: "/exists.txt", Mode: FileCloneModeCOW},
	})
	if !errors.Is(err, ErrExists) {
		t.Fatalf("CloneFilesIntoState() error = %v, want ErrExists", err)
	}
	if _, exists := target.Children[RootInode]["created.txt"]; exists {
		t.Fatal("target state was mutated after failed atomic clone")
	}
}
