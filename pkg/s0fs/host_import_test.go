package s0fs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestImportHostTreePreservesMetadataAndExcludes(t *testing.T) {
	root := t.TempDir()
	requireNoError(t, os.Mkdir(filepath.Join(root, "dir"), 0o755))
	filePath := filepath.Join(root, "dir", "file.txt")
	requireNoError(t, os.WriteFile(filePath, []byte("hello"), 0o644))
	requireNoError(t, os.Link(filePath, filepath.Join(root, "hard.txt")))
	requireNoError(t, os.Symlink("dir/file.txt", filepath.Join(root, "link.txt")))
	requireNoError(t, os.Mkdir(filepath.Join(root, "skip"), 0o755))
	requireNoError(t, os.WriteFile(filepath.Join(root, "skip", "ignored.txt"), []byte("ignored"), 0o644))
	requireNoError(t, unix.Mkfifo(filepath.Join(root, "pipe"), 0o644))

	xattrSet := unix.Lsetxattr(filePath, "user.s0fs_test", []byte("xvalue"), 0) == nil

	state, err := ImportHostTree(context.Background(), root, HostImportOptions{
		ExcludedPaths: []string{"/skip"},
	})
	if err != nil {
		t.Fatalf("ImportHostTree() error = %v", err)
	}

	fileInode := requireStatePath(t, state, "/dir/file.txt")
	fileNode := state.Nodes[fileInode]
	if fileNode.Type != TypeFile || fileNode.Size != uint64(len("hello")) {
		t.Fatalf("file node = %+v, want regular file with size", fileNode)
	}
	if got := state.Data[fileInode]; !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("file data = %q, want hello", got)
	}
	if xattrSet {
		if got := fileNode.Xattrs["user.s0fs_test"]; !bytes.Equal(got, []byte("xvalue")) {
			t.Fatalf("xattr user.s0fs_test = %q, want xvalue", got)
		}
	}

	hardlinkInode := requireStatePath(t, state, "/hard.txt")
	if hardlinkInode != fileInode {
		t.Fatalf("hardlink inode = %d, want %d", hardlinkInode, fileInode)
	}
	if fileNode.Nlink != 2 {
		t.Fatalf("file nlink = %d, want 2", fileNode.Nlink)
	}
	linkNode := state.Nodes[requireStatePath(t, state, "/link.txt")]
	if linkNode.Type != TypeSymlink || linkNode.Target != "dir/file.txt" {
		t.Fatalf("symlink node = %+v, want target dir/file.txt", linkNode)
	}
	pipeNode := state.Nodes[requireStatePath(t, state, "/pipe")]
	if pipeNode.Type != TypeFIFO {
		t.Fatalf("pipe type = %q, want fifo", pipeNode.Type)
	}
	if inode := statePath(state, "/skip/ignored.txt"); inode != 0 {
		t.Fatalf("excluded path imported with inode %d", inode)
	}
}

func TestImportHostTreePreservesBaseColdExtents(t *testing.T) {
	root := t.TempDir()
	requireNoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("hello"), 0o644))

	base, err := ImportHostTree(context.Background(), root, HostImportOptions{})
	if err != nil {
		t.Fatalf("ImportHostTree(base) error = %v", err)
	}
	baseInode := requireStatePath(t, base, "/file.txt")
	delete(base.Data, baseInode)
	base.ColdFiles[baseInode] = []FileExtent{{SegmentID: "seg-1", Offset: 1, Length: 5}}
	base.Segments["seg-1"] = &Segment{ID: "seg-1", VolumeID: "vol-1", Key: "segments/seg-1", Length: 5}

	state, err := ImportHostTree(context.Background(), root, HostImportOptions{Base: base})
	if err != nil {
		t.Fatalf("ImportHostTree(with base) error = %v", err)
	}
	inode := requireStatePath(t, state, "/file.txt")
	if _, ok := state.Data[inode]; ok {
		t.Fatalf("file data was re-imported, want cold extent reuse")
	}
	extents := state.ColdFiles[inode]
	if len(extents) != 1 || extents[0].SegmentID != "seg-1" {
		t.Fatalf("cold extents = %+v, want seg-1", extents)
	}
	if state.Segments["seg-1"] == nil {
		t.Fatalf("segment seg-1 was not copied")
	}
}

func requireStatePath(t *testing.T, state *SnapshotState, path string) uint64 {
	t.Helper()
	inode := statePath(state, path)
	if inode == 0 {
		t.Fatalf("missing state path %s", path)
	}
	return inode
}

func statePath(state *SnapshotState, target string) uint64 {
	paths := statePaths(state)
	return paths[target]
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
