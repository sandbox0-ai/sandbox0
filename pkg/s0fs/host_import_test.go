package s0fs

import (
	"bytes"
	"context"
	"errors"
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

func TestMergeHostTreeReplacesTargetSubtree(t *testing.T) {
	baseRoot := t.TempDir()
	requireNoError(t, os.MkdirAll(filepath.Join(baseRoot, "workspace", "portal"), 0o755))
	requireNoError(t, os.WriteFile(filepath.Join(baseRoot, "workspace", "portal", "old.txt"), []byte("old"), 0o644))
	requireNoError(t, os.WriteFile(filepath.Join(baseRoot, "root.txt"), []byte("root"), 0o644))
	state, err := ImportHostTree(context.Background(), baseRoot, HostImportOptions{})
	requireNoError(t, err)

	backing := t.TempDir()
	requireNoError(t, os.WriteFile(filepath.Join(backing, "new.txt"), []byte("new"), 0o644))

	merged, err := MergeHostTree(context.Background(), state, backing, "/workspace/portal", HostImportOptions{})
	requireNoError(t, err)

	if _, err := merged.Lookup(requireStatePath(t, merged, "/workspace/portal"), "old.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old portal file lookup error = %v, want ErrNotFound", err)
	}
	newInode := requireStatePath(t, merged, "/workspace/portal/new.txt")
	payload, err := merged.Read(newInode, 0, 3)
	requireNoError(t, err)
	if string(payload) != "new" {
		t.Fatalf("new portal payload = %q, want new", payload)
	}
	rootInode := requireStatePath(t, merged, "/root.txt")
	rootPayload, err := merged.Read(rootInode, 0, 4)
	requireNoError(t, err)
	if string(rootPayload) != "root" {
		t.Fatalf("root payload = %q, want root", rootPayload)
	}
}

func TestMergeHostTreeCreatesMissingParentsAndPreservesHardlinks(t *testing.T) {
	backing := t.TempDir()
	requireNoError(t, os.WriteFile(filepath.Join(backing, "a.txt"), []byte("linked"), 0o644))
	requireNoError(t, os.Link(filepath.Join(backing, "a.txt"), filepath.Join(backing, "b.txt")))

	merged, err := MergeHostTree(context.Background(), nil, backing, "/var/lib/portal", HostImportOptions{})
	requireNoError(t, err)

	a := requireStatePath(t, merged, "/var/lib/portal/a.txt")
	b := requireStatePath(t, merged, "/var/lib/portal/b.txt")
	if a != b {
		t.Fatalf("hardlink inodes = %d and %d, want same inode", a, b)
	}
	if got := merged.Nodes[a].Nlink; got != 2 {
		t.Fatalf("hardlink nlink = %d, want 2", got)
	}
}

func TestImportHostTreePreservesSparseFileHoles(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "sparse.img")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
	requireNoError(t, err)
	const sparseSize = 512 << 20
	requireNoError(t, file.Truncate(sparseSize))
	_, err = file.WriteAt([]byte("tail"), sparseSize-4)
	requireNoError(t, err)
	requireNoError(t, file.Close())

	state, err := ImportHostTree(context.Background(), root, HostImportOptions{})
	if err != nil {
		t.Fatalf("ImportHostTree() error = %v", err)
	}
	inode := requireStatePath(t, state, "/sparse.img")
	node := state.Nodes[inode]
	if node.Size != sparseSize {
		t.Fatalf("sparse size = %d, want %d", node.Size, sparseSize)
	}
	if got, want := StateStorageBytes(state), int64(4); got != want {
		t.Fatalf("StateStorageBytes() = %d, want %d", got, want)
	}
	if data := state.Data[inode]; len(data) != 0 {
		t.Fatalf("sparse file imported %d inline bytes, want none", len(data))
	}

	reader := NewSnapshotReader(state, nil)
	head, err := reader.Read(inode, 0, 4)
	requireNoError(t, err)
	if got := head; !bytes.Equal(got, []byte{0, 0, 0, 0}) {
		t.Fatalf("sparse head = %v, want zeros", got)
	}
	tail, err := reader.Read(inode, sparseSize-4, 4)
	requireNoError(t, err)
	if got := string(tail); got != "tail" {
		t.Fatalf("sparse tail = %q, want tail", got)
	}
}

func TestImportHostTreeScansLargeFilesAsColdExtents(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "large.bin")
	payload := bytes.Repeat([]byte("a"), hostImportInlineFileSize+1)
	copy(payload[hostImportInlineChunkSize-2:], []byte("boundary"))
	requireNoError(t, os.WriteFile(filePath, payload, 0o644))

	state, err := ImportHostTree(context.Background(), root, HostImportOptions{})
	if err != nil {
		t.Fatalf("ImportHostTree() error = %v", err)
	}
	inode := requireStatePath(t, state, "/large.bin")
	if data := state.Data[inode]; len(data) != 0 {
		t.Fatalf("large file imported %d inline bytes, want cold extents", len(data))
	}
	if extents := state.ColdFiles[inode]; len(extents) == 0 {
		t.Fatalf("large file has no cold extents")
	}
	if got, want := StateStorageBytes(state), int64(len(payload)); got != want {
		t.Fatalf("StateStorageBytes() = %d, want %d", got, want)
	}
	reader := NewSnapshotReader(state, nil)
	got, err := reader.Read(inode, hostImportInlineChunkSize-2, uint64(len("boundary")))
	requireNoError(t, err)
	if string(got) != "boundary" {
		t.Fatalf("large file boundary read = %q, want boundary", got)
	}
}

func TestImportHostTreeFollowsRootSymlinkOnly(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	requireNoError(t, os.Mkdir(root, 0o755))
	requireNoError(t, os.WriteFile(filepath.Join(root, "file.txt"), []byte("hello"), 0o644))
	requireNoError(t, os.Symlink("file.txt", filepath.Join(root, "link.txt")))
	rootLink := filepath.Join(parent, "root-link")
	requireNoError(t, os.Symlink(root, rootLink))

	state, err := ImportHostTree(context.Background(), rootLink, HostImportOptions{})
	if err != nil {
		t.Fatalf("ImportHostTree(root symlink) error = %v", err)
	}
	fileNode := state.Nodes[requireStatePath(t, state, "/file.txt")]
	if fileNode.Type != TypeFile {
		t.Fatalf("file node type = %q, want file", fileNode.Type)
	}
	linkNode := state.Nodes[requireStatePath(t, state, "/link.txt")]
	if linkNode.Type != TypeSymlink || linkNode.Target != "file.txt" {
		t.Fatalf("child symlink node = %+v, want symlink to file.txt", linkNode)
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
