package portal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSBackedSessionWritesThroughBackingDir(t *testing.T) {
	backing := t.TempDir()
	session := newRootFSBackedSession(backing)
	require.NoError(t, session.initErr)
	defer session.Close()
	stateInfo, err := os.Stat(session.stateFilePath())
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), stateInfo.Mode().Perm())
	stateDirectoryInfo, err := os.Stat(session.stateDirectoryPath())
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), stateDirectoryInfo.Mode().Perm())

	ctx := context.Background()
	dir, err := session.Mkdir(ctx, &pb.MkdirRequest{
		Parent: s0fs.RootInode,
		Name:   "nested",
		Mode:   0o755,
	})
	require.NoError(t, err)

	file, err := session.Create(ctx, &pb.CreateRequest{
		Parent: dir.Inode,
		Name:   "state.txt",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = session.Write(ctx, &pb.WriteRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Offset:   0,
		Data:     []byte("portal-rootfs"),
	})
	require.NoError(t, err)
	_, err = session.Release(ctx, &pb.ReleaseRequest{HandleId: file.HandleId})
	require.NoError(t, err)

	assertFileContentForPortalTest(t, filepath.Join(backing, "nested", "state.txt"), "portal-rootfs")

	lookup, err := session.Lookup(ctx, &pb.LookupRequest{
		Parent: dir.Inode,
		Name:   "state.txt",
	})
	require.NoError(t, err)
	read, err := session.Read(ctx, &pb.ReadRequest{
		Inode:  lookup.Inode,
		Offset: 0,
		Size:   64,
	})
	require.NoError(t, err)
	assert.Equal(t, "portal-rootfs", string(read.Data))

	list, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: dir.Inode})
	require.NoError(t, err)
	require.Len(t, list.Entries, 1)
	assert.Equal(t, "state.txt", list.Entries[0].Name)
}

func TestRootFSBackedSessionRecoversInodesAndOpenHandles(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)

	directory, err := first.Mkdir(ctx, &pb.MkdirRequest{
		Parent: s0fs.RootInode,
		Name:   "workspace",
		Mode:   0o755,
	})
	require.NoError(t, err)
	file, err := first.Create(ctx, &pb.CreateRequest{
		Parent: directory.Inode,
		Name:   "state.txt",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(ctx, &pb.WriteRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Data:     []byte("before"),
	})
	require.NoError(t, err)
	directoryHandle, err := first.OpenDir(ctx, &pb.OpenDirRequest{Inode: directory.Inode})
	require.NoError(t, err)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	defer second.Close()
	lookup, err := second.Lookup(ctx, &pb.LookupRequest{Parent: directory.Inode, Name: "state.txt"})
	require.NoError(t, err)
	assert.Equal(t, file.Inode, lookup.Inode)
	read, err := second.Read(ctx, &pb.ReadRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Offset:   0,
		Size:     64,
	})
	require.NoError(t, err)
	assert.Equal(t, "before", string(read.Data))
	_, err = second.Write(ctx, &pb.WriteRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Offset:   int64(len("before")),
		Data:     []byte("-after"),
	})
	require.NoError(t, err)
	listing, err := second.ReadDir(ctx, &pb.ReadDirRequest{
		Inode:    directory.Inode,
		HandleId: directoryHandle.HandleId,
	})
	require.NoError(t, err)
	require.Len(t, listing.Entries, 1)
	assert.Equal(t, file.Inode, listing.Entries[0].Inode)
	_, err = second.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
	_, err = second.ReleaseDir(ctx, &pb.ReleaseDirRequest{Inode: directory.Inode, HandleId: directoryHandle.HandleId})
	require.NoError(t, err)
	assertFileContentForPortalTest(t, filepath.Join(backing, "workspace", "state.txt"), "before-after")
}

func TestRootFSBackedSessionRecoversOpenUnlinkedFile(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	file, err := first.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "unlinked.txt",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(ctx, &pb.WriteRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Data:     []byte("survives"),
	})
	require.NoError(t, err)
	_, err = first.Unlink(ctx, &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "unlinked.txt"})
	require.NoError(t, err)
	_, err = os.Lstat(filepath.Join(backing, "unlinked.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.FileExists(t, first.hostPath(rootFSOrphanPath(file.Inode)))
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	read, err := second.Read(ctx, &pb.ReadRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Size:     64,
	})
	require.NoError(t, err)
	assert.Equal(t, "survives", string(read.Data))
	_, err = second.Write(ctx, &pb.WriteRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Offset:   int64(len("survives")),
		Data:     []byte("-restart"),
	})
	require.NoError(t, err)
	listing, err := second.ReadDir(ctx, &pb.ReadDirRequest{Inode: s0fs.RootInode})
	require.NoError(t, err)
	assert.Empty(t, listing.Entries)
	_, err = second.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
	_, err = os.Lstat(second.hostPath(rootFSOrphanPath(file.Inode)))
	require.ErrorIs(t, err, os.ErrNotExist)
	second.Close()

	third := newRootFSBackedSession(backing)
	require.NoError(t, third.initErr)
	defer third.Close()
	_, err = third.GetAttr(ctx, &pb.GetAttrRequest{Inode: file.Inode})
	require.Error(t, err)
}

func TestRootFSBackedSessionRecoversOpenRemovedDirectory(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	directory, err := first.Mkdir(ctx, &pb.MkdirRequest{Parent: s0fs.RootInode, Name: "removed", Mode: 0o700})
	require.NoError(t, err)
	handle, err := first.OpenDir(ctx, &pb.OpenDirRequest{Inode: directory.Inode})
	require.NoError(t, err)
	_, err = first.Rmdir(ctx, &pb.RmdirRequest{Parent: s0fs.RootInode, Name: "removed"})
	require.NoError(t, err)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	listing, err := second.ReadDir(ctx, &pb.ReadDirRequest{Inode: directory.Inode, HandleId: handle.HandleId})
	require.NoError(t, err)
	assert.Empty(t, listing.Entries)
	_, err = second.ReleaseDir(ctx, &pb.ReleaseDirRequest{Inode: directory.Inode, HandleId: handle.HandleId})
	require.NoError(t, err)
	second.Close()

	third := newRootFSBackedSession(backing)
	require.NoError(t, third.initErr)
	defer third.Close()
	_, err = third.GetAttr(ctx, &pb.GetAttrRequest{Inode: directory.Inode})
	require.Error(t, err)
}

func TestRootFSBackedSessionRenameAndReservedDirectory(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	directory, err := first.Mkdir(ctx, &pb.MkdirRequest{Parent: s0fs.RootInode, Name: "old", Mode: 0o755})
	require.NoError(t, err)
	file, err := first.Create(ctx, &pb.CreateRequest{
		Parent: directory.Inode,
		Name:   "file.txt",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
	_, err = first.Rename(ctx, &pb.RenameRequest{
		OldParent: s0fs.RootInode,
		OldName:   "old",
		NewParent: s0fs.RootInode,
		NewName:   "new",
	})
	require.NoError(t, err)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	defer second.Close()
	renamed, err := second.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "new"})
	require.NoError(t, err)
	assert.Equal(t, directory.Inode, renamed.Inode)
	child, err := second.Lookup(ctx, &pb.LookupRequest{Parent: renamed.Inode, Name: "file.txt"})
	require.NoError(t, err)
	assert.Equal(t, file.Inode, child.Inode)
	listing, err := second.ReadDir(ctx, &pb.ReadDirRequest{Inode: s0fs.RootInode})
	require.NoError(t, err)
	require.Len(t, listing.Entries, 1)
	assert.Equal(t, "new", listing.Entries[0].Name)
	_, err = second.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: rootFSStateDirectoryName})
	require.Error(t, err)
	_, err = second.Mkdir(ctx, &pb.MkdirRequest{Parent: s0fs.RootInode, Name: rootFSStateDirectoryName})
	require.Error(t, err)
}

func TestRootFSBackedSessionHardLinkKeepsOpenHandleRecoverable(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	file, err := first.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "original",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(ctx, &pb.WriteRequest{Inode: file.Inode, HandleId: file.HandleId, Data: []byte("linked")})
	require.NoError(t, err)
	linked, err := first.Link(ctx, &pb.LinkRequest{
		Inode:     file.Inode,
		NewParent: s0fs.RootInode,
		NewName:   "alias",
	})
	require.NoError(t, err)
	assert.Equal(t, file.Inode, linked.Inode)
	_, err = first.Unlink(ctx, &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "original"})
	require.NoError(t, err)
	_, err = os.Lstat(first.hostPath(rootFSOrphanPath(file.Inode)))
	require.ErrorIs(t, err, os.ErrNotExist)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	defer second.Close()
	alias, err := second.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "alias"})
	require.NoError(t, err)
	assert.Equal(t, file.Inode, alias.Inode)
	read, err := second.Read(ctx, &pb.ReadRequest{Inode: file.Inode, HandleId: file.HandleId, Size: 64})
	require.NoError(t, err)
	assert.Equal(t, "linked", string(read.Data))
	_, err = second.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
}

func TestRootFSBackedSessionRenamePreservesOverwrittenOpenTarget(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	source, err := first.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "source",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(ctx, &pb.WriteRequest{Inode: source.Inode, HandleId: source.HandleId, Data: []byte("source-data")})
	require.NoError(t, err)
	_, err = first.Release(ctx, &pb.ReleaseRequest{Inode: source.Inode, HandleId: source.HandleId})
	require.NoError(t, err)
	target, err := first.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "target",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(ctx, &pb.WriteRequest{Inode: target.Inode, HandleId: target.HandleId, Data: []byte("target-data")})
	require.NoError(t, err)
	_, err = first.Rename(ctx, &pb.RenameRequest{
		OldParent: s0fs.RootInode,
		OldName:   "source",
		NewParent: s0fs.RootInode,
		NewName:   "target",
	})
	require.NoError(t, err)
	visible, err := first.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "target"})
	require.NoError(t, err)
	assert.Equal(t, source.Inode, visible.Inode)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	defer second.Close()
	read, err := second.Read(ctx, &pb.ReadRequest{Inode: target.Inode, HandleId: target.HandleId, Size: 64})
	require.NoError(t, err)
	assert.Equal(t, "target-data", string(read.Data))
	_, err = second.Release(ctx, &pb.ReleaseRequest{Inode: target.Inode, HandleId: target.HandleId})
	require.NoError(t, err)
	_, err = os.Lstat(second.hostPath(rootFSOrphanPath(target.Inode)))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRootFSBackedSessionReconcilesRenameAfterStateWriteGap(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	file, err := first.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "before",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
	first.Close()
	require.NoError(t, os.Rename(filepath.Join(backing, "before"), filepath.Join(backing, "after")))

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.initErr)
	defer second.Close()
	renamed, err := second.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "after"})
	require.NoError(t, err)
	assert.Equal(t, file.Inode, renamed.Inode)
	_, err = second.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "before"})
	require.Error(t, err)
}

func TestRootFSBackedSessionCorruptStateFailsClosed(t *testing.T) {
	backing := t.TempDir()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.initErr)
	statePath := first.stateFilePath()
	first.Close()
	require.NoError(t, os.WriteFile(statePath, []byte("not-json"), 0o600))

	second := newRootFSBackedSession(backing)
	require.Error(t, second.initErr)
	require.Error(t, second.InitError())
	defer second.Close()
	_, err := second.Lookup(context.Background(), &pb.LookupRequest{Parent: s0fs.RootInode, Name: "anything"})
	require.Error(t, err)
	_, err = second.OpenDir(context.Background(), &pb.OpenDirRequest{Inode: s0fs.RootInode})
	require.Error(t, err)
	_, err = second.StatFs(context.Background(), &pb.StatFsRequest{})
	require.Error(t, err)
}

func TestRootFSBackedSessionLookupAndOpenDoNotWriteState(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	session := newRootFSBackedSession(backing)
	require.NoError(t, session.InitError())
	defer session.Close()
	before, err := os.ReadFile(session.stateFilePath())
	require.NoError(t, err)

	file, err := session.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "hot-path",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	hostInfo, err := os.Lstat(filepath.Join(backing, "hot-path"))
	require.NoError(t, err)
	identity, err := rootFSIdentityFromInfo(hostInfo)
	require.NoError(t, err)
	assert.Equal(t, identity.Inode, file.Inode)
	assert.Equal(t, file.Inode, file.HandleId)
	_, err = session.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
	for range 10 {
		lookup, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "hot-path"})
		require.NoError(t, err)
		opened, err := session.Open(ctx, &pb.OpenRequest{Inode: lookup.Inode, Flags: uint32(os.O_RDWR)})
		require.NoError(t, err)
		_, err = session.Release(ctx, &pb.ReleaseRequest{Inode: lookup.Inode, HandleId: opened.HandleId})
		require.NoError(t, err)
	}
	after, err := os.ReadFile(session.stateFilePath())
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRootFSBackedSessionConservativelyRetainsUnlinkAfterRecovery(t *testing.T) {
	backing := t.TempDir()
	ctx := context.Background()
	first := newRootFSBackedSession(backing)
	require.NoError(t, first.InitError())
	file, err := first.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "possibly-open",
		Mode:   0o600,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = first.Write(ctx, &pb.WriteRequest{Inode: file.Inode, HandleId: file.HandleId, Data: []byte("safe")})
	require.NoError(t, err)
	first.Close()

	second := newRootFSBackedSession(backing)
	require.NoError(t, second.InitError())
	defer second.Close()
	_, err = second.Unlink(ctx, &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "possibly-open"})
	require.NoError(t, err)
	read, err := second.Read(ctx, &pb.ReadRequest{Inode: file.Inode, HandleId: file.HandleId, Size: 16})
	require.NoError(t, err)
	assert.Equal(t, "safe", string(read.Data))
	_, err = second.Release(ctx, &pb.ReleaseRequest{Inode: file.Inode, HandleId: file.HandleId})
	require.NoError(t, err)
	require.FileExists(t, second.hostPath(rootFSOrphanPath(file.Inode)))
}

func assertFileContentForPortalTest(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}
