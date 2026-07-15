package portal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSBackedSessionWritesThroughBackingDir(t *testing.T) {
	backing := t.TempDir()
	session := newRootFSBackedSession(backing)
	defer session.Close()

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

func TestRootFSBackedSessionPreservesRemoveErrnos(t *testing.T) {
	backing := t.TempDir()
	session := newRootFSBackedSession(backing)
	defer session.Close()
	ctx := context.Background()

	nonEmpty, err := session.Mkdir(ctx, &pb.MkdirRequest{
		Parent: s0fs.RootInode,
		Name:   "non-empty",
		Mode:   0o755,
	})
	require.NoError(t, err)
	child, err := session.Create(ctx, &pb.CreateRequest{
		Parent: nonEmpty.Inode,
		Name:   "child",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = session.Release(ctx, &pb.ReleaseRequest{HandleId: child.HandleId})
	require.NoError(t, err)

	_, err = session.Rmdir(ctx, &pb.RmdirRequest{Parent: s0fs.RootInode, Name: "non-empty"})
	require.ErrorIs(t, err, syscall.ENOTEMPTY)
	_, statErr := os.Stat(filepath.Join(backing, "non-empty", "child"))
	require.NoError(t, statErr)

	_, err = session.Mkdir(ctx, &pb.MkdirRequest{
		Parent: s0fs.RootInode,
		Name:   "empty-dir",
		Mode:   0o755,
	})
	require.NoError(t, err)
	_, err = session.Unlink(ctx, &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "empty-dir"})
	require.ErrorIs(t, err, syscall.EISDIR)
	info, statErr := os.Stat(filepath.Join(backing, "empty-dir"))
	require.NoError(t, statErr)
	require.True(t, info.IsDir())

	file, err := session.Create(ctx, &pb.CreateRequest{
		Parent: s0fs.RootInode,
		Name:   "regular-file",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = session.Release(ctx, &pb.ReleaseRequest{HandleId: file.HandleId})
	require.NoError(t, err)
	_, err = session.Rmdir(ctx, &pb.RmdirRequest{Parent: s0fs.RootInode, Name: "regular-file"})
	require.ErrorIs(t, err, syscall.ENOTDIR)
	info, statErr = os.Stat(filepath.Join(backing, "regular-file"))
	require.NoError(t, statErr)
	require.False(t, info.IsDir())
}

func TestRootFSBackedSessionRestoresKernelInodeMapping(t *testing.T) {
	backing := t.TempDir()
	statePath := filepath.Join(t.TempDir(), "portal.jsonl")
	ctx := context.Background()

	primary, err := newRootFSBackedSessionWithState(backing, statePath)
	require.NoError(t, err)
	dir, err := primary.Mkdir(ctx, &pb.MkdirRequest{
		Parent: s0fs.RootInode,
		Name:   "before",
		Mode:   0o755,
	})
	require.NoError(t, err)
	file, err := primary.Create(ctx, &pb.CreateRequest{
		Parent: dir.Inode,
		Name:   "state.txt",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = primary.Write(ctx, &pb.WriteRequest{
		Inode:    file.Inode,
		HandleId: file.HandleId,
		Data:     []byte("survives"),
	})
	require.NoError(t, err)
	_, err = primary.Release(ctx, &pb.ReleaseRequest{HandleId: file.HandleId})
	require.NoError(t, err)
	_, err = primary.Rename(ctx, &pb.RenameRequest{
		OldParent: s0fs.RootInode,
		OldName:   "before",
		NewParent: s0fs.RootInode,
		NewName:   "after",
	})
	require.NoError(t, err)
	primary.Close()

	standby, err := newRootFSBackedSessionWithState(backing, statePath)
	require.NoError(t, err)
	defer standby.Close()
	attr, err := standby.GetAttr(ctx, &pb.GetAttrRequest{Inode: file.Inode})
	require.NoError(t, err)
	assert.Equal(t, file.Inode, attr.Ino)
	read, err := standby.Read(ctx, &pb.ReadRequest{Inode: file.Inode, Size: 64})
	require.NoError(t, err)
	assert.Equal(t, "survives", string(read.Data))
	lookup, err := standby.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "after"})
	require.NoError(t, err)
	assert.Equal(t, dir.Inode, lookup.Inode)
}

func TestRootFSBackedSessionRestoresOpenUnlinkedHandle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	statePath := filepath.Join(t.TempDir(), "rootfs-state.jsonl")
	first, err := newRootFSBackedSessionWithState(root, statePath)
	if err != nil {
		t.Fatalf("newRootFSBackedSessionWithState(first) error = %v", err)
	}
	created, err := first.Create(ctx, &pb.CreateRequest{Parent: s0fs.RootInode, Name: "transient.txt", Flags: uint32(os.O_RDWR)})
	if err != nil {
		t.Fatalf("Create(transient.txt) error = %v", err)
	}
	if _, err := first.Write(ctx, &pb.WriteRequest{Inode: created.Inode, HandleId: created.HandleId, Data: []byte("before")}); err != nil {
		t.Fatalf("Write(before) error = %v", err)
	}
	if _, err := first.Unlink(ctx, &pb.UnlinkRequest{Parent: s0fs.RootInode, Name: "transient.txt"}); err != nil {
		t.Fatalf("Unlink(transient.txt) error = %v", err)
	}
	first.Close()

	second, err := newRootFSBackedSessionWithState(root, statePath)
	if err != nil {
		t.Fatalf("newRootFSBackedSessionWithState(second) error = %v", err)
	}
	defer second.Close()
	if _, err := second.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "transient.txt"}); fserror.CodeOf(err) != fserror.NotFound {
		t.Fatalf("Lookup(unlinked path) error = %v, want not found", err)
	}
	read, err := second.Read(ctx, &pb.ReadRequest{HandleId: created.HandleId, Size: 64})
	if err != nil {
		t.Fatalf("Read(restored handle) error = %v", err)
	}
	if string(read.Data) != "before" {
		t.Fatalf("Read(restored handle) = %q, want before", string(read.Data))
	}
	if _, err := second.Write(ctx, &pb.WriteRequest{HandleId: created.HandleId, Offset: int64(len("before")), Data: []byte("-after")}); err != nil {
		t.Fatalf("Write(restored handle) error = %v", err)
	}
	if _, err := second.Release(ctx, &pb.ReleaseRequest{Inode: created.Inode, HandleId: created.HandleId}); err != nil {
		t.Fatalf("Release(restored handle) error = %v", err)
	}
	orphan := filepath.Join(root, rootFSRecoveryDir, "orphans", strconv.FormatUint(created.Inode, 10))
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan still exists after release: %v", err)
	}
	entries, err := second.ReadDir(ctx, &pb.ReadDirRequest{Inode: s0fs.RootInode})
	if err != nil {
		t.Fatalf("ReadDir(root) error = %v", err)
	}
	if len(entries.Entries) != 0 {
		t.Fatalf("ReadDir(root) exposed recovery entries: %#v", entries.Entries)
	}
}

func assertFileContentForPortalTest(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}
