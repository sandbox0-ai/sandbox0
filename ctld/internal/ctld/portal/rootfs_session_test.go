package portal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
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

func TestRootFSUnionSessionFallsBackToBaseRootFS(t *testing.T) {
	ctx := context.Background()
	backing := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(backing, "bin"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backing, "bin", "sh"), []byte("base-shell"), 0o755))
	session, _ := newRootFSUnionSessionForTest(t, backing)

	bin, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "bin"})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, bin.Inode, rootFSUnionLowerInodeBase)
	sh, err := session.Lookup(ctx, &pb.LookupRequest{Parent: bin.Inode, Name: "sh"})
	require.NoError(t, err)

	read, err := session.Read(ctx, &pb.ReadRequest{Inode: sh.Inode, Offset: 0, Size: 64})
	require.NoError(t, err)
	assert.Equal(t, "base-shell", string(read.Data))

	rootEntries, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: s0fs.RootInode})
	require.NoError(t, err)
	assert.Contains(t, portalTestDirEntryNames(rootEntries), "bin")
}

func TestRootFSUnionSessionCopiesLowerFileToS0FSOnWrite(t *testing.T) {
	ctx := context.Background()
	backing := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(backing, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backing, "etc", "config"), []byte("base-data"), 0o644))
	session, engine := newRootFSUnionSessionForTest(t, backing)

	etc, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "etc"})
	require.NoError(t, err)
	config, err := session.Lookup(ctx, &pb.LookupRequest{Parent: etc.Inode, Name: "config"})
	require.NoError(t, err)
	opened, err := session.Open(ctx, &pb.OpenRequest{Inode: config.Inode, Flags: uint32(os.O_RDWR | os.O_TRUNC)})
	require.NoError(t, err)
	_, err = session.Write(ctx, &pb.WriteRequest{
		Inode:    config.Inode,
		HandleId: opened.HandleId,
		Offset:   0,
		Data:     []byte("changed"),
	})
	require.NoError(t, err)
	_, err = session.Release(ctx, &pb.ReleaseRequest{Inode: config.Inode, HandleId: opened.HandleId})
	require.NoError(t, err)

	assertFileContentForPortalTest(t, filepath.Join(backing, "etc", "config"), "base-data")
	read, err := session.Read(ctx, &pb.ReadRequest{Inode: config.Inode, Offset: 0, Size: 64})
	require.NoError(t, err)
	assert.Equal(t, "changed", string(read.Data))

	upperEtc, err := engine.Lookup(s0fs.RootInode, "etc")
	require.NoError(t, err)
	upperConfig, err := engine.Lookup(upperEtc.Inode, "config")
	require.NoError(t, err)
	payload, err := engine.Read(upperConfig.Inode, 0, upperConfig.Size)
	require.NoError(t, err)
	assert.Equal(t, "changed", string(payload))
}

func TestRootFSUnionSessionCreatesUnderLowerOnlyDirectory(t *testing.T) {
	ctx := context.Background()
	backing := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(backing, "tmp"), 0o777))
	session, engine := newRootFSUnionSessionForTest(t, backing)

	tmp, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "tmp"})
	require.NoError(t, err)
	created, err := session.Create(ctx, &pb.CreateRequest{
		Parent: tmp.Inode,
		Name:   "state.txt",
		Mode:   0o644,
		Flags:  uint32(os.O_RDWR),
	})
	require.NoError(t, err)
	_, err = session.Write(ctx, &pb.WriteRequest{
		Inode:    created.Inode,
		HandleId: created.HandleId,
		Data:     []byte("new-state"),
	})
	require.NoError(t, err)
	_, err = session.Release(ctx, &pb.ReleaseRequest{Inode: created.Inode, HandleId: created.HandleId})
	require.NoError(t, err)

	upperTmp, err := engine.Lookup(s0fs.RootInode, "tmp")
	require.NoError(t, err)
	upperFile, err := engine.Lookup(upperTmp.Inode, "state.txt")
	require.NoError(t, err)
	payload, err := engine.Read(upperFile.Inode, 0, upperFile.Size)
	require.NoError(t, err)
	assert.Equal(t, "new-state", string(payload))
	assert.NoFileExists(t, filepath.Join(backing, "tmp", "state.txt"))
}

func TestRootFSUnionSessionHonorsUpperWhiteouts(t *testing.T) {
	ctx := context.Background()
	backing := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(backing, "etc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backing, "etc", "removed"), []byte("removed"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(backing, "etc", "kept"), []byte("kept"), 0o644))
	session, engine := newRootFSUnionSessionForTest(t, backing)
	upperEtc, err := engine.Mkdir(s0fs.RootInode, "etc", 0o755)
	require.NoError(t, err)
	_, err = engine.CreateFile(upperEtc.Inode, ".wh.removed", 0)
	require.NoError(t, err)

	etc, err := session.Lookup(ctx, &pb.LookupRequest{Parent: s0fs.RootInode, Name: "etc"})
	require.NoError(t, err)
	_, err = session.Lookup(ctx, &pb.LookupRequest{Parent: etc.Inode, Name: "removed"})
	require.Error(t, err)
	assert.Equal(t, fserror.NotFound, fserror.CodeOf(err))
	kept, err := session.Lookup(ctx, &pb.LookupRequest{Parent: etc.Inode, Name: "kept"})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, kept.Inode, rootFSUnionLowerInodeBase)

	entries, err := session.ReadDir(ctx, &pb.ReadDirRequest{Inode: etc.Inode})
	require.NoError(t, err)
	names := portalTestDirEntryNames(entries)
	assert.Contains(t, names, "kept")
	assert.NotContains(t, names, "removed")
	assert.NotContains(t, names, ".wh.removed")
}

func newRootFSUnionSessionForTest(t *testing.T, backing string) (*rootFSUnionSession, *s0fs.Engine) {
	t.Helper()
	ctx := context.Background()
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID: "rootfs-" + t.Name(),
		WALPath:  filepath.Join(t.TempDir(), "rootfs.wal"),
	})
	require.NoError(t, err)
	session := newRootFSUnionSession("rootfs-"+t.Name(), engine, NewS0FSSession("rootfs-"+t.Name(), "team-1", engine, nil), newRootFSBackedSession(backing))
	t.Cleanup(func() {
		session.Close()
		require.NoError(t, engine.Close())
	})
	return session, engine
}

func portalTestDirEntryNames(resp *pb.ReadDirResponse) []string {
	if resp == nil {
		return nil
	}
	names := make([]string, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		names = append(names, entry.GetName())
	}
	return names
}

func assertFileContentForPortalTest(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}
