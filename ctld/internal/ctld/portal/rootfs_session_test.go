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

func assertFileContentForPortalTest(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}
