package rootfsfuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsreader"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFillAttrPreservesFilesystemMetadata(t *testing.T) {
	entry := rootfshead.Entry{
		Inode:  "inode-1",
		Kind:   rootfshead.EntryFile,
		Mode:   0o100640,
		UID:    1000,
		GID:    1001,
		Nlink:  2,
		Size:   8192,
		Blocks: 8,
		Rdev:   7,
		ModTime: rootfshead.Timestamp{
			Seconds:     123,
			Nanoseconds: 456,
		},
	}
	var attr fuse.Attr
	fillAttr(&attr, entry)
	assert.Equal(t, entry.Mode, attr.Mode)
	assert.Equal(t, entry.Size, attr.Size)
	assert.Equal(t, entry.Blocks, attr.Blocks)
	assert.Equal(t, entry.UID, attr.Uid)
	assert.Equal(t, entry.GID, attr.Gid)
	assert.Equal(t, entry.Nlink, attr.Nlink)
	assert.Equal(t, uint64(123), attr.Mtime)
	assert.Equal(t, uint32(456), attr.Mtimensec)
}

func TestNodeXAttrsAreSortedAndReadOnly(t *testing.T) {
	node := &Node{entry: rootfshead.Entry{
		XAttrs: []rootfshead.XAttr{
			{Name: "security.capability", Value: []byte("cap")},
			{Name: "user.value", Value: []byte("payload")},
		},
	}}
	size, errno := node.Listxattr(context.Background(), nil)
	require.Zero(t, errno)
	destination := make([]byte, size)
	read, errno := node.Listxattr(context.Background(), destination)
	require.Zero(t, errno)
	assert.Equal(t, size, read)
	assert.Equal(t, []byte("security.capability\x00user.value\x00"), destination)

	value := make([]byte, 7)
	read, errno = node.Getxattr(context.Background(), "user.value", value)
	require.Zero(t, errno)
	assert.Equal(t, uint32(7), read)
	assert.Equal(t, []byte("payload"), value)
	_, errno = node.Getxattr(context.Background(), "missing", nil)
	assert.Equal(t, syscall.ENODATA, errno)
}

func TestOpaqueDirectorySynthesizesOverlayXAttr(t *testing.T) {
	node := &Node{entry: rootfshead.Entry{Kind: rootfshead.EntryDirectory, Opaque: true}}

	size, errno := node.Getxattr(context.Background(), overlayOpaqueXAttr, nil)
	require.Zero(t, errno)
	assert.Equal(t, uint32(1), size)
	value := make([]byte, size)
	read, errno := node.Getxattr(context.Background(), overlayOpaqueXAttr, value)
	require.Zero(t, errno)
	assert.Equal(t, uint32(1), read)
	assert.Equal(t, []byte("y"), value)

	size, errno = node.Listxattr(context.Background(), nil)
	require.Zero(t, errno)
	names := make([]byte, size)
	_, errno = node.Listxattr(context.Background(), names)
	require.Zero(t, errno)
	assert.Equal(t, []byte(overlayOpaqueXAttr+"\x00"), names)
}

func TestInodeIdentitySupportsHardlinks(t *testing.T) {
	assert.Equal(t, inodeNumber("generation:1:2"), inodeNumber("generation:1:2"))
	assert.NotEqual(t, inodeNumber("generation:1:2"), inodeNumber("generation:1:3"))
}

func TestNodeCachesDecodedFileManifest(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "fuse-manifest-team")
	require.NoError(t, err)
	payload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    4096,
	})
	require.NoError(t, err)
	object, err := writer.Put(context.Background(), rootfshead.FileMediaType, payload)
	require.NoError(t, err)
	reader, err := rootfsreader.New(rootfsreader.ReaderConfig{
		Store:              store,
		Prefix:             writer.Prefix(),
		MetadataCacheBytes: -1,
	})
	require.NoError(t, err)
	node := &Node{reader: reader, entry: rootfshead.Entry{
		Name: "sparse",
		Kind: rootfshead.EntryFile,
		Size: 4096,
		File: &object,
	}}

	first, err := node.fileManifest(context.Background())
	require.NoError(t, err)
	require.NoError(t, store.Delete(object.Key))
	second, err := node.fileManifest(context.Background())
	require.NoError(t, err)
	assert.Same(t, first, second)
}

func TestErrnoMapping(t *testing.T) {
	assert.Equal(t, syscall.ENOENT, errno(rootfsreader.ErrNotFound))
	assert.Equal(t, syscall.EINVAL, errno(rootfsreader.ErrInvalidDirectoryOffset))
	assert.Equal(t, syscall.EINTR, errno(context.Canceled))
	assert.Equal(t, syscall.EIO, errno(assert.AnError))
}
