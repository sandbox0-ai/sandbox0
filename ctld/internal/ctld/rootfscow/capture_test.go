package rootfscow

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCapturePersistsRegularFileAndMetadata(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("hello rootfs"), 0o640))
	require.NoError(t, unix.Setxattr(path, "user.rootfs-test", []byte("value"), 0))
	capture, store, prefix, editor := newTestCapture(t, root, 4)

	result, err := capture.Path(context.Background(), "file.txt")
	require.NoError(t, err)
	assert.True(t, result.Exists)
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, rootEntry, "file.txt")
	assert.Equal(t, rootfshead.EntryFile, entry.Kind)
	assert.Equal(t, uint32(0o100640), entry.Mode)
	assert.Contains(t, entry.XAttrs, rootfshead.XAttr{Name: "user.rootfs-test", Value: []byte("value")})
	assert.Equal(t, []byte("hello rootfs"), readFileEntry(t, store, prefix, entry))
}

func TestCapturePreservesSparseExtentsAndBlocks(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sparse.bin")
	file, err := os.Create(path)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("tail"), 16<<20)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	capture, store, prefix, editor := newTestCapture(t, root, 1<<20)

	_, err = capture.Path(context.Background(), "sparse.bin")
	require.NoError(t, err)
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, rootEntry, "sparse.bin")
	payload, err := rootfsstore.Read(context.Background(), store, prefix, *entry.File)
	require.NoError(t, err)
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, uint64((16<<20)+4), manifest.Size)
	assert.Less(t, manifest.Blocks, manifest.Size/512)
	require.NotEmpty(t, manifest.Extents)
	assert.GreaterOrEqual(t, manifest.Extents[0].Offset, uint64(16<<20))
}

func TestCaptureDefaultChunkSizeBoundsColdReadAmplification(t *testing.T) {
	root := t.TempDir()
	payload := bytes.Repeat([]byte("x"), 3*defaultChunkSize+17)
	path := filepath.Join(root, "large.bin")
	require.NoError(t, os.WriteFile(path, payload, 0o644))
	capture, store, prefix, editor := newTestCapture(t, root, 0)

	_, err := capture.Path(context.Background(), "large.bin")
	require.NoError(t, err)
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, rootEntry, "large.bin")
	manifestPayload, err := rootfsstore.Read(context.Background(), store, prefix, *entry.File)
	require.NoError(t, err)
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(manifestPayload))
	require.NoError(t, err)
	require.Len(t, manifest.Extents, 4)
	for _, extent := range manifest.Extents {
		assert.LessOrEqual(t, extent.Object.Size, int64(defaultChunkSize))
	}
}

func TestCaptureHardlinksShareInodeIdentity(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	require.NoError(t, os.WriteFile(first, []byte("same inode"), 0o644))
	require.NoError(t, os.Link(first, second))
	capture, store, prefix, editor := newTestCapture(t, root, 1<<20)
	require.NoError(t, capture.CaptureTree(context.Background()))
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	firstEntry := mustFindEntry(t, store, prefix, rootEntry, "first")
	secondEntry := mustFindEntry(t, store, prefix, rootEntry, "second")
	assert.Equal(t, firstEntry.Inode, secondEntry.Inode)
	assert.Equal(t, uint32(2), firstEntry.Nlink)
}

func TestCaptureHardlinkAliasesReuseCurrentInodeManifest(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	require.NoError(t, os.WriteFile(first, []byte("same inode"), 0o644))
	require.NoError(t, os.Link(first, second))
	capture, store, prefix, editor := newTestCapture(t, root, 1<<20)
	require.NoError(t, capturePath(t, capture, "first"))
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	firstEntry := mustFindEntry(t, store, prefix, rootEntry, "first")
	require.NotNil(t, firstEntry.File)
	manifestPayload, err := rootfsstore.Read(context.Background(), store, prefix, *firstEntry.File)
	require.NoError(t, err)
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(manifestPayload))
	require.NoError(t, err)
	require.Len(t, manifest.Extents, 1)
	require.Greater(t, manifest.Extents[0].Length, uint64(1))
	extent := manifest.Extents[0]
	manifest.Extents = []rootfshead.FileExtent{
		{Offset: extent.Offset, Length: 1, ObjectOffset: extent.ObjectOffset, Object: extent.Object},
		{Offset: extent.Offset + 1, Length: extent.Length - 1, ObjectOffset: extent.ObjectOffset + 1, Object: extent.Object},
	}
	alternatePayload, err := rootfshead.EncodeFileManifest(manifest)
	require.NoError(t, err)
	alternate, err := capture.writer.Put(context.Background(), rootfshead.FileMediaType, alternatePayload)
	require.NoError(t, err)
	version, exists, err := capture.Version("second")
	require.NoError(t, err)
	require.True(t, exists)
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	capture.fileMu.Lock()
	capture.fileCache[identity] = capturedFileManifest{version: version, object: alternate}
	capture.fileMu.Unlock()

	require.NoError(t, capturePath(t, capture, "second"))
	rootEntry, err = editor.Flush(context.Background())
	require.NoError(t, err)
	secondEntry := mustFindEntry(t, store, prefix, rootEntry, "second")
	assert.Equal(t, alternate, *secondEntry.File)
	assert.Equal(t, []byte("same inode"), readFileEntry(t, store, prefix, secondEntry))
}

func TestCaptureMasksSocketWithWhiteout(t *testing.T) {
	root := t.TempDir()
	socketPath := filepath.Join(root, "server.sock")
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	defer unix.Close(fd)
	require.NoError(t, unix.Bind(fd, &unix.SockaddrUnix{Name: socketPath}))
	capture, store, prefix, editor := newTestCapture(t, root, 1<<20)
	_, err = capture.Path(context.Background(), "server.sock")
	require.NoError(t, err)
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, rootEntry, "server.sock")
	assert.Equal(t, rootfshead.EntryWhiteout, entry.Kind)
	assert.Equal(t, uint32(syscall.S_IFCHR), entry.Mode&syscall.S_IFMT)
	assert.Zero(t, entry.Rdev)
}

func TestOpenFileNoAtimeDoesNotFollowSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	require.NoError(t, os.WriteFile(target, []byte("host-sensitive"), 0o600))
	link := filepath.Join(root, "link")
	require.NoError(t, os.Symlink(target, link))

	file, err := openFileNoAtime(link)
	if file != nil {
		_ = file.Close()
	}
	assert.ErrorIs(t, err, syscall.ELOOP)
}

func TestClassifyOverlayXAttrRejectsMetacopyAndDropsLocalMetadata(t *testing.T) {
	action, err := classifyOverlayXAttr("trusted.overlay.metacopy", []byte("y"))
	assert.ErrorIs(t, err, ErrUnsupportedOverlayMetadata)
	assert.Equal(t, overlayXAttrKeep, action)
	action, err = classifyOverlayXAttr("trusted.overlay.origin", []byte("digest"))
	require.NoError(t, err)
	assert.Equal(t, overlayXAttrDrop, action)
	action, err = classifyOverlayXAttr("trusted.overlay.uuid", []byte("node-local-overlay-id"))
	require.NoError(t, err)
	assert.Equal(t, overlayXAttrDrop, action)
}

func TestCaptureNormalizesOpaqueDirectoryWithoutPersistingOverlayXAttr(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "opaque")
	require.NoError(t, os.Mkdir(directory, 0o755))
	require.NoError(t, unix.Setxattr(directory, "user.overlay.opaque", []byte("y"), 0))
	capture, store, prefix, editor := newTestCapture(t, root, 1<<20)

	_, err := capture.Path(context.Background(), "opaque")
	require.NoError(t, err)
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, rootEntry, "opaque")
	assert.True(t, entry.Opaque)
	assert.NotContains(t, entry.XAttrs, rootfshead.XAttr{Name: "user.overlay.opaque", Value: []byte("y")})
	require.NoError(t, entry.Validate(false))
}

func TestCaptureNormalizesXAttrWhiteoutAsCharacterDevice(t *testing.T) {
	root := t.TempDir()
	whiteout := filepath.Join(root, "deleted")
	require.NoError(t, os.WriteFile(whiteout, nil, 0o600))
	require.NoError(t, unix.Setxattr(whiteout, "user.overlay.whiteout", nil, 0))
	capture, store, prefix, editor := newTestCapture(t, root, 1<<20)

	_, err := capture.Path(context.Background(), "deleted")
	require.NoError(t, err)
	rootEntry, err := editor.Flush(context.Background())
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, rootEntry, "deleted")
	assert.Equal(t, rootfshead.EntryWhiteout, entry.Kind)
	assert.Equal(t, uint32(syscall.S_IFCHR), entry.Mode&uint32(syscall.S_IFMT))
	assert.Zero(t, entry.Rdev)
	assert.Empty(t, entry.XAttrs)
	require.NoError(t, entry.Validate(false))
}

func TestCaptureVersionChangesAfterWrite(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "value")
	require.NoError(t, os.WriteFile(path, []byte("one"), 0o644))
	capture, _, _, _ := newTestCapture(t, root, 1<<20)
	before, exists, err := capture.Version("value")
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, os.WriteFile(path, []byte("two-two"), 0o644))
	after, exists, err := capture.Version("value")
	require.NoError(t, err)
	require.True(t, exists)
	assert.NotEqual(t, before, after)
}

func newTestCapture(t *testing.T, root string, chunkSize int) (*Capture, objectstore.Store, string, *Editor) {
	t.Helper()
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(t, err)
	capture, err := NewCapture(CaptureConfig{
		Root:         root,
		GenerationID: "generation-1",
		ChunkSize:    chunkSize,
		Editor:       editor,
		Writer:       writer,
	})
	require.NoError(t, err)
	return capture, store, writer.Prefix(), editor
}

func capturePath(t *testing.T, capture *Capture, relative string) error {
	t.Helper()
	_, err := capture.Path(context.Background(), relative)
	return err
}

func readFileEntry(t *testing.T, store objectstore.Store, prefix string, entry rootfshead.Entry) []byte {
	t.Helper()
	require.NotNil(t, entry.File)
	payload, err := rootfsstore.Read(context.Background(), store, prefix, *entry.File)
	require.NoError(t, err)
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
	require.NoError(t, err)
	result := make([]byte, manifest.Size)
	for _, extent := range manifest.Extents {
		chunk, err := rootfsstore.Read(context.Background(), store, prefix, extent.Object)
		require.NoError(t, err)
		copy(result[extent.Offset:extent.Offset+extent.Length], chunk[extent.ObjectOffset:extent.ObjectOffset+extent.Length])
	}
	return result
}

func TestVersionFromInfoIncludesAllocatedBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
	info, err := os.Lstat(path)
	require.NoError(t, err)
	version, err := versionFromInfo(info)
	require.NoError(t, err)
	stat := info.Sys().(*syscall.Stat_t)
	assert.Equal(t, uint64(stat.Ino), version.Inode)
}
