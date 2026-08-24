//go:build linux

package rootfsrebase

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const sparseRebaseSize = int64(100 << 30)

func TestApplyMergesDisjointSparseFileChangesWithoutReadingLogicalSize(t *testing.T) {
	oldRoot := t.TempDir()
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	const sourceOffset = int64(64 << 20)
	const targetOffset = int64(128 << 20)
	createSparseFixture(t, oldRoot, nil)
	createSparseFixture(t, sourceRoot, map[int64]byte{sourceOffset: 0x51})
	createSparseFixture(t, targetRoot, map[int64]byte{targetOffset: 0x62})

	request := scanApplyRequest(t, oldRoot, sourceRoot, targetRoot, nil)
	result, err := Apply(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, result.HealthProof, 64)
	proof, err := result.HealthProofBytes()
	require.NoError(t, err)
	require.Len(t, proof, 32)
	require.Equal(t, uint64(4096), result.IO.SourceReadBytes)
	require.Equal(t, uint64(4096), result.IO.WrittenBytes)
	require.Less(t, result.IO.SourceReadBytes, uint64(sparseRebaseSize/1024))
	require.Equal(t, bytes.Repeat([]byte{0x51}, 4096), readAt(t, filepath.Join(targetRoot, "huge"), sourceOffset, 4096))
	require.Equal(t, bytes.Repeat([]byte{0x62}, 4096), readAt(t, filepath.Join(targetRoot, "huge"), targetOffset, 4096))
	info, err := os.Stat(filepath.Join(targetRoot, "huge"))
	require.NoError(t, err)
	require.Equal(t, sparseRebaseSize, info.Size())
}

func TestApplyReportsSameRangeConflictBeforeMutation(t *testing.T) {
	oldRoot := t.TempDir()
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	const offset = int64(64 << 20)
	createSparseFixture(t, oldRoot, nil)
	createSparseFixture(t, sourceRoot, map[int64]byte{offset: 0x51})
	createSparseFixture(t, targetRoot, map[int64]byte{offset: 0x62})

	request := scanApplyRequest(t, oldRoot, sourceRoot, targetRoot, nil)
	result, err := Apply(context.Background(), request)
	require.Nil(t, result)
	var conflict *ConflictError
	require.True(t, errors.As(err, &conflict))
	require.NotEmpty(t, conflict.Conflicts)
	require.Equal(t, bytes.Repeat([]byte{0x62}, 4096), readAt(t, filepath.Join(targetRoot, "huge"), offset, 4096))
}

func TestApplyPreservesFilesystemSemantics(t *testing.T) {
	oldRoot := t.TempDir()
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	createSemanticFixture(t, oldRoot, "old")
	createSemanticFixture(t, sourceRoot, "source")
	createSemanticFixture(t, targetRoot, "target")
	require.NoError(t, unix.Setxattr(filepath.Join(targetRoot, "xattrs"), "user.new-base", []byte("kept"), 0))
	normalizeFixtureTimes(t, targetRoot)

	oldManifest := scanWithLineage(t, oldRoot, "lineage")
	sourceManifest := scanWithLineage(t, sourceRoot, "lineage")
	alignStableRegularIdentities(oldManifest, sourceManifest)
	patchRenameIdentity(oldManifest, "renamed-old", sourceManifest, "renamed-new")
	diff, err := Diff(*oldManifest, *sourceManifest, nil)
	require.NoError(t, err)
	request := ApplyRequest{
		OldRoot: oldRoot, SourceRoot: sourceRoot, TargetRoot: targetRoot,
		Old: *oldManifest, Source: *sourceManifest, Diff: *diff,
	}
	result, err := Apply(context.Background(), request)
	require.NoError(t, err)
	require.Greater(t, result.AppliedChanges, 0)
	require.Len(t, result.HealthProof, 64)

	_, err = os.Lstat(filepath.Join(targetRoot, "deleted"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, []byte("rename-data"), readFile(t, filepath.Join(targetRoot, "renamed-new")))
	_, err = os.Lstat(filepath.Join(targetRoot, "renamed-old"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, "new-target", readlink(t, filepath.Join(targetRoot, "symbolic")))
	require.Equal(t, []byte("source"), getXattr(t, filepath.Join(targetRoot, "xattrs"), "user.state"))
	require.Empty(t, getXattr(t, filepath.Join(targetRoot, "xattrs"), "user.empty"))
	require.Equal(t, []byte("kept"), getXattr(t, filepath.Join(targetRoot, "xattrs"), "user.new-base"))

	anchor, err := os.Stat(filepath.Join(targetRoot, "link-a"))
	require.NoError(t, err)
	link, err := os.Stat(filepath.Join(targetRoot, "link-c"))
	require.NoError(t, err)
	require.True(t, os.SameFile(anchor, link))
	require.Equal(t, int64(4096), statBlocks(t, filepath.Join(targetRoot, "hole"))*512)
	truncated, err := os.Stat(filepath.Join(targetRoot, "truncated"))
	require.NoError(t, err)
	require.Equal(t, int64(4096), truncated.Size())
	require.Equal(t, posixACL(), getXattr(t, filepath.Join(targetRoot, "xattrs"), "system.posix_acl_access"))

	retry, err := Apply(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, result.HealthProof, retry.HealthProof)
	require.Zero(t, retry.AppliedChanges)
	require.Equal(t, len(diff.Changes), retry.ConvergedChanges)
}

func TestSecureRootRejectsSymlinkParent(t *testing.T) {
	target := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(target, "escape")))
	root, err := openSecureRoot(target, true)
	require.NoError(t, err)
	defer root.close()
	_, _, err = root.parent("escape/payload")
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(outside, "payload"))
}

func TestApplyRecreatesOverlayWhiteoutAndOpaqueDirectory(t *testing.T) {
	oldRoot := t.TempDir()
	sourceRoot := t.TempDir()
	targetRoot := t.TempDir()
	whiteout := filepath.Join(sourceRoot, ".wh.deleted")
	if err := unix.Mknod(whiteout, unix.S_IFCHR|0o600, 0); err != nil {
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
			t.Skipf("whiteout creation requires CAP_MKNOD: %v", err)
		}
		require.NoError(t, err)
	}
	opaque := filepath.Join(sourceRoot, "opaque")
	require.NoError(t, os.Mkdir(opaque, 0o700))
	if err := unix.Setxattr(opaque, "trusted.overlay.opaque", []byte("y"), 0); err != nil {
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.ENOTSUP) {
			t.Skipf("opaque xattr requires trusted-xattr support: %v", err)
		}
		require.NoError(t, err)
	}
	normalizeFixtureTimes(t, oldRoot)
	normalizeFixtureTimes(t, sourceRoot)
	normalizeFixtureTimes(t, targetRoot)

	request := scanApplyRequest(t, oldRoot, sourceRoot, targetRoot, nil)
	_, err := Apply(context.Background(), request)
	require.NoError(t, err)
	var stat unix.Stat_t
	require.NoError(t, unix.Lstat(filepath.Join(targetRoot, ".wh.deleted"), &stat))
	require.Equal(t, uint32(unix.S_IFCHR), stat.Mode&unix.S_IFMT)
	require.Zero(t, stat.Rdev)
	require.Equal(t, []byte("y"), getXattr(t, filepath.Join(targetRoot, "opaque"), "trusted.overlay.opaque"))
}

func createSparseFixture(t *testing.T, root string, values map[int64]byte) {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(root, "huge"), os.O_CREATE|os.O_RDWR, 0o640)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(sparseRebaseSize))
	for offset, value := range values {
		_, err := file.WriteAt(bytes.Repeat([]byte{value}, 4096), offset)
		require.NoError(t, err)
	}
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	normalizeFixtureTimes(t, root)
}

func createSemanticFixture(t *testing.T, root, variant string) {
	t.Helper()
	require.NoError(t, os.Mkdir(filepath.Join(root, "dir"), 0o750))
	if variant != "source" {
		writeFile(t, filepath.Join(root, "deleted"), []byte("delete-data"))
		writeFile(t, filepath.Join(root, "renamed-old"), []byte("rename-data"))
	} else {
		writeFile(t, filepath.Join(root, "renamed-new"), []byte("rename-data"))
	}
	writeFile(t, filepath.Join(root, "link-a"), []byte("linked-data"))
	if variant == "source" {
		require.NoError(t, os.Link(filepath.Join(root, "link-a"), filepath.Join(root, "link-c")))
	}
	writeFile(t, filepath.Join(root, "xattrs"), []byte("xattr-data"))
	xattrValue := []byte("old")
	if variant == "source" {
		xattrValue = []byte("source")
	}
	require.NoError(t, unix.Setxattr(filepath.Join(root, "xattrs"), "user.state", xattrValue, 0))
	if variant == "source" {
		require.NoError(t, unix.Setxattr(filepath.Join(root, "xattrs"), "user.empty", nil, 0))
		require.NoError(t, unix.Setxattr(filepath.Join(root, "xattrs"), "system.posix_acl_access", posixACL(), 0))
	}

	writeFile(t, filepath.Join(root, "hole"), bytes.Repeat([]byte{0x71}, 8192))
	if variant == "source" {
		file, err := os.OpenFile(filepath.Join(root, "hole"), os.O_RDWR, 0)
		require.NoError(t, err)
		require.NoError(t, unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 4096, 4096))
		require.NoError(t, file.Sync())
		require.NoError(t, file.Close())
	}
	writeFile(t, filepath.Join(root, "truncated"), bytes.Repeat([]byte{0x37}, 8192))
	if variant == "source" {
		require.NoError(t, os.Truncate(filepath.Join(root, "truncated"), 4096))
	}
	target := "old-target"
	if variant == "source" {
		target = "new-target"
	}
	require.NoError(t, os.Symlink(target, filepath.Join(root, "symbolic")))
	normalizeFixtureTimes(t, root)
}

func posixACL() []byte {
	const (
		aclUserObj  = uint16(0x01)
		aclUser     = uint16(0x02)
		aclGroupObj = uint16(0x04)
		aclMask     = uint16(0x10)
		aclOther    = uint16(0x20)
	)
	value := make([]byte, 4+5*8)
	binary.LittleEndian.PutUint32(value[0:4], 2)
	entries := []struct {
		tag  uint16
		perm uint16
		id   uint32
	}{
		{aclUserObj, 6, ^uint32(0)},
		{aclUser, 4, 1234},
		{aclGroupObj, 4, ^uint32(0)},
		{aclMask, 4, ^uint32(0)},
		{aclOther, 0, ^uint32(0)},
	}
	for index, entry := range entries {
		offset := 4 + index*8
		binary.LittleEndian.PutUint16(value[offset:offset+2], entry.tag)
		binary.LittleEndian.PutUint16(value[offset+2:offset+4], entry.perm)
		binary.LittleEndian.PutUint32(value[offset+4:offset+8], entry.id)
	}
	return value
}

func normalizeFixtureTimes(t *testing.T, root string) {
	t.Helper()
	fixed := unix.NsecToTimespec(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano())
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return unix.UtimesNanoAt(unix.AT_FDCWD, path, []unix.Timespec{fixed, fixed}, unix.AT_SYMLINK_NOFOLLOW)
	}))
}

func scanApplyRequest(t *testing.T, oldRoot, sourceRoot, targetRoot string, dirty map[string][]ByteRange) ApplyRequest {
	t.Helper()
	oldManifest := scanWithLineage(t, oldRoot, "lineage")
	sourceManifest := scanWithLineage(t, sourceRoot, "lineage")
	alignStableRegularIdentities(oldManifest, sourceManifest)
	diff, err := Diff(*oldManifest, *sourceManifest, dirty)
	require.NoError(t, err)
	return ApplyRequest{
		OldRoot: oldRoot, SourceRoot: sourceRoot, TargetRoot: targetRoot,
		Old: *oldManifest, Source: *sourceManifest, Diff: *diff,
	}
}

func alignStableRegularIdentities(old, source *Manifest) {
	type sourceIdentity struct {
		device uint64
		inode  uint64
	}
	type stableIdentity struct {
		inode           uint64
		generation      uint32
		generationKnown bool
	}
	oldByPath := nodesByPath(*old)
	mapping := make(map[sourceIdentity]stableIdentity)
	for _, node := range source.Nodes {
		oldNode, exists := oldByPath[node.Path]
		if exists && node.Type == NodeRegular && oldNode.Type == NodeRegular {
			mapping[sourceIdentity{node.Device, node.Inode}] = stableIdentity{
				inode: oldNode.Inode, generation: oldNode.Generation, generationKnown: oldNode.GenerationKnown,
			}
		}
	}
	for index := range source.Nodes {
		identity, exists := mapping[sourceIdentity{source.Nodes[index].Device, source.Nodes[index].Inode}]
		if exists {
			source.Nodes[index].Inode = identity.inode
			source.Nodes[index].Generation = identity.generation
			source.Nodes[index].GenerationKnown = identity.generationKnown
		}
	}
}

func scanWithLineage(t *testing.T, root, lineage string) *Manifest {
	t.Helper()
	manifest, err := ScanWithOptions(root, ScanOptions{LineageID: lineage})
	require.NoError(t, err)
	return manifest
}

func patchRenameIdentity(old *Manifest, oldPath string, source *Manifest, sourcePath string) {
	oldByPath := nodesByPath(*old)
	oldNode := oldByPath[oldPath]
	for index := range old.Nodes {
		if old.Nodes[index].Path == oldPath {
			old.Nodes[index].Generation = 77
			old.Nodes[index].GenerationKnown = true
			oldNode = old.Nodes[index]
		}
	}
	for index := range source.Nodes {
		if source.Nodes[index].Path == sourcePath {
			source.Nodes[index].Inode = oldNode.Inode
			source.Nodes[index].Generation = oldNode.Generation
			source.Nodes[index].GenerationKnown = true
		}
	}
}

func writeFile(t *testing.T, path string, value []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, value, 0o640))
	file, err := os.Open(path)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	value, err := os.ReadFile(path)
	require.NoError(t, err)
	return value
}

func readAt(t *testing.T, path string, offset int64, length int) []byte {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	value := make([]byte, length)
	_, err = file.ReadAt(value, offset)
	require.NoError(t, err)
	return value
}

func readlink(t *testing.T, path string) string {
	t.Helper()
	value, err := os.Readlink(path)
	require.NoError(t, err)
	return value
}

func getXattr(t *testing.T, path, name string) []byte {
	t.Helper()
	size, err := unix.Getxattr(path, name, nil)
	require.NoError(t, err)
	value := make([]byte, size)
	if size != 0 {
		_, err = unix.Getxattr(path, name, value)
		require.NoError(t, err)
	}
	return value
}

func statBlocks(t *testing.T, path string) int64 {
	t.Helper()
	var stat unix.Stat_t
	require.NoError(t, unix.Stat(path, &stat))
	return stat.Blocks
}
