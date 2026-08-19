//go:build linux

package rootfsrebase

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestScanCapturesSparseExtentsHardlinksSymlinksAndXattrs(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "data"), 0o750))
	sparsePath := filepath.Join(root, "data", "sparse")
	file, err := os.OpenFile(sparsePath, os.O_CREATE|os.O_RDWR, 0o640)
	require.NoError(t, err)
	const logicalSize = int64(1 << 30)
	require.NoError(t, file.Truncate(logicalSize))
	_, err = file.WriteAt(bytes.Repeat([]byte{0x5a}, 4096), logicalSize-4096)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	require.NoError(t, os.Link(sparsePath, filepath.Join(root, "data", "sparse-link")))
	require.NoError(t, os.Symlink("data/sparse", filepath.Join(root, "latest")))
	require.NoError(t, unix.Setxattr(sparsePath, "user.sandbox0", []byte("rebase"), 0))

	first, err := Scan(root)
	require.NoError(t, err)
	second, err := Scan(root)
	require.NoError(t, err)
	firstDigest, err := first.Digest()
	require.NoError(t, err)
	secondDigest, err := second.Digest()
	require.NoError(t, err)
	require.Equal(t, firstDigest, secondDigest)

	byPath := manifestByPath(first)
	sparse := byPath["data/sparse"]
	hardlink := byPath["data/sparse-link"]
	require.Equal(t, NodeRegular, sparse.Type)
	require.Equal(t, logicalSize, sparse.Size)
	require.Equal(t, []byte("rebase"), sparse.Xattrs["user.sandbox0"])
	require.Equal(t, sparse.Inode, hardlink.Inode)
	require.Equal(t, sparse.Extents, hardlink.Extents)
	require.GreaterOrEqual(t, sparse.LinkCount, uint64(2))
	require.NotEmpty(t, sparse.Extents)
	var allocated uint64
	for _, extent := range sparse.Extents {
		allocated += extent.Length
	}
	require.Less(t, allocated, uint64(16<<20), "FIEMAP must represent holes without reading or expanding them")
	require.Equal(t, NodeSymlink, byPath["latest"].Type)
	require.Equal(t, "data/sparse", byPath["latest"].LinkTarget)
}

func TestDirtyFileRangesMapsPhysicalBlocksToEveryHardlink(t *testing.T) {
	manifest := Manifest{Version: ManifestVersion, Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "a", Type: NodeRegular, Size: 8192, Device: 1, Inode: 2, LinkCount: 2,
			Extents: []Extent{{Logical: 0, Physical: 4096, Length: 8192}}},
		{Path: "b", Type: NodeRegular, Size: 8192, Device: 1, Inode: 2, LinkCount: 2,
			Extents: []Extent{{Logical: 0, Physical: 4096, Length: 8192}}},
	}}
	hints, err := DirtyFileRanges(manifest, []uint64{2, 2}, 4096)
	require.NoError(t, err)
	want := []ByteRange{{Offset: 4096, Length: 4096}}
	require.Equal(t, want, hints["a"])
	require.Equal(t, want, hints["b"])
}

func TestDirtyFileRangesUsesRealSparseFIEMAP(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sparse")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	const offset = int64(64 << 20)
	_, err = file.WriteAt(bytes.Repeat([]byte{0x7c}, 4096), offset)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	manifest, err := Scan(root)
	require.NoError(t, err)
	node := manifestByPath(manifest)["sparse"]
	require.NotEmpty(t, node.Extents)
	var extent Extent
	for _, candidate := range node.Extents {
		if uint64(offset) >= candidate.Logical && uint64(offset) < candidate.Logical+candidate.Length {
			extent = candidate
			break
		}
	}
	require.NotZero(t, extent.Length)
	physical := extent.Physical + uint64(offset) - extent.Logical
	hints, err := DirtyFileRanges(*manifest, []uint64{physical / 4096}, 4096)
	require.NoError(t, err)
	require.NotEmpty(t, hints["sparse"])
	rangeStart := hints["sparse"][0].Offset
	rangeEnd := rangeStart + hints["sparse"][0].Length
	require.LessOrEqual(t, rangeStart, uint64(offset))
	require.Greater(t, rangeEnd, uint64(offset))
}

func TestDirtyFileRangesRejectsOverflow(t *testing.T) {
	manifest := Manifest{Version: ManifestVersion, Nodes: []Node{{Path: ".", Type: NodeDirectory, LinkCount: 1}}}
	_, err := DirtyFileRanges(manifest, []uint64{math.MaxUint64}, 4096)
	require.ErrorContains(t, err, "overflows")
}

func TestScanRejectsSymlinkRoot(t *testing.T) {
	realRoot := t.TempDir()
	linkRoot := filepath.Join(t.TempDir(), "root")
	require.NoError(t, os.Symlink(realRoot, linkRoot))
	_, err := Scan(linkRoot)
	require.ErrorContains(t, err, "real directory")
}

func TestDirtyFileRangesClampsBlockAlignedExtentToFileSize(t *testing.T) {
	manifest := Manifest{Version: ManifestVersion, Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "short", Type: NodeRegular, Size: 1, Device: 1, Inode: 2, LinkCount: 1,
			Extents: []Extent{{Logical: 0, Physical: 4096, Length: 4096}}},
	}}
	ranges, err := DirtyFileRanges(manifest, []uint64{1}, 4096)
	require.NoError(t, err)
	require.Equal(t, []ByteRange{{Offset: 0, Length: 1}}, ranges["short"])
}

func manifestByPath(manifest *Manifest) map[string]Node {
	result := make(map[string]Node, len(manifest.Nodes))
	for _, node := range manifest.Nodes {
		result[node.Path] = node
	}
	return result
}
