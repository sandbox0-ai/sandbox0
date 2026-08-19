package rootfsrebase

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiffUsesDirtyExtentsAndSparseLayoutWithoutCopyingWholeFiles(t *testing.T) {
	old := Manifest{Version: ManifestVersion, LineageID: "filesystem-1", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "deleted", Type: NodeRegular, Size: 4096, Device: 1, Inode: 4, LinkCount: 1,
			Extents: []Extent{{Logical: 0, Physical: 40 << 12, Length: 4096}}},
		{Path: "file", Type: NodeRegular, Size: 8192, Device: 1, Inode: 2, LinkCount: 1,
			Xattrs:  map[string][]byte{"user.empty": nil},
			Extents: []Extent{{Logical: 0, Physical: 10 << 12, Length: 8192}}},
		{Path: "renamed-old", Type: NodeRegular, Size: 4096, Device: 1, Inode: 5, Generation: 9, GenerationKnown: true, LinkCount: 1,
			Extents: []Extent{{Logical: 0, Physical: 50 << 12, Length: 4096}}},
		{Path: "sparse", Type: NodeRegular, Size: 8192, Device: 1, Inode: 3, LinkCount: 1,
			Extents: []Extent{{Logical: 0, Physical: 30 << 12, Length: 8192}}},
	}}
	source := Manifest{Version: ManifestVersion, LineageID: old.LineageID, Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "added-large-sparse", Type: NodeRegular, Size: 1 << 30, Device: 2, Inode: 6, LinkCount: 1,
			Extents: []Extent{{Logical: (1 << 30) - 4096, Physical: 60 << 12, Length: 4096}}},
		{Path: "file", Type: NodeRegular, Size: 8192, Device: 2, Inode: 2, LinkCount: 2,
			Xattrs:  map[string][]byte{"user.empty": nil, "user.new-empty": nil},
			Extents: []Extent{{Logical: 0, Physical: 10 << 12, Length: 8192}}},
		{Path: "file-link", Type: NodeRegular, Size: 8192, Device: 2, Inode: 2, LinkCount: 2,
			Xattrs:  map[string][]byte{"user.empty": nil, "user.new-empty": nil},
			Extents: []Extent{{Logical: 0, Physical: 10 << 12, Length: 8192}}},
		{Path: "renamed-new", Type: NodeRegular, Size: 4096, Device: 2, Inode: 5, Generation: 9, GenerationKnown: true, LinkCount: 1,
			Extents: []Extent{{Logical: 0, Physical: 50 << 12, Length: 4096}}},
		{Path: "sparse", Type: NodeRegular, Size: 8192, Device: 2, Inode: 3, LinkCount: 1,
			Extents: []Extent{{Logical: 0, Physical: 30 << 12, Length: 4096}}},
	}}
	dirty := map[string][]ByteRange{
		"file": {{Offset: 4096, Length: 4096}},
	}
	result, err := Diff(old, source, dirty)
	require.NoError(t, err)
	require.NoError(t, result.Validate())
	changes := changesByPath(result)
	require.Equal(t, ChangeRemove, changes["deleted"].Kind)
	require.Equal(t, ChangeRename, changes["renamed-new"].Kind)
	require.Equal(t, "renamed-old", changes["renamed-new"].FromPath)
	require.Equal(t, []DataChange{{Offset: 4096, Length: 4096, SourceData: true}}, changes["file"].Data)
	value, exists := changes["file"].Metadata.SetXattrs["user.new-empty"]
	require.True(t, exists)
	require.Empty(t, value)
	require.Equal(t, "file", changes["file-link"].HardlinkTarget)
	require.Equal(t, []DataChange{{Offset: 4096, Length: 4096, SourceData: false}}, changes["sparse"].Data)
	require.Equal(t, []DataChange{{Offset: (1 << 30) - 4096, Length: 4096, SourceData: true}}, changes["added-large-sparse"].Data)
	require.Equal(t, uint64(8192), result.ReadBytes, "the 1 GiB sparse file must contribute only its allocated 4 KiB")
}

func TestDiffCountsChangedHardlinkInodeDataOnce(t *testing.T) {
	old := Manifest{Version: ManifestVersion, Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "a", Type: NodeRegular, Size: 4096, Device: 1, Inode: 8, LinkCount: 2,
			Extents: []Extent{{Logical: 0, Physical: 4096, Length: 4096}}},
		{Path: "b", Type: NodeRegular, Size: 4096, Device: 1, Inode: 8, LinkCount: 2,
			Extents: []Extent{{Logical: 0, Physical: 4096, Length: 4096}}},
	}}
	source := old
	source.Nodes = append([]Node(nil), old.Nodes...)
	source.Nodes[1].Device = 2
	source.Nodes[2].Device = 2
	dirty := map[string][]ByteRange{
		"a": {{Offset: 0, Length: 4096}},
		"b": {{Offset: 0, Length: 4096}},
	}

	result, err := Diff(old, source, dirty)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), result.ReadBytes)
	require.Equal(t, []Change{{
		Kind: ChangeModify, Path: "a",
		Data: []DataChange{{Offset: 0, Length: 4096, SourceData: true}},
	}}, result.Changes)
}

func TestDiffReplacesACommonPathWhenItBecomesAHardlink(t *testing.T) {
	old := Manifest{Version: ManifestVersion, LineageID: "filesystem", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "a", Type: NodeRegular, Device: 1, Inode: 8, Generation: 1, GenerationKnown: true, LinkCount: 1},
		{Path: "b", Type: NodeRegular, Device: 1, Inode: 9, Generation: 2, GenerationKnown: true, LinkCount: 1},
	}}
	source := Manifest{Version: ManifestVersion, LineageID: "filesystem", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "a", Type: NodeRegular, Device: 2, Inode: 8, Generation: 1, GenerationKnown: true, LinkCount: 2},
		{Path: "b", Type: NodeRegular, Device: 2, Inode: 8, Generation: 1, GenerationKnown: true, LinkCount: 2},
	}}

	result, err := Diff(old, source, nil)
	require.NoError(t, err)
	require.Equal(t, []Change{{Kind: ChangeReplace, Path: "b", HardlinkTarget: "a"}}, result.Changes)
}

func TestDiffResultRejectsDuplicatePathsAndOverlappingData(t *testing.T) {
	result := DiffResult{Version: DiffVersion, Changes: []Change{
		{Kind: ChangeRemove, Path: "same"},
		{Kind: ChangeRemove, Path: "same"},
	}}
	require.ErrorContains(t, result.Validate(), "unique")

	result = DiffResult{Version: DiffVersion, ReadBytes: 8, Changes: []Change{{
		Kind: ChangeModify, Path: "file", Data: []DataChange{
			{Offset: 0, Length: 4, SourceData: true},
			{Offset: 2, Length: 4, SourceData: true},
		},
	}}}
	require.ErrorContains(t, result.Validate(), "overlapping")
}

func TestDiffDoesNotInferRenameAcrossDifferentLineages(t *testing.T) {
	old := Manifest{Version: ManifestVersion, LineageID: "old", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "old-name", Type: NodeRegular, Device: 1, Inode: 7, Generation: 1, GenerationKnown: true, LinkCount: 1},
	}}
	source := Manifest{Version: ManifestVersion, LineageID: "source", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "new-name", Type: NodeRegular, Device: 2, Inode: 7, Generation: 1, GenerationKnown: true, LinkCount: 1},
	}}
	result, err := Diff(old, source, nil)
	require.NoError(t, err)
	changes := changesByPath(result)
	require.Equal(t, ChangeRemove, changes["old-name"].Kind)
	require.Equal(t, ChangeAdd, changes["new-name"].Kind)
}

func TestDiffDoesNotInferRenameAfterInodeReuse(t *testing.T) {
	old := Manifest{Version: ManifestVersion, LineageID: "filesystem", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "old-name", Type: NodeRegular, Inode: 7, Generation: 10, GenerationKnown: true, LinkCount: 1},
	}}
	source := Manifest{Version: ManifestVersion, LineageID: "filesystem", Nodes: []Node{
		{Path: ".", Type: NodeDirectory, LinkCount: 1},
		{Path: "new-name", Type: NodeRegular, Inode: 7, Generation: 11, GenerationKnown: true, LinkCount: 1},
	}}
	result, err := Diff(old, source, nil)
	require.NoError(t, err)
	changes := changesByPath(result)
	require.Equal(t, ChangeRemove, changes["old-name"].Kind)
	require.Equal(t, ChangeAdd, changes["new-name"].Kind)
}

func changesByPath(result *DiffResult) map[string]Change {
	changes := make(map[string]Change, len(result.Changes))
	for _, change := range result.Changes {
		changes[change.Path] = change
	}
	return changes
}
