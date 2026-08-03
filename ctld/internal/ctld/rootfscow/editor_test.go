package rootfscow

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEditorOpaqueDirectoryPreservesCurrentGenerationChildren(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	parentWriter, err := NewObjectWriter(store, "sandbox-rootfs/test")
	require.NoError(t, err)
	parentEditor, err := NewEditor(store, parentWriter, nil)
	require.NoError(t, err)

	require.NoError(t, parentEditor.Set(context.Background(), "state", testDirectoryEntry("parent-state"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "state/inherited.link", testSymlinkEntry("parent-link", "inherited"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "state/nested", testDirectoryEntry("parent-nested"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "state/nested/inherited.link", testSymlinkEntry("parent-nested-link", "nested-inherited"), false))
	parentRoot, err := parentEditor.Flush(context.Background())
	require.NoError(t, err)
	parent := &rootfshead.Head{
		Version:         rootfshead.Version,
		HeadID:          "parent",
		BaseImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaseSnapshotKey: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Root:            parentRoot,
	}

	childWriter, err := NewObjectWriter(store, "sandbox-rootfs/test")
	require.NoError(t, err)
	childEditor, err := NewEditor(store, childWriter, parent)
	require.NoError(t, err)

	// Concurrent capture workers may observe a child before the parent
	// directory's opaque xattr. The later parent update must hide inherited
	// children without discarding children already captured in this generation.
	require.NoError(t, childEditor.Set(context.Background(), "state/current.link", testSymlinkEntry("current-link", "current"), false))
	require.NoError(t, childEditor.Set(context.Background(), "state/nested/current.link", testSymlinkEntry("current-nested-link", "nested-current"), false))
	require.NoError(t, childEditor.Set(context.Background(), "state", testDirectoryEntry("current-state"), true))
	childRoot, err := childEditor.Flush(context.Background())
	require.NoError(t, err)

	_, found := findEntry(t, store, childRoot, "state/inherited.link")
	assert.False(t, found)
	current, found := findEntry(t, store, childRoot, "state/current.link")
	require.True(t, found)
	assert.Equal(t, rootfshead.EntrySymlink, current.Kind)
	assert.Equal(t, "current", current.Target)
	_, found = findEntry(t, store, childRoot, "state/nested/inherited.link")
	assert.False(t, found)
	nestedCurrent, found := findEntry(t, store, childRoot, "state/nested/current.link")
	require.True(t, found)
	assert.Equal(t, "nested-current", nestedCurrent.Target)
}

func testDirectoryEntry(inode string) rootfshead.Entry {
	return rootfshead.Entry{Inode: inode, Kind: rootfshead.EntryDirectory, Mode: 0o751, Nlink: 2}
}

func testSymlinkEntry(inode, target string) rootfshead.Entry {
	return rootfshead.Entry{Inode: inode, Kind: rootfshead.EntrySymlink, Mode: 0o777, Nlink: 1, Target: target}
}
