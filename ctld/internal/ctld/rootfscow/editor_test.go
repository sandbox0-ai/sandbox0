package rootfscow

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEditorStructurallySharesUnchangedSubtrees(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	parentWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	parentEditor, err := NewEditor(store, parentWriter, nil)
	require.NoError(t, err)
	require.NoError(t, parentEditor.Set(context.Background(), "left", testDirectoryEntry("left"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "left/a", testSymlinkEntry("a", "old"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "right", testDirectoryEntry("right"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "right/b", testSymlinkEntry("b", "keep"), false))
	parentRoot, err := parentEditor.Flush(context.Background())
	require.NoError(t, err)
	parent := testHead(parentRoot, "parent")

	beforeRight := mustFindEntry(t, store, parentWriter.Prefix(), parentRoot, "right")
	childWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	childEditor, err := NewEditor(store, childWriter, &parent)
	require.NoError(t, err)
	require.NoError(t, childEditor.Set(context.Background(), "left/a", testSymlinkEntry("a-new", "new"), false))
	childRoot, err := childEditor.Flush(context.Background())
	require.NoError(t, err)
	afterRight := mustFindEntry(t, store, childWriter.Prefix(), childRoot, "right")
	assert.Equal(t, beforeRight.Directory, afterRight.Directory)
	assert.Equal(t, "new", mustFindEntry(t, store, childWriter.Prefix(), childRoot, "left/a").Target)
}

func TestEditorOpaqueDirectoryPreservesCurrentGenerationChildren(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	parentWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	parentEditor, err := NewEditor(store, parentWriter, nil)
	require.NoError(t, err)
	require.NoError(t, parentEditor.Set(context.Background(), "state", testDirectoryEntry("parent-state"), false))
	require.NoError(t, parentEditor.Set(context.Background(), "state/inherited", testSymlinkEntry("inherited", "old"), false))
	parentRoot, err := parentEditor.Flush(context.Background())
	require.NoError(t, err)
	parent := testHead(parentRoot, "parent")

	childWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	childEditor, err := NewEditor(store, childWriter, &parent)
	require.NoError(t, err)
	require.NoError(t, childEditor.Set(context.Background(), "state/current", testSymlinkEntry("current", "new"), false))
	require.NoError(t, childEditor.Set(context.Background(), "state", testDirectoryEntry("current-state"), true))
	childRoot, err := childEditor.Flush(context.Background())
	require.NoError(t, err)

	_, found := findEntry(t, store, childWriter.Prefix(), childRoot, "state/inherited")
	assert.False(t, found)
	assert.Equal(t, "new", mustFindEntry(t, store, childWriter.Prefix(), childRoot, "state/current").Target)
	assert.True(t, mustFindEntry(t, store, childWriter.Prefix(), childRoot, "state").Opaque)

	childHead := testHead(childRoot, "child")
	grandchildWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	grandchildEditor, err := NewEditor(store, grandchildWriter, &childHead)
	require.NoError(t, err)
	require.NoError(t, grandchildEditor.Set(context.Background(), "state", testDirectoryEntry("next-state"), false))
	grandchildRoot, err := grandchildEditor.Flush(context.Background())
	require.NoError(t, err)
	assert.True(t, mustFindEntry(t, store, grandchildWriter.Prefix(), grandchildRoot, "state").Opaque)
}

func TestEditorResetRestoresParentEntry(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	parentWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	parentEditor, err := NewEditor(store, parentWriter, nil)
	require.NoError(t, err)
	require.NoError(t, parentEditor.Set(context.Background(), "value", testSymlinkEntry("parent", "old"), false))
	parentRoot, err := parentEditor.Flush(context.Background())
	require.NoError(t, err)
	parent := testHead(parentRoot, "parent")

	childWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	childEditor, err := NewEditor(store, childWriter, &parent)
	require.NoError(t, err)
	require.NoError(t, childEditor.Set(context.Background(), "value", testSymlinkEntry("child", "new"), false))
	require.NoError(t, childEditor.Reset(context.Background(), "value"))
	childRoot, err := childEditor.Flush(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "old", mustFindEntry(t, store, childWriter.Prefix(), childRoot, "value").Target)
}

func TestEditorRejectsParentFromAnotherTeam(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	sourceWriter, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	sourceEditor, err := NewEditor(store, sourceWriter, nil)
	require.NoError(t, err)
	root, err := sourceEditor.Flush(context.Background())
	require.NoError(t, err)
	parent := testHead(root, "source")
	targetWriter, err := rootfsstore.NewTeamWriter(store, "team-2")
	require.NoError(t, err)
	_, err = NewEditor(store, targetWriter, &parent)
	assert.Error(t, err)
}

func TestEditorPropagatesDeepDirtyAndReusesCleanDescriptor(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(t, err)
	require.NoError(t, editor.Set(context.Background(), "a", testDirectoryEntry("a"), false))
	require.NoError(t, editor.Set(context.Background(), "a/b", testDirectoryEntry("b"), false))
	require.NoError(t, editor.Set(context.Background(), "a/b/value", testSymlinkEntry("value", "before"), false))
	first, err := editor.Flush(context.Background())
	require.NoError(t, err)
	require.NotNil(t, first.Directory)
	assert.False(t, editor.dir.dirty)

	second, err := editor.Flush(context.Background())
	require.NoError(t, err)
	assert.Equal(t, first.Directory, second.Directory, "clean flush must reuse the root descriptor")
	assert.False(t, editor.dir.dirty)

	require.NoError(t, editor.Set(context.Background(), "a/b/value", testSymlinkEntry("value", "after"), false))
	assert.True(t, editor.dir.dirty)
	rootShard := editor.dir.shards[rootfshead.NameBucket("a")]
	require.NotNil(t, rootShard)
	assert.True(t, rootShard.dirty)
	a := rootShard.entries["a"]
	require.NotNil(t, a)
	require.NotNil(t, a.child)
	assert.True(t, a.child.dirty)

	third, err := editor.Flush(context.Background())
	require.NoError(t, err)
	assert.NotEqual(t, first.Directory, third.Directory)
	assert.Equal(t, "after", mustFindEntry(t, store, writer.Prefix(), third, "a/b/value").Target)
	assert.False(t, editor.dir.dirty)
}

func BenchmarkEditorCleanFlush(b *testing.B) {
	store := objectstore.NewMemoryStore(b.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "benchmark-team")
	if err != nil {
		b.Fatal(err)
	}
	editor, err := NewEditor(store, writer, nil)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 10_000; index++ {
		name := fmt.Sprintf("file-%05d", index)
		if err := editor.Set(context.Background(), name, testSymlinkEntry(name, "target"), false); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := editor.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := editor.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func testHead(root rootfshead.Entry, id string) rootfshead.Head {
	return rootfshead.Head{
		Version: rootfshead.Version,
		HeadID:  id,
		Base: rootfshead.BaseIdentity{
			ImageReference: "registry.example/base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ChainID:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			OS:             "linux",
			Architecture:   "amd64",
		},
		Root: root,
	}
}

func testDirectoryEntry(inode string) rootfshead.Entry {
	return rootfshead.Entry{Inode: inode, Kind: rootfshead.EntryDirectory, Mode: 0o040751, Nlink: 2}
}

func testSymlinkEntry(inode, target string) rootfshead.Entry {
	return rootfshead.Entry{Inode: inode, Kind: rootfshead.EntrySymlink, Mode: 0o120777, Nlink: 1, Target: target, Size: uint64(len(target))}
}

func mustFindEntry(t *testing.T, store objectstore.Store, prefix string, root rootfshead.Entry, value string) rootfshead.Entry {
	t.Helper()
	entry, ok := findEntry(t, store, prefix, root, value)
	require.True(t, ok, "entry %s not found", value)
	return entry
}

func findEntry(t *testing.T, store objectstore.Store, prefix string, root rootfshead.Entry, value string) (rootfshead.Entry, bool) {
	t.Helper()
	parts, err := splitRelativePath(value)
	require.NoError(t, err)
	current := root
	for _, name := range parts {
		if current.Directory == nil {
			return rootfshead.Entry{}, false
		}
		payload, err := rootfsstore.Read(context.Background(), store, prefix, *current.Directory)
		require.NoError(t, err)
		index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
		require.NoError(t, err)
		bucket := rootfshead.NameBucket(name)
		position := sort.Search(len(index.Shards), func(i int) bool { return index.Shards[i].Bucket >= bucket })
		if position == len(index.Shards) || index.Shards[position].Bucket != bucket {
			return rootfshead.Entry{}, false
		}
		payload, err = rootfsstore.Read(context.Background(), store, prefix, index.Shards[position].Object)
		require.NoError(t, err)
		shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
		require.NoError(t, err)
		entryPosition := sort.Search(len(shard.Entries), func(i int) bool { return shard.Entries[i].Name >= name })
		if entryPosition == len(shard.Entries) || shard.Entries[entryPosition].Name != name {
			return rootfshead.Entry{}, false
		}
		current = shard.Entries[entryPosition]
	}
	return current, true
}

func TestSplitRelativePathRejectsTraversal(t *testing.T) {
	for _, value := range []string{"../x", "a/../x", "a//b"} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			_, err := splitRelativePath(value)
			assert.Error(t, err)
		})
	}
}
