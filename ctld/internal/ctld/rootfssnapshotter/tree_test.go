package rootfssnapshotter

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfscow"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayerTreeLoadsHeadAndPathMetadataLazily(t *testing.T) {
	baseStore := objectstore.NewMemoryStore(t.Name())
	head, reference := sealUpperHead(t, baseStore, "lazy-head", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", func(root string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, "workspace", "nested"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "workspace", "nested", "file.txt"), []byte("persistent-value"), 0o640))
	})
	store := &recordingStore{Store: baseStore}

	loaded, err := loadHead(context.Background(), store, reference)
	require.NoError(t, err)
	assert.Equal(t, head.HeadID, loaded.HeadID)
	assert.Equal(t, 1, store.getCount(), "attach should read only the bounded head")

	tree, err := LoadLayerTree(context.Background(), store, loaded)
	require.NoError(t, err)
	assert.Equal(t, 1, store.getCount(), "building the lazy tree must not read descendants")

	entry := lookupTreePath(t, tree, "workspace/nested/file.txt")
	assert.Equal(t, uint64(len("persistent-value")), entry.entry.Size)
	readsAfterLookup := store.getCount()
	assert.GreaterOrEqual(t, readsAfterLookup, 7)

	buffer := make([]byte, 6)
	n, err := tree.readEntryRange(context.Background(), entry, buffer, 3)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, "sisten", string(buffer))
	assert.Greater(t, store.getCount(), readsAfterLookup)
	readsAfterFirstRange := store.getCount()
	_, err = tree.readEntryRange(context.Background(), entry, buffer, 4)
	require.NoError(t, err)
	assert.Equal(t, readsAfterFirstRange, store.getCount(), "verified chunks should be served by the shared byte cache")

	beforeCachedLookup := store.getCount()
	_ = lookupTreePath(t, tree, "workspace/nested/file.txt")
	assert.Equal(t, beforeCachedLookup, store.getCount(), "metadata should be served by the bounded LRU")
}

func TestLayerTreeReadsSparseHolesAsZeroes(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	head, _ := sealUpperHead(t, store, "sparse-head", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", func(root string) {
		file, err := os.Create(filepath.Join(root, "sparse.bin"))
		require.NoError(t, err)
		require.NoError(t, file.Truncate(16<<20))
		_, err = file.WriteAt([]byte("tail"), (16<<20)-4)
		require.NoError(t, err)
		require.NoError(t, file.Close())
	})
	tree, err := LoadLayerTree(context.Background(), store, head)
	require.NoError(t, err)
	entry := lookupTreePath(t, tree, "sparse.bin")

	buffer := bytes.Repeat([]byte{0xff}, 12)
	n, err := tree.readEntryRange(context.Background(), entry, buffer, (16<<20)-12)
	require.NoError(t, err)
	assert.Equal(t, 12, n)
	assert.Equal(t, append(make([]byte, 8), []byte("tail")...), buffer)
}

func TestLayerTreeRejectsCorruptHeadPayload(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	_, reference := sealUpperHead(t, store, "corrupt-head", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", func(root string) {
		require.NoError(t, os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0o644))
	})
	require.NoError(t, store.Put(reference.Manifest.Key, bytes.NewReader([]byte("corrupt"))))
	_, err := loadHead(context.Background(), store, reference)
	require.Error(t, err)
	assert.ErrorContains(t, err, "object size")
}

func sealUpperHead(t *testing.T, store objectstore.Store, headID, baseSnapshotKey string, populate func(string)) (rootfshead.Head, rootfshead.HeadReference) {
	t.Helper()
	upper := t.TempDir()
	if populate != nil {
		populate(upper)
	}
	session, err := rootfscow.NewSession(context.Background(), rootfscow.SessionConfig{
		Root:            upper,
		GenerationID:    headID,
		TeamID:          "team-test",
		FilesystemID:    t.Name() + "-filesystem",
		BaseImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaseSnapshotKey: baseSnapshotKey,
		Store:           store,
	})
	require.NoError(t, err)
	result, err := session.Seal(context.Background(), headID)
	require.NoError(t, err)
	require.NoError(t, session.Close())
	return result.Head, result.Reference
}

func lookupTreePath(t *testing.T, tree *LayerTree, value string) *treeEntry {
	t.Helper()
	entry := tree.root
	for _, name := range splitTestPath(value) {
		var err error
		entry, err = tree.lookup(context.Background(), entry.entry.Directory, name)
		require.NoError(t, err)
	}
	return entry
}

func splitTestPath(value string) []string {
	var parts []string
	for _, part := range bytes.Split([]byte(filepath.ToSlash(value)), []byte{'/'}) {
		if len(part) > 0 {
			parts = append(parts, string(part))
		}
	}
	return parts
}

type recordingStore struct {
	objectstore.Store
	mu   sync.Mutex
	gets int
}

func (s *recordingStore) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets++
	s.mu.Unlock()
	return s.Store.Get(key, offset, limit)
}

func (s *recordingStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}
