package rootfscow

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestSessionBuildsCompleteHeadWithStructuralSharing(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	firstUpper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(firstUpper, "workspace"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(firstUpper, "workspace", "unchanged"), bytes.Repeat([]byte("a"), 6<<20), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(firstUpper, "workspace", "changed"), bytes.Repeat([]byte("b"), 6<<20), 0o600))

	firstSession := newTestSession(t, store, firstUpper, nil, nil)
	first, err := firstSession.Seal(context.Background(), "head-1")
	require.NoError(t, err)
	require.NoError(t, firstSession.Close())
	unchangedBefore := lookupEntry(t, store, first.Head.Root, "workspace/unchanged")

	secondUpper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(secondUpper, "workspace"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(secondUpper, "workspace", "changed"), bytes.Repeat([]byte("c"), 6<<20), 0o600))
	secondSession := newTestSession(t, store, secondUpper, &first.Head, nil)
	second, err := secondSession.Seal(context.Background(), "head-2")
	require.NoError(t, err)
	require.NoError(t, secondSession.Close())

	unchangedAfter := lookupEntry(t, store, second.Head.Root, "workspace/unchanged")
	changedAfter := lookupEntry(t, store, second.Head.Root, "workspace/changed")
	require.NotNil(t, unchangedBefore.File)
	require.NotNil(t, unchangedAfter.File)
	require.NotNil(t, changedAfter.File)
	assert.Equal(t, unchangedBefore.File.Digest, unchangedAfter.File.Digest, "unchanged subtree must be structurally shared")
	assert.NotEqual(t, unchangedAfter.File.Digest, changedAfter.File.Digest)
	assert.Less(t, second.CreatedBytes, first.CreatedBytes, "a generation should upload only changed content and metadata")
	assert.Equal(t, first.Head.BaseSnapshotKey, second.Head.BaseSnapshotKey)
	assert.Equal(t, rootfshead.Version, second.Reference.Version)
}

func TestSessionUploadsLargeFileChunksConcurrentlyAndSealsBoundedTail(t *testing.T) {
	base := objectstore.NewMemoryStore(t.Name())
	store := &delayedChunkStore{Store: base, delay: 8 * time.Millisecond}
	upper := t.TempDir()
	payload := make([]byte, 64<<20)
	for chunk := 0; chunk < 16; chunk++ {
		block := payload[chunk*(4<<20) : (chunk+1)*(4<<20)]
		for position := range block {
			block[position] = byte(chunk + 1)
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(upper, "large.bin"), payload, 0o644))
	session := newTestSession(t, store, upper, nil, nil)
	defer session.Close()

	deadline := time.Now().Add(5 * time.Second)
	for store.chunkPuts() < 16 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.GreaterOrEqual(t, store.chunkPuts(), 16, "background sync did not drain the large file")

	started := time.Now()
	result, err := session.Seal(context.Background(), "large-head")
	require.NoError(t, err)
	sealDuration := time.Since(started)
	assert.Less(t, sealDuration, time.Second, "seal must not scale with already uploaded rootfs bytes")
	assert.Greater(t, store.maxConcurrent(), 1, "large single files must use concurrent chunk uploads")
	assert.Less(t, result.Duration, time.Second)
}

func TestSessionDebouncesGrowingFileBeforeUploadingStableChunks(t *testing.T) {
	base := objectstore.NewMemoryStore(t.Name())
	store := &delayedChunkStore{Store: base}
	upper := t.TempDir()
	session := newTestSession(t, store, upper, nil, nil)
	defer session.Close()

	file, err := os.Create(filepath.Join(upper, "growing.bin"))
	require.NoError(t, err)
	for part := 0; part < 8; part++ {
		payload := bytes.Repeat([]byte{byte(part + 1)}, 1<<20)
		_, err = file.Write(payload)
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, file.Close())

	result, err := session.Seal(context.Background(), "growing-head")
	require.NoError(t, err)
	assert.LessOrEqual(t, store.chunkPuts(), 2, "stable chunks should not be re-uploaded for every write event")
	entry := lookupEntry(t, base, result.Head.Root, "growing.bin")
	require.NotNil(t, entry.File)
	payload, err := readObject(context.Background(), base, *entry.File)
	require.NoError(t, err)
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, uint64(8<<20), manifest.Size)
}

func TestSessionCapturesUnboundPortalAsOpaqueCurrentState(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	firstUpper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(firstUpper, "state"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(firstUpper, "state", "old"), []byte("old"), 0o644))
	firstSession := newTestSession(t, store, firstUpper, nil, nil)
	first, err := firstSession.Seal(context.Background(), "portal-parent")
	require.NoError(t, err)
	require.NoError(t, firstSession.Close())

	portal := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(portal, "new"), []byte("new"), 0o600))
	secondSession := newTestSession(t, store, t.TempDir(), &first.Head, []ctldapi.RootFSPortalPath{{
		MountPath: "/state", BackingPath: portal,
	}})
	second, err := secondSession.Seal(context.Background(), "portal-child")
	require.NoError(t, err)
	require.NoError(t, secondSession.Close())

	_, found := findEntry(t, store, second.Head.Root, "state/old")
	assert.False(t, found, "opaque portal root must not resurrect deleted parent entries")
	entry, found := findEntry(t, store, second.Head.Root, "state/new")
	require.True(t, found)
	assert.Equal(t, rootfshead.EntryFile, entry.Kind)
}

func TestSessionPreservesHardlinkSymlinkSparseAndXAttrMetadata(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	upper := t.TempDir()
	original := filepath.Join(upper, "executable")
	require.NoError(t, os.WriteFile(original, []byte("shared-content"), 0o751))
	require.NoError(t, os.Link(original, filepath.Join(upper, "hardlink")))
	require.NoError(t, os.Symlink("executable", filepath.Join(upper, "symlink")))
	sparse, err := os.Create(filepath.Join(upper, "sparse.bin"))
	require.NoError(t, err)
	require.NoError(t, sparse.Truncate(32<<20))
	_, err = sparse.WriteAt([]byte("tail"), (32<<20)-4)
	require.NoError(t, err)
	require.NoError(t, sparse.Close())
	xattrSupported := unix.Lsetxattr(original, "user.sandbox0.test", []byte("value"), 0) == nil

	session := newTestSession(t, store, upper, nil, nil)
	result, err := session.Seal(context.Background(), "metadata-head")
	require.NoError(t, err)
	require.NoError(t, session.Close())

	executable := lookupEntry(t, store, result.Head.Root, "executable")
	hardlink := lookupEntry(t, store, result.Head.Root, "hardlink")
	symlink := lookupEntry(t, store, result.Head.Root, "symlink")
	sparseEntry := lookupEntry(t, store, result.Head.Root, "sparse.bin")
	require.NotNil(t, executable.File)
	require.NotNil(t, hardlink.File)
	assert.Equal(t, executable.Inode, hardlink.Inode)
	assert.Equal(t, uint32(2), executable.Nlink)
	assert.Equal(t, executable.File.Digest, hardlink.File.Digest)
	assert.Equal(t, uint32(0o751), executable.Mode)
	assert.Equal(t, rootfshead.EntrySymlink, symlink.Kind)
	assert.Equal(t, "executable", symlink.Target)
	require.NotNil(t, sparseEntry.File)
	payload, err := readObject(context.Background(), store, *sparseEntry.File)
	require.NoError(t, err)
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, uint64(32<<20), manifest.Size)
	if xattrSupported {
		assert.Contains(t, executable.XAttrs, rootfshead.XAttr{Name: "user.sandbox0.test", Value: []byte("value")})
	}
}

func newTestSession(t *testing.T, store objectstore.Store, root string, parent *rootfshead.Head, portals []ctldapi.RootFSPortalPath) *Session {
	t.Helper()
	session, err := NewSession(context.Background(), SessionConfig{
		Root:            root,
		GenerationID:    strings.ReplaceAll(t.Name(), "/", "-") + time.Now().Format("150405.000000000"),
		TeamID:          "team-1",
		FilesystemID:    "filesystem-1",
		BaseImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaseSnapshotKey: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Parent:          parent,
		PortalPaths:     portals,
		Store:           store,
	})
	require.NoError(t, err)
	return session
}

func lookupEntry(t *testing.T, store objectstore.Store, root rootfshead.Entry, value string) rootfshead.Entry {
	t.Helper()
	entry, found := findEntry(t, store, root, value)
	require.True(t, found, "entry %s was not found", value)
	return entry
}

func findEntry(t *testing.T, store objectstore.Store, root rootfshead.Entry, value string) (rootfshead.Entry, bool) {
	t.Helper()
	current := root
	for _, name := range strings.Split(strings.Trim(value, "/"), "/") {
		if current.Directory == nil {
			return rootfshead.Entry{}, false
		}
		payload, err := readObject(context.Background(), store, *current.Directory)
		require.NoError(t, err)
		index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
		require.NoError(t, err)
		bucket := rootfshead.NameBucket(name)
		var shardObject *rootfshead.Object
		for position := range index.Shards {
			if index.Shards[position].Bucket == bucket {
				shardObject = &index.Shards[position].Object
				break
			}
		}
		if shardObject == nil {
			return rootfshead.Entry{}, false
		}
		payload, err = readObject(context.Background(), store, *shardObject)
		require.NoError(t, err)
		shard, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
		require.NoError(t, err)
		found := false
		for _, candidate := range shard.Entries {
			if candidate.Name == name {
				current = candidate
				found = true
				break
			}
		}
		if !found {
			return rootfshead.Entry{}, false
		}
	}
	return current, true
}

type delayedChunkStore struct {
	objectstore.Store
	delay time.Duration

	mu            sync.Mutex
	active        int
	maximum       int
	completedPuts int
}

func (s *delayedChunkStore) Put(key string, reader io.Reader) error {
	if !strings.Contains(key, "/chunks/") {
		return s.Store.Put(key, reader)
	}
	s.mu.Lock()
	s.active++
	if s.active > s.maximum {
		s.maximum = s.active
	}
	s.mu.Unlock()
	time.Sleep(s.delay)
	err := s.Store.Put(key, reader)
	s.mu.Lock()
	s.active--
	if err == nil {
		s.completedPuts++
	}
	s.mu.Unlock()
	return err
}

func (s *delayedChunkStore) chunkPuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completedPuts
}

func (s *delayedChunkStore) maxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maximum
}
