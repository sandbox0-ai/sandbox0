package rootfsreader

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookupLoadsOnlyMatchingShardAndCachesMetadata(t *testing.T) {
	fixture := newReaderFixture(t)
	fixture.store.resetGets()

	entry, err := fixture.reader.Lookup(context.Background(), fixture.root, fixture.firstName)
	require.NoError(t, err)
	assert.Equal(t, fixture.firstName, entry.Name)
	assert.Equal(t, 2, fixture.store.getCount())

	entry, err = fixture.reader.Lookup(context.Background(), fixture.root, fixture.firstName)
	require.NoError(t, err)
	assert.Equal(t, fixture.firstName, entry.Name)
	assert.Equal(t, 2, fixture.store.getCount())
	_, err = fixture.reader.Lookup(context.Background(), fixture.root, "missing")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestReadDirLoadsAllShardsInNameOrder(t *testing.T) {
	fixture := newReaderFixture(t)
	entries, err := fixture.reader.ReadDir(context.Background(), fixture.root)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Less(t, entries[0].Name, entries[1].Name)
}

func TestOpenDirLoadsOneShardAtATime(t *testing.T) {
	fixture := newReaderFixture(t)
	fixture.store.resetGets()
	iterator, err := fixture.reader.OpenDir(context.Background(), fixture.root)
	require.NoError(t, err)
	assert.Equal(t, 1, fixture.store.getCount(), "OpenDir should load only the directory index")

	_, ok, err := iterator.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 2, fixture.store.getCount(), "the first entry should load only its shard")
	_, ok, err = iterator.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 3, fixture.store.getCount())
	_, ok, err = iterator.Next(context.Background())
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestDirectoryIteratorSeekReplaysStableOffsets(t *testing.T) {
	fixture := newReaderFixture(t)
	iterator, err := fixture.reader.OpenDir(context.Background(), fixture.root)
	require.NoError(t, err)
	first, ok, err := iterator.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	second, ok, err := iterator.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, iterator.Seek(context.Background(), 1))
	replayed, ok, err := iterator.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, second, replayed)

	require.NoError(t, iterator.Seek(context.Background(), 0))
	replayed, ok, err = iterator.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, first, replayed)

	err = iterator.Seek(context.Background(), 3)
	assert.ErrorIs(t, err, ErrInvalidDirectoryOffset)
}

func TestDirectoryViewReusesDecodedShard(t *testing.T) {
	fixture := newReaderFixture(t)
	view, err := fixture.reader.OpenDirectory(context.Background(), fixture.root)
	require.NoError(t, err)
	entry, err := view.Lookup(context.Background(), fixture.firstName)
	require.NoError(t, err)
	fixture.store.resetGets()

	reused, err := view.Lookup(context.Background(), fixture.firstName)
	require.NoError(t, err)
	assert.Equal(t, entry, reused)
	assert.Zero(t, fixture.store.getCount())
	assert.Equal(t, 1, view.order.Len())
}

func TestDirectoryViewRejectsIndexShardBucketMismatch(t *testing.T) {
	fixture := newReaderFixture(t)
	view, err := fixture.reader.OpenDirectory(context.Background(), fixture.root)
	require.NoError(t, err)
	reference := view.index.Shards[0]
	reference.Bucket++
	_, err = view.loadShard(context.Background(), reference)
	assert.ErrorContains(t, err, "does not match index bucket")
}

func TestReadFilePreservesSparseHolesAndSupportsRanges(t *testing.T) {
	fixture := newReaderFixture(t)
	destination := bytes.Repeat([]byte{0xff}, 12)
	n, err := fixture.reader.ReadFile(context.Background(), fixture.file, destination, 0)
	require.NoError(t, err)
	assert.Equal(t, 12, n)
	assert.Equal(t, []byte{'a', 'b', 'c', 'd', 0, 0, 0, 0, 'W', 'X', 'Y', 'Z'}, destination)

	ranged := make([]byte, 8)
	n, err = fixture.reader.ReadFile(context.Background(), fixture.file, ranged, 2)
	require.NoError(t, err)
	assert.Equal(t, 8, n)
	assert.Equal(t, []byte{'c', 'd', 0, 0, 0, 0, 'W', 'X'}, ranged)
}

func TestLoadFileManifestRejectsEntryMetadataMismatch(t *testing.T) {
	fixture := newReaderFixture(t)
	file := fixture.file
	file.Size++
	_, err := fixture.reader.LoadFileManifest(context.Background(), file)
	assert.ErrorContains(t, err, "metadata mismatch")
}

func TestSharedMetadataCacheDoesNotHideDescriptorSizeMismatch(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	_, err := fixture.reader.LoadFileManifest(context.Background(), fixture.file)
	require.NoError(t, err)

	corrupt := fixture.file
	corrupt.File = &rootfshead.Object{
		Key:       fixture.file.File.Key,
		Digest:    fixture.file.File.Digest,
		MediaType: fixture.file.File.MediaType,
		Size:      fixture.file.File.Size + 1,
	}
	_, err = fixture.reader.LoadFileManifest(context.Background(), corrupt)
	require.ErrorContains(t, err, "size")
}

func TestReadRejectsCorruptObject(t *testing.T) {
	fixture := newReaderFixture(t)
	require.NoError(t, fixture.store.Put(fixture.file.File.Key, bytes.NewReader([]byte("corrupt"))))
	reader, err := New(ReaderConfig{Store: fixture.store, Prefix: fixture.prefix, MetadataCacheBytes: -1})
	require.NoError(t, err)
	_, err = reader.LoadFileManifest(context.Background(), fixture.file)
	assert.ErrorContains(t, err, "failed size or digest validation")
}

func TestReadObjectRangeStreamsOneFallbackAfterCacheWriteFailure(t *testing.T) {
	store := &countingStore{Store: objectstore.NewMemoryStore(t.Name())}
	writer, err := rootfsstore.NewTeamWriter(store, "fallback-team")
	require.NoError(t, err)
	object, err := writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("0123456789"))
	require.NoError(t, err)
	cachePath := filepath.Join(t.TempDir(), "cache")
	store.onFirstGet = func() { require.NoError(t, os.WriteFile(cachePath, []byte("not a directory"), 0o600)) }
	reader, err := New(ReaderConfig{
		Store:       store,
		Prefix:      writer.Prefix(),
		ObjectCache: ctldrootfs.NewObjectCache(ctldrootfs.ObjectCacheConfig{Dir: cachePath, MaxBytes: 1 << 20}),
	})
	require.NoError(t, err)
	store.resetGets()
	destination := make([]byte, 4)
	read, err := reader.readObjectRange(context.Background(), object, destination, 3)
	require.NoError(t, err)
	assert.Equal(t, 4, read)
	assert.Equal(t, []byte("3456"), destination)
	assert.Equal(t, 2, store.getCount(), "one failed cache fill and one streamed fallback should not trigger another download")
}

func TestReadVerifiedObjectRangeDoesNotTreatZeroReadAsEOF(t *testing.T) {
	payload := []byte("data")
	object := rootfshead.Object{
		Key:       "object",
		Digest:    digest.FromBytes(payload).String(),
		Size:      int64(len(payload)),
		MediaType: rootfshead.ChunkMediaType,
	}
	reader := &scriptedReader{steps: []readStep{
		{payload: payload},
		{},
		{payload: []byte("x")},
		{err: io.EOF},
	}}
	destination := make([]byte, len(payload))

	_, err := readVerifiedObjectRange(context.Background(), reader, object, destination, 0)
	assert.ErrorContains(t, err, "exceeds declared size")
}

func TestSharedMetadataCacheSeparatesTeams(t *testing.T) {
	store := &countingStore{Store: objectstore.NewMemoryStore(t.Name())}
	payload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    4096,
	})
	require.NoError(t, err)
	shared := NewMetadataCache(1 << 20)
	for _, teamID := range []string{"metadata-team-one", "metadata-team-two"} {
		writer, err := rootfsstore.NewTeamWriter(store, teamID)
		require.NoError(t, err)
		object, err := writer.Put(context.Background(), rootfshead.FileMediaType, payload)
		require.NoError(t, err)
		reader, err := New(ReaderConfig{
			Store:               store,
			Prefix:              writer.Prefix(),
			SharedMetadataCache: shared,
		})
		require.NoError(t, err)
		_, err = reader.LoadFileManifest(context.Background(), rootfshead.Entry{
			Name: "sparse",
			Kind: rootfshead.EntryFile,
			Size: 4096,
			File: &object,
		})
		require.NoError(t, err)
	}
	assert.Equal(t, 2, store.getCount())
}

type readerFixture struct {
	store     *countingStore
	reader    *Reader
	prefix    string
	root      rootfshead.Entry
	file      rootfshead.Entry
	firstName string
}

func newReaderFixture(t testing.TB) readerFixture {
	t.Helper()
	store := &countingStore{Store: objectstore.NewMemoryStore(t.Name())}
	writer, err := rootfsstore.NewTeamWriter(store, "reader-team")
	require.NoError(t, err)
	firstChunk, err := writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("abcd"))
	require.NoError(t, err)
	secondChunk, err := writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("WXYZ"))
	require.NoError(t, err)
	manifestPayload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    12,
		Blocks:  16,
		Extents: []rootfshead.FileExtent{
			{Offset: 0, Length: 4, Object: firstChunk},
			{Offset: 8, Length: 4, Object: secondChunk},
		},
	})
	require.NoError(t, err)
	fileObject, err := writer.Put(context.Background(), rootfshead.FileMediaType, manifestPayload)
	require.NoError(t, err)
	firstName, secondName := namesInDifferentBuckets()
	fileEntry := rootfshead.Entry{
		Name:   firstName,
		Inode:  "inode-file",
		Kind:   rootfshead.EntryFile,
		Mode:   0o100640,
		Nlink:  1,
		Size:   12,
		Blocks: 16,
		File:   &fileObject,
	}
	symlinkEntry := rootfshead.Entry{
		Name:   secondName,
		Inode:  "inode-link",
		Kind:   rootfshead.EntrySymlink,
		Mode:   0o120777,
		Nlink:  1,
		Size:   6,
		Target: "target",
	}
	var refs []rootfshead.ShardRef
	for _, entry := range []rootfshead.Entry{fileEntry, symlinkEntry} {
		bucket := rootfshead.NameBucket(entry.Name)
		payload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
			Version: rootfshead.Version,
			Bucket:  bucket,
			Entries: []rootfshead.Entry{entry},
		})
		require.NoError(t, err)
		object, err := writer.Put(context.Background(), rootfshead.DirectoryShardMediaType, payload)
		require.NoError(t, err)
		refs = append(refs, rootfshead.ShardRef{Bucket: bucket, Object: object})
	}
	if refs[0].Bucket > refs[1].Bucket {
		refs[0], refs[1] = refs[1], refs[0]
	}
	indexPayload, err := rootfshead.EncodeDirectoryIndex(rootfshead.DirectoryIndex{Version: rootfshead.Version, Shards: refs})
	require.NoError(t, err)
	indexObject, err := writer.Put(context.Background(), rootfshead.DirectoryIndexMediaType, indexPayload)
	require.NoError(t, err)
	root := rootfshead.Entry{
		Inode:     "root",
		Kind:      rootfshead.EntryDirectory,
		Mode:      0o040755,
		Nlink:     2,
		Directory: &indexObject,
	}
	reader, err := New(ReaderConfig{Store: store, Prefix: writer.Prefix(), MetadataCacheBytes: 1 << 20})
	require.NoError(t, err)
	return readerFixture{store: store, reader: reader, prefix: writer.Prefix(), root: root, file: fileEntry, firstName: firstName}
}

func namesInDifferentBuckets() (string, string) {
	first := "alpha"
	for index := 0; ; index++ {
		second := "entry-" + string(rune('a'+index))
		if rootfshead.NameBucket(first) != rootfshead.NameBucket(second) {
			return first, second
		}
	}
}

type countingStore struct {
	objectstore.Store
	mu         sync.Mutex
	gets       int
	onFirstGet func()
	getOnce    sync.Once
}

type readStep struct {
	payload []byte
	err     error
}

type scriptedReader struct {
	steps []readStep
}

func (r *scriptedReader) Read(destination []byte) (int, error) {
	if len(r.steps) == 0 {
		return 0, io.EOF
	}
	step := r.steps[0]
	r.steps = r.steps[1:]
	return copy(destination, step.payload), step.err
}

func (s *countingStore) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets++
	onFirstGet := s.onFirstGet
	s.mu.Unlock()
	if onFirstGet != nil {
		s.getOnce.Do(onFirstGet)
	}
	return s.Store.Get(key, offset, limit)
}

func (s *countingStore) resetGets() {
	s.mu.Lock()
	s.gets = 0
	s.mu.Unlock()
}

func (s *countingStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}
