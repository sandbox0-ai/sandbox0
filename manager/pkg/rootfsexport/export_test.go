package rootfsexport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

func TestExportIsDeterministicAndPreservesOverlaySemantics(t *testing.T) {
	t.Parallel()

	store := objectstore.NewMemoryStore(t.Name())
	reference := rootFSExportFixture(t, store, "team-1")
	first, err := Export(context.Background(), store, "team-1", reference)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	second, err := Export(context.Background(), store, "team-1", reference)
	if err != nil {
		t.Fatalf("second Export() error = %v", err)
	}
	if first != second {
		t.Fatalf("exports differ: first=%#v second=%#v", first, second)
	}

	compressed := readStoreObject(t, store, first.Object)
	gzipReader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	uncompressed, err := io.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("read uncompressed export: %v", err)
	}
	if err := gzipReader.Close(); err != nil {
		t.Fatalf("close gzip reader: %v", err)
	}
	if got := digest.FromBytes(uncompressed).String(); got != first.DiffID {
		t.Fatalf("DiffID = %s, want %s", first.DiffID, got)
	}

	type archiveEntry struct {
		header *tar.Header
		body   []byte
	}
	entries := make(map[string]archiveEntry)
	var order []string
	reader := tar.NewReader(bytes.NewReader(uncompressed))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar header: %v", err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read tar body %s: %v", header.Name, err)
		}
		copy := *header
		entries[header.Name] = archiveEntry{header: &copy, body: body}
		order = append(order, header.Name)
	}

	wantOrder := []string{".", "sym", "b", "opaque/", "opaque/.wh..wh..opq", "opaque/.wh.deleted", "opaque/value", "a"}
	if !equalStrings(order, wantOrder) {
		t.Fatalf("archive order = %#v, want %#v", order, wantOrder)
	}
	if got := entries["b"].body; !bytes.Equal(got, []byte{0, 0, 0, 0, 'h', 'e', 'l', 'l', 'o', 0}) {
		t.Fatalf("sparse file body = %v", got)
	}
	if header := entries["a"].header; header.Typeflag != tar.TypeLink || header.Linkname != "b" {
		t.Fatalf("hardlink header = %#v", header)
	}
	if got := []byte(entries["b"].header.Xattrs["security.capability"]); !bytes.Equal(got, []byte{1, 0, 2, 3}) {
		t.Fatalf("binary xattr = %v", got)
	}
	if entries["opaque/.wh..wh..opq"].header.Typeflag != tar.TypeReg || entries["opaque/.wh.deleted"].header.Typeflag != tar.TypeReg {
		t.Fatalf("opaque or whiteout marker is not a regular OCI marker")
	}
}

func TestExportDeduplicatesThroughEncryptedStore(t *testing.T) {
	t.Parallel()

	base := objectstore.NewMemoryStore(t.Name())
	store := objectstore.Encrypting(base, objectstore.EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: exportCopyEncryptor{},
		ChunkSize:    8,
	})
	reference := rootFSExportFixture(t, store, "team-1")
	first, err := Export(context.Background(), store, "team-1", reference)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	physical, err := base.Head(first.Object.Key)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if physical.Size == first.Object.Size {
		t.Fatalf("encrypted physical size unexpectedly equals plaintext size %d", physical.Size)
	}
	second, err := Export(context.Background(), store, "team-1", reference)
	if err != nil {
		t.Fatalf("second Export() error = %v", err)
	}
	if first != second {
		t.Fatalf("exports differ: first=%#v second=%#v", first, second)
	}
}

func TestExportRejectsCorruptImmutableObject(t *testing.T) {
	t.Parallel()

	store := objectstore.NewMemoryStore(t.Name())
	reference := rootFSExportFixture(t, store, "team-1")
	if err := store.Put(reference.Manifest.Key, bytes.NewReader([]byte("corrupt"))); err != nil {
		t.Fatalf("replace fixture object: %v", err)
	}
	if _, err := Export(context.Background(), store, "team-1", reference); err == nil {
		t.Fatal("Export() error = nil, want integrity failure")
	}
}

func TestExportRejectsCrossTeamReference(t *testing.T) {
	t.Parallel()

	store := objectstore.NewMemoryStore(t.Name())
	reference := rootFSExportFixture(t, store, "team-1")
	if _, err := Export(context.Background(), store, "team-2", reference); err == nil {
		t.Fatal("Export() error = nil, want team scope failure")
	}
}

func TestDirectoryIterationRejectsIndexShardBucketMismatch(t *testing.T) {
	t.Parallel()

	store := objectstore.NewMemoryStore(t.Name())
	prefix, err := rootfshead.TeamObjectPrefix("team-1")
	if err != nil {
		t.Fatalf("TeamObjectPrefix() error = %v", err)
	}
	put := func(mediaType string, payload []byte) rootfshead.Object {
		t.Helper()
		digestValue := digest.FromBytes(payload)
		key, keyErr := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		if keyErr != nil {
			t.Fatalf("ObjectKey() error = %v", keyErr)
		}
		if putErr := store.Put(key, bytes.NewReader(payload)); putErr != nil {
			t.Fatalf("store fixture object: %v", putErr)
		}
		return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	}
	bucket := rootfshead.NameBucket("value")
	shardPayload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
		Version: rootfshead.Version,
		Bucket:  bucket,
		Entries: []rootfshead.Entry{{Name: "value", Inode: "inode", Kind: rootfshead.EntryFIFO, Mode: 0o010644}},
	})
	if err != nil {
		t.Fatalf("EncodeDirectoryShard() error = %v", err)
	}
	shardObject := put(rootfshead.DirectoryShardMediaType, shardPayload)
	indexPayload, err := rootfshead.EncodeDirectoryIndex(rootfshead.DirectoryIndex{
		Version: rootfshead.Version,
		Shards:  []rootfshead.ShardRef{{Bucket: bucket + 1, Object: shardObject}},
	})
	if err != nil {
		t.Fatalf("EncodeDirectoryIndex() error = %v", err)
	}
	indexObject := put(rootfshead.DirectoryIndexMediaType, indexPayload)
	writer := archiveWriter{objects: objectLoader{ctx: context.Background(), store: store, prefix: prefix}}
	err = writer.forEachDirectoryEntry(rootfshead.Entry{Directory: &indexObject}, func(rootfshead.Entry) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "does not match index bucket") {
		t.Fatalf("forEachDirectoryEntry() error = %v, want bucket mismatch", err)
	}
}

func TestDirectoryIterationRejectsOCIReservedWhiteoutName(t *testing.T) {
	t.Parallel()

	store := objectstore.NewMemoryStore(t.Name())
	prefix, err := rootfshead.TeamObjectPrefix("team-1")
	if err != nil {
		t.Fatalf("TeamObjectPrefix() error = %v", err)
	}
	put := func(mediaType string, payload []byte) rootfshead.Object {
		t.Helper()
		digestValue := digest.FromBytes(payload)
		key, keyErr := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		if keyErr != nil {
			t.Fatalf("ObjectKey() error = %v", keyErr)
		}
		if putErr := store.Put(key, bytes.NewReader(payload)); putErr != nil {
			t.Fatalf("store fixture object: %v", putErr)
		}
		return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	}
	name := ".wh.user-file"
	bucket := rootfshead.NameBucket(name)
	shardPayload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
		Version: rootfshead.Version,
		Bucket:  bucket,
		Entries: []rootfshead.Entry{{Name: name, Inode: "inode", Kind: rootfshead.EntryFIFO, Mode: 0o010644}},
	})
	if err != nil {
		t.Fatalf("EncodeDirectoryShard() error = %v", err)
	}
	shardObject := put(rootfshead.DirectoryShardMediaType, shardPayload)
	indexPayload, err := rootfshead.EncodeDirectoryIndex(rootfshead.DirectoryIndex{
		Version: rootfshead.Version,
		Shards:  []rootfshead.ShardRef{{Bucket: bucket, Object: shardObject}},
	})
	if err != nil {
		t.Fatalf("EncodeDirectoryIndex() error = %v", err)
	}
	indexObject := put(rootfshead.DirectoryIndexMediaType, indexPayload)
	writer := archiveWriter{objects: objectLoader{ctx: context.Background(), store: store, prefix: prefix}}
	err = writer.forEachDirectoryEntry(rootfshead.Entry{Directory: &indexObject}, func(rootfshead.Entry) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "OCI-reserved whiteout name") {
		t.Fatalf("forEachDirectoryEntry() error = %v, want reserved whiteout failure", err)
	}
}

func rootFSExportFixture(t *testing.T, store objectstore.Store, teamID string) rootfshead.HeadReference {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		t.Fatalf("TeamObjectPrefix() error = %v", err)
	}
	put := func(mediaType string, payload []byte) rootfshead.Object {
		t.Helper()
		digestValue := digest.FromBytes(payload)
		key, err := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		if err != nil {
			t.Fatalf("ObjectKey() error = %v", err)
		}
		if err := store.Put(key, bytes.NewReader(payload)); err != nil {
			t.Fatalf("store fixture object: %v", err)
		}
		return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	}

	chunk := put(rootfshead.ChunkMediaType, []byte("hello"))
	filePayload, err := rootfshead.EncodeFileManifest(rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    10,
		Blocks:  8,
		Extents: []rootfshead.FileExtent{{Offset: 4, Length: 5, Object: chunk}},
	})
	if err != nil {
		t.Fatalf("EncodeFileManifest() error = %v", err)
	}
	fileObject := put(rootfshead.FileMediaType, filePayload)
	regular := func(name, inode string) rootfshead.Entry {
		return rootfshead.Entry{
			Name: name, Inode: inode, Kind: rootfshead.EntryFile, Mode: 0o100640,
			UID: 1000, GID: 1001, Nlink: 2, Size: 10, Blocks: 8,
			XAttrs: []rootfshead.XAttr{{Name: "security.capability", Value: []byte{1, 0, 2, 3}}},
			File:   &fileObject,
		}
	}

	opaqueEntries := []rootfshead.Entry{
		{Name: "deleted", Inode: "whiteout", Kind: rootfshead.EntryWhiteout, Mode: 0o020000},
		regular("value", "value-inode"),
	}
	opaqueEntries[1].Nlink = 1
	opaqueDirectory := putDirectory(t, put, opaqueEntries)
	rootEntries := []rootfshead.Entry{
		regular("a", "hardlink-inode"),
		regular("b", "hardlink-inode"),
		{Name: "opaque", Inode: "opaque-dir", Kind: rootfshead.EntryDirectory, Mode: 0o040755, Nlink: 2, Opaque: true, Directory: &opaqueDirectory},
		{Name: "sym", Inode: "symlink", Kind: rootfshead.EntrySymlink, Mode: 0o120777, Nlink: 1, Target: "a", Size: 1},
	}
	rootDirectory := putDirectory(t, put, rootEntries)
	head := rootfshead.Head{
		Version: rootfshead.Version,
		HeadID:  "head-export",
		Base: rootfshead.BaseIdentity{
			ImageReference: "docker.io/library/busybox:1.36",
			ManifestDigest: digest.FromString("base manifest").String(),
			ChainID:        digest.FromString("base chain").String(),
			OS:             "linux",
			Architecture:   "arm64",
			Variant:        "v8",
		},
		Root: rootfshead.Entry{Inode: "root", Kind: rootfshead.EntryDirectory, Mode: 0o040755, Nlink: 2, Directory: &rootDirectory},
	}
	headPayload, err := rootfshead.EncodeHead(head)
	if err != nil {
		t.Fatalf("EncodeHead() error = %v", err)
	}
	return rootfshead.HeadReference{Version: rootfshead.Version, HeadID: head.HeadID, Manifest: put(rootfshead.HeadMediaType, headPayload)}
}

func putDirectory(
	t *testing.T,
	put func(string, []byte) rootfshead.Object,
	entries []rootfshead.Entry,
) rootfshead.Object {
	t.Helper()
	buckets := make(map[uint8][]rootfshead.Entry)
	for _, entry := range entries {
		buckets[rootfshead.NameBucket(entry.Name)] = append(buckets[rootfshead.NameBucket(entry.Name)], entry)
	}
	positions := make([]int, 0, len(buckets))
	for bucket := range buckets {
		positions = append(positions, int(bucket))
	}
	sort.Ints(positions)
	index := rootfshead.DirectoryIndex{Version: rootfshead.Version}
	for _, position := range positions {
		bucket := uint8(position)
		sort.Slice(buckets[bucket], func(i, j int) bool { return buckets[bucket][i].Name < buckets[bucket][j].Name })
		payload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{
			Version: rootfshead.Version,
			Bucket:  bucket,
			Entries: buckets[bucket],
		})
		if err != nil {
			t.Fatalf("EncodeDirectoryShard() error = %v", err)
		}
		index.Shards = append(index.Shards, rootfshead.ShardRef{Bucket: bucket, Object: put(rootfshead.DirectoryShardMediaType, payload)})
	}
	payload, err := rootfshead.EncodeDirectoryIndex(index)
	if err != nil {
		t.Fatalf("EncodeDirectoryIndex() error = %v", err)
	}
	return put(rootfshead.DirectoryIndexMediaType, payload)
}

func readStoreObject(t *testing.T, store objectstore.Store, object rootfshead.Object) []byte {
	t.Helper()
	reader, err := store.Get(object.Key, 0, object.Size)
	if err != nil {
		t.Fatalf("read store object: %v", err)
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read store object payload: %v", err)
	}
	return payload
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for position := range left {
		if left[position] != right[position] {
			return false
		}
	}
	return true
}

type exportCopyEncryptor struct{}

func (exportCopyEncryptor) Encrypt(payload []byte) ([]byte, error) {
	return append([]byte(nil), payload...), nil
}

func (exportCopyEncryptor) Decrypt(payload []byte) ([]byte, error) {
	return append([]byte(nil), payload...), nil
}
