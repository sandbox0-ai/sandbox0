package s0fs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

type copyKeyEncryptor struct{}

func (copyKeyEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (copyKeyEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

func testEncryptionConfig(chunkSize uint64) *EncryptionConfig {
	return testEncryptionConfigWithAlgorithm(objectstore.EncryptionAlgoAES256GCMRSA, chunkSize)
}

func testEncryptionConfigWithAlgorithm(algorithm string, chunkSize uint64) *EncryptionConfig {
	return &EncryptionConfig{
		Enabled:          true,
		Algorithm:        algorithm,
		KeyEncryptor:     copyKeyEncryptor{},
		SegmentChunkSize: chunkSize,
	}
}

func (s *recordingStore) clearGets() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = nil
}

func (s *recordingStore) snapshotGets() []getCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]getCall(nil), s.gets...)
}

func TestEncryptedS0FSObjectsAndLocalStateHidePlaintext(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewMemoryStore(t.Name())
	heads := newMemoryHeadStore()
	secretName := "secret-name.txt"
	secretPayload := []byte("secret payload that must not appear in object or local cache bytes")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-enc",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
		Encryption:  testEncryptionConfig(16),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, secretName, 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, secretPayload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	assertFileDoesNotContain(t, filepath.Join(dir, "engine.wal"), []byte(secretName))
	assertFileDoesNotContain(t, filepath.Join(dir, "engine.wal"), secretPayload)

	snapshotState, err := engine.CreateSnapshot("snap-1")
	if err != nil {
		t.Fatalf("CreateSnapshot() error = %v", err)
	}
	if got := string(snapshotState.Data[node.Inode]); got != string(secretPayload) {
		t.Fatalf("snapshot data = %q, want %q", got, string(secretPayload))
	}
	assertFileDoesNotContain(t, filepath.Join(dir, "snapshots", "snap-1.json"), []byte(secretName))
	assertFileDoesNotContain(t, filepath.Join(dir, "snapshots", "snap-1.json"), secretPayload)

	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	if manifest == nil {
		t.Fatal("SyncMaterialize() returned nil manifest")
	}
	assertFileDoesNotContain(t, filepath.Join(dir, "head.json"), []byte(secretName))
	assertFileDoesNotContain(t, filepath.Join(dir, "head.json"), secretPayload)
	assertObjectDoesNotContain(t, store, manifestKey(manifest.ManifestSeq), []byte(secretName))
	assertObjectDoesNotContain(t, store, manifestKey(manifest.ManifestSeq), secretPayload)
	if len(manifest.State.Segments) != 1 {
		t.Fatalf("manifest segment count = %d, want 1", len(manifest.State.Segments))
	}
	for _, segment := range manifest.State.Segments {
		if segment.Encryption == nil {
			t.Fatal("expected materialized segment to include encryption metadata")
		}
		assertObjectDoesNotContain(t, store, segment.Key, secretPayload)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(ctx, Config{
		VolumeID:    "vol-enc",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
		Encryption:  testEncryptionConfig(16),
	})
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer reopened.Close()
	reopenedNode, err := reopened.Lookup(RootInode, secretName)
	if err != nil {
		t.Fatalf("Lookup() after reopen error = %v", err)
	}
	got, err := reopened.Read(reopenedNode.Inode, 0, uint64(len(secretPayload)))
	if err != nil {
		t.Fatalf("Read() after reopen error = %v", err)
	}
	if !bytes.Equal(got, secretPayload) {
		t.Fatalf("reopened payload = %q, want %q", string(got), string(secretPayload))
	}
}

func TestEncryptedS0FSUsesStoredAlgorithmsWhenConfigChanges(t *testing.T) {
	ctx := context.Background()
	previousCacheMax := segmentCacheMaxBytes
	segmentCacheMaxBytes = 1
	defer func() { segmentCacheMaxBytes = previousCacheMax }()

	dir := t.TempDir()
	store := objectstore.NewMemoryStore(t.Name())
	heads := newMemoryHeadStore()
	payload := []byte("chacha-encrypted payload read through aes default config")

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-algorithm",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
		Encryption:  testEncryptionConfigWithAlgorithm(objectstore.EncryptionAlgoCHACHA20RSA, 8),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "algorithm.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	manifest, err := engine.SyncMaterialize(ctx)
	if err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	for _, segment := range manifest.State.Segments {
		if segment.Encryption == nil || segment.Encryption.Algorithm != objectstore.EncryptionAlgoCHACHA20RSA {
			t.Fatalf("segment encryption = %#v, want chacha20-rsa", segment.Encryption)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(ctx, Config{
		VolumeID:    "vol-algorithm",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
		Encryption:  testEncryptionConfigWithAlgorithm(objectstore.EncryptionAlgoAES256GCMRSA, 8),
	})
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	defer reopened.Close()
	readNode, err := reopened.Lookup(RootInode, "algorithm.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	got, err := reopened.Read(readNode.Inode, 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", string(got), string(payload))
	}
}

func TestEncryptedS0FSRequiresEncryptionConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewMemoryStore(t.Name())
	heads := newMemoryHeadStore()

	engine, err := Open(ctx, Config{
		VolumeID:    "vol-requires-config",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
		Encryption:  testEncryptionConfig(8),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	node, err := engine.CreateFile(RootInode, "requires-config.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := engine.Write(node.Inode, 0, []byte("encrypted local state")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = Open(ctx, Config{
		VolumeID:    "vol-requires-config",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err == nil {
		t.Fatal("Open() without encryption config succeeded for encrypted local state")
	}
}

func TestEncryptedSegmentRangeReadFetchesOnlyNeededCiphertextChunks(t *testing.T) {
	ctx := context.Background()
	previousCacheMax := segmentCacheMaxBytes
	segmentCacheMaxBytes = 1
	defer func() { segmentCacheMaxBytes = previousCacheMax }()

	dir := t.TempDir()
	store := &recordingStore{Store: objectstore.NewMemoryStore(t.Name())}
	engine, err := Open(ctx, Config{
		VolumeID:    "vol-range",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   newMemoryHeadStore(),
		Encryption:  testEncryptionConfig(8),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer engine.Close()

	node, err := engine.CreateFile(RootInode, "range.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	payload := []byte("abcdefghijklmnopqrstuvwxyz012345")
	if _, err := engine.Write(node.Inode, 0, payload); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := engine.SyncMaterialize(ctx); err != nil {
		t.Fatalf("SyncMaterialize() error = %v", err)
	}
	store.clearGets()

	got, err := engine.Read(node.Inode, 10, 7)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(got) != string(payload[10:17]) {
		t.Fatalf("range payload = %q, want %q", string(got), string(payload[10:17]))
	}

	gets := store.snapshotGets()
	if len(gets) != 2 {
		t.Fatalf("Get calls = %#v, want two encrypted chunk ranges", gets)
	}
	if gets[0].off != 24 || gets[0].limit != 24 || gets[1].off != 48 || gets[1].limit != 24 {
		t.Fatalf("Get calls = %#v, want ciphertext ranges [24,24] and [48,24]", gets)
	}
}

func TestEncryptedS0FSCanReadLegacyPlaintextStateAndObjects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewMemoryStore(t.Name())
	heads := newMemoryHeadStore()

	legacy, err := Open(ctx, Config{
		VolumeID:    "vol-legacy",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
	})
	if err != nil {
		t.Fatalf("legacy Open() error = %v", err)
	}
	node, err := legacy.CreateFile(RootInode, "legacy.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := legacy.Write(node.Inode, 0, []byte("legacy plaintext")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := legacy.SyncMaterialize(ctx); err != nil {
		t.Fatalf("legacy SyncMaterialize() error = %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("legacy Close() error = %v", err)
	}

	encrypted, err := Open(ctx, Config{
		VolumeID:    "vol-legacy",
		WALPath:     filepath.Join(dir, "engine.wal"),
		ObjectStore: store,
		HeadStore:   heads,
		Encryption:  testEncryptionConfig(8),
	})
	if err != nil {
		t.Fatalf("encrypted Open() error = %v", err)
	}
	defer encrypted.Close()
	readNode, err := encrypted.Lookup(RootInode, "legacy.txt")
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	got, err := encrypted.Read(readNode.Inode, 0, uint64(len("legacy plaintext")))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(got) != "legacy plaintext" {
		t.Fatalf("payload = %q, want legacy plaintext", string(got))
	}
}

func assertFileDoesNotContain(t *testing.T, path string, needle []byte) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if bytes.Contains(payload, needle) {
		t.Fatalf("%s contains plaintext %q", path, string(needle))
	}
}

func assertObjectDoesNotContain(t *testing.T, store objectstore.Store, key string, needle []byte) {
	t.Helper()
	reader, err := store.Get(key, 0, -1)
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read object %s: %v", key, err)
	}
	if bytes.Contains(payload, needle) {
		t.Fatalf("object %s contains plaintext %q", key, string(needle))
	}
}
