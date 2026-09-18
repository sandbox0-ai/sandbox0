package rootfsobjectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

func TestWrapEncryptionReadsLogicalRanges(t *testing.T) {
	cfg := rootFSObjectEncryptionTestConfig(t)
	rawStore := &countingRootFSStore{ContextConditionalStore: objectstore.NewMemoryStore("rootfs").(objectstore.ContextConditionalStore)}
	writer, err := WrapEncryption(rawStore, cfg)
	if err != nil {
		t.Fatal(err)
	}
	readerStore, err := WrapEncryption(rawStore, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(readerStore.String(), "encrypted(") {
		t.Fatalf("expected encrypted object store, got %q", readerStore.String())
	}
	const objectKey = "rootfs/objects/sha256/pack"
	want := bytes.Repeat([]byte("rootfs block pack"), 1<<17)
	if err := writer.Put(objectKey, bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}
	for i, offset := range []int64{2, (1 << 20) + 2} {
		rangeReader, err := readerStore.Get(objectKey, offset, 5)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rangeReader)
		_ = rangeReader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if expected := want[offset : offset+5]; !bytes.Equal(got, expected) {
			t.Fatalf("RootFS object range = %q, want %q", got, expected)
		}
		if calls := rawStore.gets.Load(); calls != int64(2+i) {
			t.Fatalf("RootFS range %d made %d total GETs, want %d", i, calls, 2+i)
		}
		if limit := rawStore.lastLimit.Load(); limit != rootFSObjectChunkSize+4+16 {
			t.Fatalf("cipher frame range = %d, want 16 KiB payload plus framing and tag", limit)
		}
	}
	// Manager GC and a later publisher can recreate a plaintext content key
	// while ctld keeps its independent wrapper (and old envelope) alive.
	if err := writer.Delete(objectKey); err != nil {
		t.Fatal(err)
	}
	created, err := writer.(objectstore.ContextConditionalStore).PutIfAbsentContext(t.Context(), objectKey, bytes.NewReader(want))
	if err != nil || !created {
		t.Fatalf("RootFS recreation = %v, %v", created, err)
	}
	reader, err := readerStore.Get(objectKey, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("recreated RootFS object: size=%d error=%v", len(got), err)
	}
	if gets := rawStore.gets.Load(); gets != 6 {
		t.Fatalf("recreation used %d total GETs, want 6 with exactly one refresh and retry", gets)
	}
}

func rootFSObjectEncryptionTestConfig(t *testing.T) config.RootFSObjectStorageConfig {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test encryption key: %v", err)
	}
	keyPath := filepath.Join(t.TempDir(), "rootfs-object-key.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write test encryption key: %v", err)
	}
	return config.RootFSObjectStorageConfig{
		ObjectEncryptionEnabled: true, ObjectEncryptionKeyPath: keyPath,
		ObjectEncryptionAlgo: "aes256gcm-rsa",
	}
}

type countingRootFSStore struct {
	objectstore.ContextConditionalStore
	gets      atomic.Int64
	lastLimit atomic.Int64
}

func (s *countingRootFSStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return s.GetContext(context.Background(), key, off, limit)
}

func (s *countingRootFSStore) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	s.gets.Add(1)
	s.lastLimit.Store(limit)
	return s.ContextConditionalStore.GetContext(ctx, key, off, limit)
}
