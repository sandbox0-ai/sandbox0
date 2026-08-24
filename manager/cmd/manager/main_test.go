package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	managerconfig "github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

func TestWrapRootFSObjectStoreEncryptionReadsLogicalRanges(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test encryption key: %v", err)
	}
	keyPath := filepath.Join(t.TempDir(), "rootfs-object-key.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write test encryption key: %v", err)
	}

	encryptionConfig := managerconfig.RootFSObjectStorageConfig{
		ObjectEncryptionEnabled: true,
		ObjectEncryptionKeyPath: keyPath,
		ObjectEncryptionAlgo:    "aes256gcm-rsa",
	}
	rawStore := objectstore.NewMemoryStore("rootfs")
	ctldStore, err := wrapRootFSObjectStoreEncryption(rawStore, encryptionConfig)
	if err != nil {
		t.Fatalf("wrap ctld rootfs object store: %v", err)
	}
	managerStore, err := wrapRootFSObjectStoreEncryption(rawStore, encryptionConfig)
	if err != nil {
		t.Fatalf("wrap manager rootfs object store: %v", err)
	}
	if !strings.HasPrefix(managerStore.String(), "encrypted(") {
		t.Fatalf("expected encrypted object store, got %q", managerStore.String())
	}

	const objectKey = "rootfs/objects/sha256/pack"
	want := []byte("rootfs block pack")
	if err := ctldStore.Put(objectKey, bytes.NewReader(want)); err != nil {
		t.Fatalf("put encrypted rootfs object: %v", err)
	}
	reader, err := managerStore.Get(objectKey, 0, -1)
	if err != nil {
		t.Fatalf("get encrypted rootfs object: %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read encrypted rootfs object: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("rootfs object = %q, want %q", got, want)
	}

	rangeReader, err := managerStore.Get(objectKey, 2, 5)
	if err != nil {
		t.Fatalf("get encrypted rootfs object range: %v", err)
	}
	defer rangeReader.Close()
	rangeGot, err := io.ReadAll(rangeReader)
	if err != nil {
		t.Fatalf("read encrypted rootfs object range: %v", err)
	}
	if rangeWant := want[2:7]; !bytes.Equal(rangeGot, rangeWant) {
		t.Fatalf("rootfs object range = %q, want %q", rangeGot, rangeWant)
	}
}
