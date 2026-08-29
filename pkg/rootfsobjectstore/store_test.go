package rootfsobjectstore

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

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

func TestWrapEncryptionReadsLogicalRanges(t *testing.T) {
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
	cfg := config.RootFSObjectStorageConfig{
		ObjectEncryptionEnabled: true, ObjectEncryptionKeyPath: keyPath,
		ObjectEncryptionAlgo: "aes256gcm-rsa",
	}
	rawStore := objectstore.NewMemoryStore("rootfs")
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
	want := []byte("rootfs block pack")
	if err := writer.Put(objectKey, bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}
	rangeReader, err := readerStore.Get(objectKey, 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer rangeReader.Close()
	got, err := io.ReadAll(rangeReader)
	if err != nil {
		t.Fatal(err)
	}
	if expected := want[2:7]; !bytes.Equal(got, expected) {
		t.Fatalf("RootFS object range = %q, want %q", got, expected)
	}
}
