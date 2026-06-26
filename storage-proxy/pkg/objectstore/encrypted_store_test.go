package objectstore

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestEncryptedStoreRoundTripHidesPlaintext(t *testing.T) {
	base := NewMemoryStore(t.Name())
	store := Encrypting(base, EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: reversibleTestEncryptor{},
		ChunkSize:    8,
	})
	plaintext := []byte("rootfs secret marker across chunks")

	if err := store.Put("rootfs/layer.tar", bytes.NewReader(plaintext)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	rawReader, err := base.Get("rootfs/layer.tar", 0, -1)
	if err != nil {
		t.Fatalf("raw Get() error = %v", err)
	}
	raw, err := io.ReadAll(rawReader)
	_ = rawReader.Close()
	if err != nil {
		t.Fatalf("read raw object: %v", err)
	}
	if bytes.Contains(raw, plaintext) || bytes.Contains(raw, []byte("secret marker")) {
		t.Fatalf("encrypted object contains plaintext: %q", raw)
	}

	reader, err := store.Get("rootfs/layer.tar", 0, -1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read decrypted object: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("decrypted object = %q, want %q", got, plaintext)
	}
}

func TestEncryptedStorePlaintextRange(t *testing.T) {
	base := NewMemoryStore(t.Name())
	store := Encrypting(base, EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: reversibleTestEncryptor{},
		ChunkSize:    5,
	})
	if err := store.Put("rootfs/layer.tar", strings.NewReader("0123456789abcdef")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	reader, err := store.Get("rootfs/layer.tar", 4, 7)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if string(got) != "456789a" {
		t.Fatalf("range = %q, want 456789a", got)
	}
}

func TestEncryptedStoreReadsExistingPlaintextObject(t *testing.T) {
	base := NewMemoryStore(t.Name())
	if err := base.Put("rootfs/plain.tar", strings.NewReader("plaintext")); err != nil {
		t.Fatalf("raw Put() error = %v", err)
	}
	store := Encrypting(base, EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: reversibleTestEncryptor{},
	})

	reader, err := store.Get("rootfs/plain.tar", 5, -1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read plaintext object: %v", err)
	}
	if string(got) != "text" {
		t.Fatalf("plaintext range = %q, want text", got)
	}
}

type reversibleTestEncryptor struct{}

func (reversibleTestEncryptor) Encrypt(in []byte) ([]byte, error) {
	out := append([]byte(nil), in...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (reversibleTestEncryptor) Decrypt(in []byte) ([]byte, error) {
	return reversibleTestEncryptor{}.Encrypt(in)
}
