package objectstore

import (
	"bytes"
	"context"
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

func TestEncryptedStoreConditionalCreateDoesNotOverwrite(t *testing.T) {
	base := NewMemoryStore(t.Name())
	store := Encrypting(base, EncryptionConfig{
		Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8,
	}).(ConditionalStore)
	created, err := store.PutIfAbsent("rootfs/object", strings.NewReader("first"))
	if err != nil || !created {
		t.Fatalf("first PutIfAbsent() = %v, %v", created, err)
	}
	created, err = store.PutIfAbsent("rootfs/object", strings.NewReader("second"))
	if err != nil || created {
		t.Fatalf("second PutIfAbsent() = %v, %v", created, err)
	}
	reader, err := store.Get("rootfs/object", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(got) != "first" {
		t.Fatalf("Get() = %q, %v, want first", got, err)
	}
}

func TestEncryptedStoreContextConditionalRoundTripAndCancellation(t *testing.T) {
	base := NewMemoryStore(t.Name())
	wrapped := Encrypting(base, EncryptionConfig{
		Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8,
	})
	store, ok := wrapped.(ContextConditionalStore)
	if !ok || !SupportsContextConditionalCreate(wrapped) {
		t.Fatal("encryption wrapper lost contextual conditional access")
	}
	created, err := store.PutIfAbsentContext(t.Context(), "rootfs/object", strings.NewReader("first"))
	if err != nil || !created {
		t.Fatalf("PutIfAbsentContext() = %v, %v", created, err)
	}
	reader, err := store.GetContext(t.Context(), "rootfs/object", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(payload) != "first" {
		t.Fatalf("GetContext() = %q, %v", payload, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PutIfAbsentContext(ctx, "rootfs/canceled", strings.NewReader("secret")); err != context.Canceled {
		t.Fatalf("canceled PutIfAbsentContext() error = %v", err)
	}
	if _, err := base.Get("rootfs/canceled", 0, -1); err == nil {
		t.Fatal("canceled encrypted write created an object")
	}
}

func TestSupportsConditionalCreateFollowsStoreWrappers(t *testing.T) {
	base := NewMemoryStore(t.Name())
	if !SupportsConditionalCreate(base) || !SupportsConditionalCreate(Prefix(base, "rootfs")) {
		t.Fatal("conditional memory store capability was lost through prefix wrapper")
	}
	if !SupportsContextConditionalCreate(base) || !SupportsContextConditionalCreate(Prefix(base, "rootfs")) {
		t.Fatal("contextual conditional memory store capability was lost through prefix wrapper")
	}
	nonConditional := objectStoreWithoutConditionalCreate{Store: base}
	if SupportsConditionalCreate(nonConditional) || SupportsConditionalCreate(Prefix(nonConditional, "rootfs")) {
		t.Fatal("prefix wrapper invented conditional create capability")
	}
	if SupportsContextConditionalCreate(nonConditional) || SupportsContextConditionalCreate(Prefix(nonConditional, "rootfs")) {
		t.Fatal("prefix wrapper invented contextual conditional access")
	}
	encrypted := Encrypting(nonConditional, EncryptionConfig{
		Enabled: true, KeyEncryptor: reversibleTestEncryptor{},
	})
	if SupportsConditionalCreate(encrypted) {
		t.Fatal("encryption wrapper invented conditional create capability")
	}
	if SupportsContextConditionalCreate(encrypted) {
		t.Fatal("encryption wrapper invented contextual conditional access")
	}
}

type objectStoreWithoutConditionalCreate struct{ Store }

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
