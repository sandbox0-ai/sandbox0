package objectstore

import (
	"bytes"
	"io"
	"strings"
	"sync"
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

func TestEncryptedStoreRangeReadsOnlyRequiredCiphertextChunks(t *testing.T) {
	base := &recordingObjectStore{Store: NewMemoryStore(t.Name())}
	keyEncryptor := &countingTestEncryptor{}
	store := Encrypting(base, EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: keyEncryptor,
		ChunkSize:    8,
	})
	if err := store.Put("rootfs/layer.tar", strings.NewReader("0123456789abcdefghijklmnopqrstuv")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	base.resetGets()

	reader, err := store.Get("rootfs/layer.tar", 22, 5)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if string(got) != "mnopq" {
		t.Fatalf("range = %q, want mnopq", got)
	}

	gets := base.getsSnapshot()
	if len(gets) != 3 {
		t.Fatalf("first range Get calls = %#v, want header prefix, header, and ciphertext range", gets)
	}
	for _, get := range gets {
		if get.offset == 0 && get.limit < 0 {
			t.Fatalf("range read downloaded the full encrypted object: %#v", gets)
		}
	}
	if gets[2].offset <= 0 || gets[2].limit <= 0 {
		t.Fatalf("ciphertext Get = %#v, want bounded non-zero range", gets[2])
	}

	base.resetGets()
	reader, err = store.Get("rootfs/layer.tar", 24, 3)
	if err != nil {
		t.Fatalf("cached Get() error = %v", err)
	}
	_, err = io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read cached range: %v", err)
	}
	gets = base.getsSnapshot()
	if len(gets) != 1 || gets[0].offset <= 0 || gets[0].limit <= 0 {
		t.Fatalf("cached range Get calls = %#v, want one bounded ciphertext request", gets)
	}
	if decrypts := keyEncryptor.decryptCount(); decrypts != 1 {
		t.Fatalf("data-key decrypt calls = %d, want one cached unwrap", decrypts)
	}
}

func TestEncryptedStorePlainRangeDoesNotDownloadWholeObject(t *testing.T) {
	base := &recordingObjectStore{Store: NewMemoryStore(t.Name())}
	if err := base.Put("rootfs/plain.tar", strings.NewReader("0123456789abcdef")); err != nil {
		t.Fatalf("raw Put() error = %v", err)
	}
	store := Encrypting(base, EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: reversibleTestEncryptor{},
	})
	base.resetGets()

	reader, err := store.Get("rootfs/plain.tar", 6, 4)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if string(got) != "6789" {
		t.Fatalf("range = %q, want 6789", got)
	}
	gets := base.getsSnapshot()
	if len(gets) != 2 || gets[1] != (objectGet{offset: 6, limit: 4}) {
		t.Fatalf("plaintext range Get calls = %#v, want prefix probe and exact range", gets)
	}
	for _, get := range gets {
		if get.offset == 0 && get.limit < 0 {
			t.Fatalf("plaintext range downloaded the full object: %#v", gets)
		}
	}
}

type objectGet struct {
	offset int64
	limit  int64
}

type recordingObjectStore struct {
	Store
	mu   sync.Mutex
	gets []objectGet
}

func (s *recordingObjectStore) Get(key string, offset, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets = append(s.gets, objectGet{offset: offset, limit: limit})
	s.mu.Unlock()
	return s.Store.Get(key, offset, limit)
}

func (s *recordingObjectStore) resetGets() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = nil
}

func (s *recordingObjectStore) getsSnapshot() []objectGet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]objectGet(nil), s.gets...)
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

type countingTestEncryptor struct {
	mu       sync.Mutex
	decrypts int
}

func (*countingTestEncryptor) Encrypt(in []byte) ([]byte, error) {
	return reversibleTestEncryptor{}.Encrypt(in)
}

func (e *countingTestEncryptor) Decrypt(in []byte) ([]byte, error) {
	e.mu.Lock()
	e.decrypts++
	e.mu.Unlock()
	return reversibleTestEncryptor{}.Decrypt(in)
}

func (e *countingTestEncryptor) decryptCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.decrypts
}
