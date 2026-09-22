package objectstore

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type encryptedWriteFailure struct{ err error }

func (w encryptedWriteFailure) Write([]byte) (int, error) { return 0, w.err }

type encryptedWriteCounter struct {
	bytes.Buffer
	calls int
}

func (w *encryptedWriteCounter) Write(p []byte) (int, error) { w.calls++; return w.Buffer.Write(p) }

func TestEncryptedScratchFinalFlushFailureIsReturned(t *testing.T) {
	store := &encryptedStore{cfg: EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 16 << 10}}
	injected := errors.New("ciphertext scratch write failed")
	err := store.encryptTo(encryptedWriteFailure{injected}, "key", bytes.NewReader([]byte("small object")))
	if !errors.Is(err, injected) {
		t.Fatalf("lost final ciphertext flush error: %v", err)
	}
}

func TestEncryptedScratchBufferPreservesFramesAcrossPartialTail(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		t.Run(algorithm, func(t *testing.T) {
			const chunk = 16 << 10
			store := &encryptedStore{cfg: EncryptionConfig{Enabled: true, Algorithm: algorithm, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: chunk}}
			plaintext := make([]byte, 9*chunk+117)
			for i := range plaintext {
				plaintext[i] = byte(i*31 + i/chunk)
			}
			out := &encryptedWriteCounter{}
			if err := store.encryptTo(out, "frame-key", bytes.NewReader(plaintext)); err != nil {
				t.Fatal(err)
			}
			if out.calls > 4 {
				t.Fatalf("small frames still generate excessive writes: %d", out.calls)
			}
			base := NewMemoryStore(t.Name())
			if err := base.Put("frame-key", bytes.NewReader(out.Bytes())); err != nil {
				t.Fatal(err)
			}
			readerStore := Encrypting(base, store.cfg)
			reader, err := readerStore.Get("frame-key", 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(reader)
			closeErr := reader.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("decrypt: %v %v", err, closeErr)
			}
			if !bytes.Equal(plaintext, got) {
				t.Fatal("reused ciphertext buffer changed authenticated frames")
			}
		})
	}
}
