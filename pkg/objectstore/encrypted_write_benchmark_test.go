package objectstore

import (
	"bytes"
	"context"
	"io"
	"testing"
)

type encryptedWriteBenchmarkSink struct{ ContextConditionalStore }

func (s encryptedWriteBenchmarkSink) PutIfAbsentContext(ctx context.Context, key string, in io.Reader) (bool, error) {
	_, err := io.Copy(io.Discard, in)
	return err == nil, err
}

// BenchmarkEncryptedObjectScratch includes the private ciphertext scratch file
// used by real object uploads. It excludes network/durable-store latency and
// uses fixed-cost test key wrapping to isolate framing, allocation and writes.
func BenchmarkEncryptedObjectScratch(b *testing.B) {
	const size = 8 << 20
	data := bytes.Repeat([]byte{0x9a}, size)
	base := encryptedWriteBenchmarkSink{NewMemoryStore("").(ContextConditionalStore)}
	store := Encrypting(base, EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 16 << 10}).(ContextConditionalStore)
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ok, err := store.PutIfAbsentContext(b.Context(), "isolated-encryption-benchmark", bytes.NewReader(data))
		if err != nil || !ok {
			b.Fatalf("encrypted write: created=%t error=%v", ok, err)
		}
	}
}
