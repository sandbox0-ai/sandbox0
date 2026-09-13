package rootfsblock

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type groupCipherSource struct {
	objectstore.ContextConditionalStore
	pack  string
	mu    sync.Mutex
	calls []adaptiveGet
}

func (s *groupCipherSource) Get(key string, offset, length int64) (io.ReadCloser, error) {
	return s.GetContext(context.Background(), key, offset, length)
}
func (s *groupCipherSource) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if key == s.pack {
		s.mu.Lock()
		s.calls = append(s.calls, adaptiveGet{offset, length})
		s.mu.Unlock()
	}
	return s.ContextConditionalStore.GetContext(ctx, key, offset, length)
}

func TestMappingGroupEncryptedDelivery(t *testing.T) {
	store, d, children, pack := groupFixture(t, true, "encrypted")
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	encryptor, err := objectstore.NewKeyEncryptor(string(pemKey), "")
	require.NoError(t, err)
	for _, algorithm := range []string{objectstore.EncryptionAlgoAES256GCMRSA, objectstore.EncryptionAlgoCHACHA20RSA} {
		for _, corrupt := range []bool{false, true} {
			t.Run(algorithm+map[bool]string{false: "/healthy", true: "/corrupt-neighbor-frame"}[corrupt], func(t *testing.T) {
				raw := &groupCipherSource{ContextConditionalStore: objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore), pack: pack}
				cfg := objectstore.EncryptionConfig{Enabled: true, Algorithm: algorithm, KeyEncryptor: encryptor, ChunkSize: 16 << 10}
				writer := objectstore.Encrypting(raw, cfg)
				for k, v := range store.objects {
					if strings.HasPrefix(k, "maps/") {
						require.NoError(t, writer.Put(k, bytes.NewReader(v)))
					}
				}
				if corrupt {
					body, err := raw.ContextConditionalStore.Get(pack, 0, -1)
					require.NoError(t, err)
					ciphertext, err := io.ReadAll(body)
					require.NoError(t, err)
					require.NoError(t, body.Close())
					ciphertext[len(ciphertext)-1] ^= 1
					require.NoError(t, raw.Put(pack, bytes.NewReader(ciphertext)))
				}
				// Match current RootFS crypto policy, with a cold wrapper per cohort.
				encrypted := objectstore.EncryptingImmutable(raw, cfg, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 1024, MaxBytes: 8 << 20, MaxPrefixBytes: 256 << 10, MaxParallelReadBytes: 256 << 10})
				r, err := NewReader(encrypted, d, DefaultReadCacheBytes)
				require.NoError(t, err)
				require.NoError(t, groupRead(r, r.root, children[0].LogicalStart), "corrupt trailing frame must not break an independently valid demanded page")
				if corrupt {
					require.Error(t, groupRead(r, r.root, children[3].LogicalStart))
					return
				}
				for _, child := range children[1:] {
					require.NoError(t, groupRead(r, r.root, child.LogicalStart))
				}
				raw.mu.Lock()
				calls := append([]adaptiveGet(nil), raw.calls...)
				raw.mu.Unlock()
				want := 4
				if groupCandidate() {
					want = 1
				}
				require.Len(t, calls, want)
				var bytes int64
				for _, call := range calls {
					bytes += call.length
				}
				t.Logf("cipher_provider_calls=%d requested_cipher_bytes=%d", len(calls), bytes)
			})
		}
	}
}
