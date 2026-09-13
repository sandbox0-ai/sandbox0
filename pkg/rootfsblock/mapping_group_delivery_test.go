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

type mappingDeliverySource struct {
	objectstore.ContextConditionalStore
	mu               sync.Mutex
	calls, requested int64
}

func (s *mappingDeliverySource) Get(k string, o, n int64) (io.ReadCloser, error) {
	return s.GetContext(context.Background(), k, o, n)
}
func (s *mappingDeliverySource) GetContext(ctx context.Context, k string, o, n int64) (io.ReadCloser, error) {
	if strings.Contains(k, "/maps/") {
		s.mu.Lock()
		s.calls++
		s.requested += n
		s.mu.Unlock()
	}
	return s.ContextConditionalStore.GetContext(ctx, k, o, n)
}

// The actual materialized-image publisher supplies every locator. This is a
// generated block-image fixture, not XFS, an OCI import, or a startup sample.
func TestMappingPublicationEncryptedDelivery(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encryptor, err := objectstore.NewKeyEncryptor(string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})), "")
	require.NoError(t, err)
	const size = 5121 * LogicalBlockSize
	var counts []int64
	for _, policy := range []string{"", ContiguousMappingV1} {
		t.Run("policy-"+policy, func(t *testing.T) {
			store := newBuildTestStore()
			result, err := BuildMaterializedGeneration(t.Context(), &streamPatternReader{size: size}, size, store, BuildOptions{FormatVersion: 2, DataRangeBytes: LogicalBlockSize, MappingGroupPolicy: policy})
			require.NoError(t, err)
			raw := &mappingDeliverySource{ContextConditionalStore: objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore)}
			cfg := objectstore.EncryptionConfig{Enabled: true, Algorithm: objectstore.EncryptionAlgoAES256GCMRSA, KeyEncryptor: encryptor, ChunkSize: 16 << 10}
			writer := objectstore.Encrypting(raw, cfg)
			for _, ref := range result.References {
				require.NoError(t, writer.Put(ref.Key, bytes.NewReader(store.objects[ref.Key])))
			}
			encrypted := objectstore.EncryptingImmutable(raw, cfg, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 1024, MaxBytes: 8 << 20, MaxPrefixBytes: 256 << 10, MaxParallelReadBytes: 256 << 10})
			r, err := NewReader(encrypted, result.Descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			got, want := make([]byte, size), make([]byte, size)
			_, err = r.ReadAt(got, 0)
			require.NoError(t, err)
			_, err = (&streamPatternReader{size: size}).ReadAt(want, 0)
			require.NoError(t, err)
			require.Equal(t, want, got)
			counts = append(counts, raw.calls)
			t.Logf("block_image_bytes=%d mapping_provider_calls=%d mapping_requested_cipher_bytes=%d metadata_decodes=%d", size, raw.calls, raw.requested, r.cache.decodes.Load())
		})
	}
	require.Equal(t, []int64{7, 3}, counts)
}
