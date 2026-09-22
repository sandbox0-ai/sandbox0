package runtimecheckpoint

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type manifestReadCounter struct {
	objectstore.ContextConditionalStore
	reads atomic.Int32
}

func (s *manifestReadCounter) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	if strings.HasSuffix(key, "/manifest.json") {
		s.reads.Add(1)
	}
	return s.ContextConditionalStore.GetContext(ctx, key, off, limit)
}

func TestManifestCacheAvoidsRepeatedReadsWithoutTrustingLocalChunks(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	counted := &manifestReadCounter{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
	store, err := New(counted, ChunkBytes)
	require.NoError(t, err)
	directory := privateImage(t, map[string][]byte{"pages.img": []byte("memory")})
	ref, err := store.Publish(t.Context(), testBinding(), directory)
	require.NoError(t, err)
	first, err := store.VerifyLocal(t.Context(), testBinding(), ref, directory)
	require.NoError(t, err)
	reads := counted.reads.Load()
	require.Positive(t, reads, "publication alone must not populate the read cache")
	first.Files[0].Chunks[0].Digest = digest.FromString("changed returned manifest").String()
	_, err = store.VerifyLocal(t.Context(), testBinding(), ref, directory)
	require.NoError(t, err, "returned manifests cannot alias cached metadata")
	require.Equal(t, reads, counted.reads.Load(), "restore admission reuses the exact regional manifest")
	require.NoError(t, os.WriteFile(filepath.Join(directory, "pages.img"), []byte("damage"), 0o600))
	_, err = store.VerifyLocal(t.Context(), testBinding(), ref, directory)
	require.Error(t, err, "cached metadata never replaces local content verification")
	require.Equal(t, reads, counted.reads.Load())
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = store.loadManifest(canceled, testBinding(), ref)
	require.ErrorIs(t, err, context.Canceled)
	changed := ref
	changed.ManifestDigest = digest.FromString("another manifest").String()
	_, err = store.loadManifest(t.Context(), testBinding(), changed)
	require.Error(t, err, "a changed reference cannot reuse another cached manifest")
	require.Equal(t, reads+1, counted.reads.Load())
}

func TestManifestCacheRejectsCorruptReadsAndDoesNotCacheErrors(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	counted := &manifestReadCounter{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
	store, err := New(counted, ChunkBytes)
	require.NoError(t, err)
	directory := privateImage(t, map[string][]byte{"pages.img": []byte("memory")})
	ref, err := store.Publish(t.Context(), testBinding(), directory)
	require.NoError(t, err)
	reader, err := raw.Get(manifestKey(ref.BindingDigest), 0, -1)
	require.NoError(t, err)
	payload, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.NoError(t, raw.Put(manifestKey(ref.BindingDigest), strings.NewReader("{}")))
	for range 2 {
		_, err = store.loadManifest(t.Context(), testBinding(), ref)
		require.Error(t, err)
	}
	require.Equal(t, int32(2), counted.reads.Load())
	require.NoError(t, raw.Put(manifestKey(ref.BindingDigest), strings.NewReader(string(payload))))
	_, err = store.loadManifest(t.Context(), testBinding(), ref)
	require.NoError(t, err)
	require.Equal(t, int32(3), counted.reads.Load())
	_, err = store.loadManifest(t.Context(), testBinding(), ref)
	require.NoError(t, err)
	require.Equal(t, int32(3), counted.reads.Load())
}

func TestManifestCacheBoundsConcurrentRetentionAndEvicts(t *testing.T) {
	var cache manifestCache
	first := Reference{BindingDigest: "first", ManifestDigest: "first"}
	cache.put(first, "first")
	var workers sync.WaitGroup
	for i := range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ref := Reference{BindingDigest: fmt.Sprint(i), ManifestDigest: fmt.Sprint(i)}
			cache.put(ref, strings.Repeat("x", MaxManifestBytes))
			cache.get(ref)
		}()
	}
	workers.Wait()
	_, present := cache.get(first)
	require.False(t, present)
	bytes := 0
	entries := 0
	for _, entry := range cache.entries {
		bytes += len(entry.payload)
		if entry.payload != "" {
			entries++
		}
	}
	require.Equal(t, cache.bytes, bytes)
	require.LessOrEqual(t, bytes, manifestCacheBytes)
	require.LessOrEqual(t, entries, manifestCacheEntries)
	before := cache.bytes
	cache.put(first, strings.Repeat("x", MaxManifestBytes+1))
	require.Equal(t, before, cache.bytes, "oversized metadata must not evict or enter the cache")
	var small manifestCache
	for i := range manifestCacheEntries + 1 {
		small.put(Reference{BindingDigest: fmt.Sprint(i)}, "small")
	}
	_, present = small.get(Reference{BindingDigest: "0"})
	require.False(t, present, "entry count is bounded even for tiny manifests")
}
