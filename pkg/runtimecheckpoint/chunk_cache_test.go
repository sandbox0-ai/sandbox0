package runtimecheckpoint

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type checkpointCacheObjects struct {
	objectstore.ContextConditionalStore
	cleanup    objectstore.ContextCleanupStore
	chunkReads atomic.Int32
}

func (s *checkpointCacheObjects) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	if strings.Contains(key, "/chunks/") {
		s.chunkReads.Add(1)
	}
	return s.ContextConditionalStore.GetContext(ctx, key, off, limit)
}

func (s *checkpointCacheObjects) ListContext(ctx context.Context, prefix, after, token, delimiter string, limit int64) ([]objectstore.Info, bool, string, error) {
	return s.cleanup.ListContext(ctx, prefix, after, token, delimiter, limit)
}

func (s *checkpointCacheObjects) DeleteContext(ctx context.Context, key string) error {
	return s.cleanup.DeleteContext(ctx, key)
}

func TestPublishedCheckpointReusesRootFSDiskCacheAcrossRestartAndCorruption(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	objects := &checkpointCacheObjects{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), cleanup: raw.(objectstore.ContextCleanupStore)}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	config := rootfsblock.DiskCacheConfig{Directory: directory, MaxBytes: 4 * ChunkBytes}
	cache, err := rootfsblock.NewReadCacheWithDisk(0, config)
	require.NoError(t, err)
	store, err := NewWithChunkCache(objects, 3*ChunkBytes, cache)
	require.NoError(t, err)
	data := append(bytes.Repeat([]byte{0x41}, ChunkBytes), bytes.Repeat([]byte{0x42}, ChunkBytes)...)
	data = append(data, bytes.Repeat([]byte{0x43}, 17)...)
	source := privateImage(t, map[string][]byte{"pages.img": data})
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	require.Equal(t, 3, cache.Stats().DiskEntries)
	first := filepath.Join(t.TempDir(), "first")
	manifest, stats, err := store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, first, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.CacheChunks)
	require.EqualValues(t, len(data), stats.CacheBytes)
	require.Zero(t, stats.RegionalChunks)
	got, err := os.ReadFile(filepath.Join(first, "pages.img"))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Zero(t, objects.chunkReads.Load(), "same-node restore should need only the regional manifest")
	require.NoError(t, cache.Close())

	restarted, err := rootfsblock.NewReadCacheWithDisk(0, config)
	require.NoError(t, err)
	defer func() { require.NoError(t, restarted.Close()) }()
	recovered, err := NewWithChunkCache(objects, 3*ChunkBytes, restarted)
	require.NoError(t, err)
	second := filepath.Join(t.TempDir(), "second")
	_, stats, err = recovered.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, second, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.CacheChunks)
	require.Zero(t, objects.chunkReads.Load(), "verified chunks survive a ctld restart")

	chunk := manifest.Files[0].Chunks[0]
	name := strings.TrimPrefix(chunk.Digest, "sha256:") + "-" + strconv.FormatInt(chunk.Size, 10)
	require.NoError(t, os.WriteFile(filepath.Join(directory, name), bytes.Repeat([]byte{0x44}, ChunkBytes), 0o600))
	third := filepath.Join(t.TempDir(), "third")
	_, stats, err = recovered.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, third, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.RegionalChunks)
	require.EqualValues(t, 2, stats.CacheChunks)
	got, err = os.ReadFile(filepath.Join(third, "pages.img"))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.EqualValues(t, 1, objects.chunkReads.Load(), "corrupt cache bytes must fall back to the authenticated region")
	fourth := filepath.Join(t.TempDir(), "fourth")
	_, stats, err = recovered.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, fourth, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.CacheChunks)
	require.Zero(t, stats.RegionalChunks)
	require.EqualValues(t, 1, objects.chunkReads.Load(), "regional fallback must repair the node cache")
}

func TestStagedCheckpointCaptureWarmsSharedDiskCache(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	objects := &checkpointCacheObjects{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), cleanup: raw.(objectstore.ContextCleanupStore)}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	cache, err := rootfsblock.NewReadCacheWithDisk(0, rootfsblock.DiskCacheConfig{Directory: directory, MaxBytes: 2 * ChunkBytes})
	require.NoError(t, err)
	defer func() { require.NoError(t, cache.Close()) }()
	store, err := NewWithChunkCache(objects, 2*ChunkBytes, cache)
	require.NoError(t, err)
	binding := testBinding()
	scope, err := captureScopeForBinding(binding)
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 2*ChunkBytes)
	require.NoError(t, err)
	data := append(bytes.Repeat([]byte{0x52}, ChunkBytes), []byte("tail")...)
	source := privateImage(t, map[string][]byte{"pages.img": data})
	ref, err := stage.Publish(t.Context(), binding, source)
	require.NoError(t, err)
	require.Equal(t, 2, cache.Stats().DiskEntries)
	destination := filepath.Join(t.TempDir(), "restored")
	_, err = store.Download(t.Context(), binding, ref, destination)
	require.NoError(t, err)
	got, err := os.ReadFile(filepath.Join(destination, "pages.img"))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Zero(t, objects.chunkReads.Load())
}

func TestCheckpointRestoreCombinesLRUHitWithRegionalFallback(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	objects := &checkpointCacheObjects{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), cleanup: raw.(objectstore.ContextCleanupStore)}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	cache, err := rootfsblock.NewReadCacheWithDisk(0, rootfsblock.DiskCacheConfig{Directory: directory, MaxBytes: ChunkBytes})
	require.NoError(t, err)
	defer func() { require.NoError(t, cache.Close()) }()
	store, err := NewWithChunkCache(objects, 2*ChunkBytes, cache)
	require.NoError(t, err)
	data := append(bytes.Repeat([]byte{0x21}, ChunkBytes), bytes.Repeat([]byte{0x34}, ChunkBytes)...)
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"pages.img": data}))
	require.NoError(t, err)
	require.EqualValues(t, ChunkBytes, cache.Stats().DiskBytes)
	destination := filepath.Join(t.TempDir(), "restored")
	_, stats, err := store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, destination, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.CacheChunks)
	require.EqualValues(t, 1, stats.RegionalChunks)
	got, err := os.ReadFile(filepath.Join(destination, "pages.img"))
	require.NoError(t, err)
	require.Equal(t, data, got)
}
