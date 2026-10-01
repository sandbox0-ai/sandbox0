//go:build linux

package runtimecheckpoint

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// Run only on an isolated Linux host, with TMPDIR on a reflink-enabled XFS
// filesystem. Exercise the same verified node-cache clone and restore admission
// used by ctld without reading customer images or altering a runtime journal.
func TestResumeLatencyRemote(t *testing.T) {
	if os.Getenv("SANDBOX0_RESUME_LATENCY_TEST") != "1" {
		t.Skip("isolated remote benchmark")
	}
	const imageBytes = 66 * ChunkBytes
	raw := objectstore.NewMemoryStore("")
	cacheDir := t.TempDir()
	cache, err := rootfsblock.NewReadCacheWithDisk(0, rootfsblock.DiskCacheConfig{Directory: cacheDir, MaxBytes: imageBytes + ChunkBytes})
	require.NoError(t, err)
	defer cache.Close()
	store, err := NewWithChunkCache(raw, imageBytes+ChunkBytes, cache)
	require.NoError(t, err)
	source := t.TempDir()
	require.NoError(t, os.Chmod(source, 0700))
	output, err := os.OpenFile(filepath.Join(source, "pages.img"), os.O_CREATE|os.O_WRONLY, 0600)
	require.NoError(t, err)
	for index := range 66 {
		_, err = output.Write(bytes.Repeat([]byte{byte(index + 1)}, ChunkBytes))
		require.NoError(t, err)
	}
	require.NoError(t, output.Sync())
	require.NoError(t, output.Close())
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	for run := range 3 {
		destination := filepath.Join(t.TempDir(), "destination")
		start := time.Now()
		_, stats, err := store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, destination, func(int64, uint64) error { return nil })
		require.NoError(t, err)
		preparation := time.Since(start)
		require.EqualValues(t, 66, stats.ClonedChunks, "benchmark requires actual filesystem range cloning")
		start = time.Now()
		_, err = store.VerifyLocal(t.Context(), testBinding(), ref, destination)
		require.NoError(t, err)
		verification := time.Since(start)
		fmt.Printf("RESUME_LATENCY run=%d bytes=%d cloned=%d preparation_us=%d restore_verification_us=%d\n", run, imageBytes, stats.ClonedChunks, preparation.Microseconds(), verification.Microseconds())
		require.NoError(t, os.RemoveAll(destination))
	}
}
