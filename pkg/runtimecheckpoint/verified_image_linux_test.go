//go:build linux

package runtimecheckpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func TestVerifiedImageGuardsDetectChangesAndRestart(t *testing.T) {
	for _, change := range []string{"unchanged", "overwrite", "replace", "restored-mtime", "extra-file", "closed-watch", "restart"} {
		t.Run(change, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			store, err := New(raw, ChunkBytes)
			require.NoError(t, err)
			t.Cleanup(store.Close)
			data := []byte("verified memory image")
			ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"nested/pages.img": data}))
			require.NoError(t, err)
			directory := filepath.Join(t.TempDir(), "destination")
			manifest, err := store.Download(t.Context(), testBinding(), ref, directory)
			require.NoError(t, err)
			guard := store.verifiedImages.take(ref, directory)
			require.NotNil(t, guard, "Linux materialization must produce a change-guarded proof")
			root, err := openPrivateDirectory(directory)
			require.NoError(t, err)
			defer root.Close()
			require.True(t, guard.valid(root, manifest))
			store.verifiedImages.put(ref, directory, guard)
			file := filepath.Join(directory, "nested/pages.img")
			info, err := os.Stat(file)
			require.NoError(t, err)
			switch change {
			case "overwrite", "restored-mtime":
				require.NoError(t, os.WriteFile(file, []byte("corrupt! memory image"), 0600))
				if change == "restored-mtime" {
					require.NoError(t, os.Chtimes(file, info.ModTime(), info.ModTime()))
				}
			case "replace":
				require.NoError(t, os.Remove(file))
				require.NoError(t, os.WriteFile(file, data, 0600))
			case "extra-file":
				require.NoError(t, os.WriteFile(filepath.Join(directory, "unexpected"), data, 0600))
			case "closed-watch":
				guard.close()
				require.NoError(t, os.WriteFile(file, []byte("corrupt! memory image"), 0600))
			case "restart":
				store.Close()
				store, err = New(raw, ChunkBytes)
				require.NoError(t, err)
				t.Cleanup(store.Close)
				require.NoError(t, os.WriteFile(file, []byte("corrupt! memory image"), 0600))
			}
			if change != "unchanged" {
				require.False(t, guard.valid(root, manifest), "mutations must invalidate guarded evidence")
			}
			_, err = store.VerifyLocal(t.Context(), testBinding(), ref, directory)
			if change == "unchanged" || change == "replace" {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "invalid evidence must fall back to full verification")
			}
		})
	}
}

func TestVerifiedImageProofIsSingleUseAndBounded(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	defer store.Close()
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"pages.img": []byte("state")}))
	require.NoError(t, err)
	var first *imageVerificationGuard
	for i := range verifiedImageEntries + 1 {
		directory := filepath.Join(t.TempDir(), "destination")
		_, err := store.Download(t.Context(), testBinding(), ref, directory)
		require.NoError(t, err)
		guard := store.verifiedImages.take(ref, directory)
		require.NotNil(t, guard)
		require.Nil(t, store.verifiedImages.take(ref, directory), "a proof is consumed at most once")
		if i == 0 {
			first = guard
		}
		store.verifiedImages.put(ref, directory, guard)
	}
	require.Equal(t, -1, first.fd, "eviction must close its kernel watches")
	store.Close()
	for _, entry := range store.verifiedImages.entries {
		require.Nil(t, entry)
	}
}

func TestVerifiedImageKernelEventsCoverMatchingMetadata(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	defer store.Close()
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"pages.img": []byte("state")}))
	require.NoError(t, err)
	directory := filepath.Join(t.TempDir(), "destination")
	manifest, err := store.Download(t.Context(), testBinding(), ref, directory)
	require.NoError(t, err)
	guard := store.verifiedImages.take(ref, directory)
	require.NotNil(t, guard)
	defer guard.close()
	// Model a filesystem whose timestamp resolution cannot distinguish a write.
	// Even matching metadata must not hide an event on the watched open inode.
	file := filepath.Join(directory, "pages.img")
	require.NoError(t, os.WriteFile(file, []byte("state"), 0600))
	guard.files["pages.img"], err = os.Stat(file)
	require.NoError(t, err)
	root, err := openPrivateDirectory(directory)
	require.NoError(t, err)
	defer root.Close()
	require.False(t, guard.valid(root, manifest))
}

func TestVerifiedImageUnconsumedProofExpires(t *testing.T) {
	var cache verifiedImageCache
	defer cache.close()
	guard := newImageVerificationGuard()
	require.NotNil(t, guard)
	cache.put(Reference{}, "unused", guard)
	cache.mu.Lock()
	cache.entries[0].timer.Reset(time.Millisecond)
	cache.mu.Unlock()
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return cache.entries[0] == nil && guard.fd == -1
	}, time.Second, time.Millisecond)
	require.Nil(t, cache.take(Reference{}, "unused"))
}
