//go:build linux || darwin

package runtimecheckpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func retainedLimits(t *testing.T) RetainedImageLimits {
	t.Helper()
	return RetainedImageLimits{Directory: filepath.Join(t.TempDir(), "images"), Bytes: 4 * ChunkBytes, Inodes: 128, Entries: 4, Lifetime: time.Hour}
}

func TestRetainedImagePreservesCaptureInodeAcrossCleanupAndRestart(t *testing.T) {
	for _, mode := range []string{"publish", "planned", "staged"} {
		t.Run(mode, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			store, err := New(raw, ChunkBytes)
			require.NoError(t, err)
			limits := retainedLimits(t)
			require.NoError(t, store.ConfigureRetainedImages(limits))
			source := privateImage(t, map[string][]byte{"pages.img": []byte("guest RAM"), "nested/state": []byte("tasks"), "empty": {}})
			// Match stock runsc's file creation mode, rather than only the
			// private modes used by synthetic transfer fixtures.
			require.NoError(t, os.Chmod(filepath.Join(source, "pages.img"), 0o644))
			inode, err := os.Stat(filepath.Join(source, "pages.img"))
			require.NoError(t, err)
			var ref Reference
			switch mode {
			case "publish":
				ref, err = store.Publish(t.Context(), testBinding(), source)
			case "planned":
				plan, planErr := store.PlanLocal(t.Context(), testBinding(), source)
				require.NoError(t, planErr)
				ref, err = store.PublishPlanned(t.Context(), testBinding(), plan, source)
			case "staged":
				scope, scopeErr := captureScopeForBinding(testBinding())
				require.NoError(t, scopeErr)
				stage, stageErr := store.OpenCaptureStaging(t.Context(), scope, 4*ChunkBytes)
				require.NoError(t, stageErr)
				plan, planErr := stage.PlanLocal(t.Context(), testBinding(), source)
				require.NoError(t, planErr)
				ref, err = stage.PublishPlanned(t.Context(), testBinding(), plan, source)
			}
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(source))
			store.Close()
			store, err = New(raw, ChunkBytes)
			require.NoError(t, err)
			defer store.Close()
			require.NoError(t, store.ConfigureRetainedImages(limits))
			destination := filepath.Join(t.TempDir(), "restore")
			_, stats, err := store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, destination, func(bytes int64, inodes uint64) error {
				require.Positive(t, bytes)
				require.Positive(t, inodes)
				return nil
			})
			require.NoError(t, err)
			require.True(t, stats.RetainedImage)
			require.EqualValues(t, 0, stats.RegionalBytes)
			restored, err := os.Stat(filepath.Join(destination, "pages.img"))
			require.NoError(t, err)
			require.True(t, os.SameFile(inode, restored), "reflinks cannot preserve capture page-cache identity")
			require.Equal(t, os.FileMode(0o600), restored.Mode().Perm())
			_, err = store.VerifyLocal(t.Context(), testBinding(), ref, destination)
			require.NoError(t, err)
			require.NoError(t, store.EvictRetainedImages(t.Context()))
			_, err = store.VerifyLocal(t.Context(), testBinding(), ref, destination)
			require.NoError(t, err, "cache eviction must not remove restore custody")
		})
	}
}

func TestRetainedImageCorruptionFallsBackWithoutTrustingMetadata(t *testing.T) {
	for _, damage := range []string{"content", "manifest", "missing", "symlink", "permissions", "inventory"} {
		t.Run(damage, func(t *testing.T) {
			store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
			require.NoError(t, err)
			defer store.Close()
			limits := retainedLimits(t)
			require.NoError(t, store.ConfigureRetainedImages(limits))
			ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"pages.img": []byte("correct")}))
			require.NoError(t, err)
			entry := filepath.Join(limits.Directory, retainedImageName(ref))
			file := filepath.Join(entry, "image", "pages.img")
			outside := filepath.Join(t.TempDir(), "outside")
			require.NoError(t, os.WriteFile(outside, []byte("outside"), 0o600))
			switch damage {
			case "content":
				require.NoError(t, os.WriteFile(file, []byte("altered"), 0o600))
			case "manifest":
				require.NoError(t, os.WriteFile(filepath.Join(entry, "manifest.json"), []byte("{}"), 0o600))
			case "missing":
				require.NoError(t, os.Remove(file))
			case "symlink":
				require.NoError(t, os.Remove(file))
				require.NoError(t, os.Symlink(outside, file))
			case "permissions":
				require.NoError(t, os.Chmod(file, 0o644))
			case "inventory":
				require.NoError(t, os.WriteFile(filepath.Join(entry, "image", "extra"), []byte("extra"), 0o600))
			}
			destination := filepath.Join(t.TempDir(), "restore")
			_, stats, err := store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, destination, func(int64, uint64) error { return nil })
			require.NoError(t, err)
			require.False(t, stats.RetainedImage)
			require.EqualValues(t, 7, stats.RegionalBytes)
			actual, err := os.ReadFile(filepath.Join(destination, "pages.img"))
			require.NoError(t, err)
			require.Equal(t, "correct", string(actual))
			actual, err = os.ReadFile(outside)
			require.NoError(t, err)
			require.Equal(t, "outside", string(actual))
		})
	}
}

func TestRetainedImageStartupDiscardsOnlyOwnedCrashPartials(t *testing.T) {
	limits := retainedLimits(t)
	require.NoError(t, os.Mkdir(limits.Directory, 0o700))
	partial := filepath.Join(limits.Directory, "partial-crashed")
	require.NoError(t, os.Mkdir(partial, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(partial, "manifest.json"), []byte("incomplete"), 0o600))
	outside := privateImage(t, map[string][]byte{"state": []byte("custody")})
	require.NoError(t, os.Symlink(outside, filepath.Join(limits.Directory, "partial-link")))
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.ConfigureRetainedImages(limits))
	require.NoDirExists(t, partial)
	contents, err := os.ReadFile(filepath.Join(outside, "state"))
	require.NoError(t, err)
	require.Equal(t, "custody", string(contents))
}

func TestRetainedImageRequiresRegionalAuthorityAndAdmission(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.ConfigureRetainedImages(retainedLimits(t)))
	ref, err := store.Publish(t.Context(), testBinding(), privateImage(t, map[string][]byte{"pages.img": []byte("RAM")}))
	require.NoError(t, err)
	destination := filepath.Join(t.TempDir(), "restore")
	denied := errors.New("no staging budget")
	_, _, err = store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, destination, func(int64, uint64) error { return denied })
	require.ErrorIs(t, err, denied)
	_, err = os.Stat(destination)
	require.ErrorIs(t, err, os.ErrNotExist)
	wrong := testBinding()
	wrong.TeamID = "other-team"
	_, err = store.Download(t.Context(), wrong, ref, destination)
	require.Error(t, err)
	_, err = os.Stat(destination)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, os.Mkdir(destination, 0o700))
	marker := filepath.Join(destination, "custody")
	require.NoError(t, os.WriteFile(marker, []byte("existing"), 0o600))
	_, err = store.Download(t.Context(), testBinding(), ref, destination)
	require.ErrorIs(t, err, os.ErrExist)
	actual, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "existing", string(actual))
	// Even a perfectly valid local image cannot replace the regional manifest.
	require.NoError(t, store.objects.Delete(manifestKey(ref.BindingDigest)))
	fresh, err := New(store.objects, ChunkBytes)
	require.NoError(t, err)
	defer fresh.Close()
	require.NoError(t, fresh.ConfigureRetainedImages(store.retainedCache().limits))
	_, err = fresh.Download(t.Context(), testBinding(), ref, filepath.Join(t.TempDir(), "no-authority"))
	require.Error(t, err)
}

func TestRetainedImageBoundsExpiryAndSharedGenerations(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	limits := retainedLimits(t)
	limits.Entries = 1
	first, err := New(raw, ChunkBytes)
	require.NoError(t, err)
	defer first.Close()
	second, err := New(raw, ChunkBytes)
	require.NoError(t, err)
	defer second.Close()
	require.NoError(t, first.ConfigureRetainedImages(limits))
	require.NoError(t, second.ConfigureRetainedImages(limits))
	var refs [2]Reference
	for i, store := range []*Store{first, second} {
		binding := testBinding()
		binding.OperationID += string(rune('a' + i))
		refs[i], err = store.Publish(t.Context(), binding, privateImage(t, map[string][]byte{"pages.img": []byte("guest")}))
		require.NoError(t, err)
	}
	_, err = os.Stat(filepath.Join(limits.Directory, retainedImageName(refs[0])))
	require.ErrorIs(t, err, os.ErrNotExist)
	binding := testBinding()
	binding.OperationID += "b"
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		store := first
		if i%2 != 0 {
			store = second
		}
		destination := filepath.Join(t.TempDir(), "restore")
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Download(t.Context(), binding, refs[1], destination)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(limits.Directory, retainedImageName(refs[1])), old, old))
	_, stats, err := first.DownloadWithAdmissionStats(t.Context(), binding, refs[1], filepath.Join(t.TempDir(), "expired"), func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.False(t, stats.RetainedImage)
	// Admission may evict the hint; it must run without holding its lock.
	_, _, err = first.DownloadWithAdmissionStats(t.Context(), binding, refs[1], filepath.Join(t.TempDir(), "pressure"), func(int64, uint64) error { return first.EvictRetainedImages(context.Background()) })
	require.NoError(t, err)
}

func TestRetainedImageBoundsBytesAndInodes(t *testing.T) {
	for _, bound := range []string{"bytes", "inodes"} {
		t.Run(bound, func(t *testing.T) {
			store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
			require.NoError(t, err)
			defer store.Close()
			source := privateImage(t, map[string][]byte{"pages.img": []byte("RAM"), "nested/state": []byte("tasks")})
			plan, err := store.PlanLocal(t.Context(), testBinding(), source)
			require.NoError(t, err)
			payload, err := plan.Manifest.Encode(ChunkBytes)
			require.NoError(t, err)
			bytes, inodes, err := retainedImageFootprint(plan.Manifest, payload)
			require.NoError(t, err)
			limits := retainedLimits(t)
			if bound == "bytes" {
				limits.Bytes = 2*bytes - 1
			} else {
				limits.Inodes = 2*inodes - 1
			}
			require.NoError(t, store.ConfigureRetainedImages(limits))
			old, err := store.PublishPlanned(t.Context(), testBinding(), plan, source)
			require.NoError(t, err)
			binding := testBinding()
			binding.OperationID += "next"
			fresh, err := store.Publish(t.Context(), binding, source)
			require.NoError(t, err)
			require.NoDirExists(t, filepath.Join(limits.Directory, retainedImageName(old)))
			require.DirExists(t, filepath.Join(limits.Directory, retainedImageName(fresh)))
		})
	}
}

func TestRetainedImageFailureDoesNotChangePublicationOutcome(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	defer store.Close()
	limits := retainedLimits(t)
	limits.Bytes = 1 // A valid image cannot fit.
	require.NoError(t, store.ConfigureRetainedImages(limits))
	source := privateImage(t, map[string][]byte{"pages.img": []byte("original")})
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err, "disposable retention cannot fail durable publication")
	names, err := os.ReadDir(limits.Directory)
	require.NoError(t, err)
	require.Empty(t, names)
	_, stats, err := store.DownloadWithAdmissionStats(t.Context(), testBinding(), ref, filepath.Join(t.TempDir(), "restore"), func(int64, uint64) error { return nil })
	require.NoError(t, err)
	require.False(t, stats.RetainedImage)
	// Failed publication must never populate a prospective reference's cache.
	full, err := New(store.objects, ChunkBytes)
	require.NoError(t, err)
	defer full.Close()
	fullLimits := retainedLimits(t)
	require.NoError(t, full.ConfigureRetainedImages(fullLimits))
	changed := testBinding()
	changed.OperationID += "failure"
	plan, err := full.PlanLocal(t.Context(), changed, source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "pages.img"), []byte("modified"), 0o600))
	_, err = full.PublishPlanned(t.Context(), changed, plan, source)
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(fullLimits.Directory, retainedImageName(plan.Reference)))
}
