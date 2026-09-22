package runtimecheckpoint

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func TestGrowingCaptureRechecksRewrittenPagesAndLeavesProducerDirectoryCreation(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	counted := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
	store, err := New(counted, 3*ChunkBytes)
	require.NoError(t, err)
	binding := testBinding()
	scope, err := captureScopeForBinding(binding)
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 4*ChunkBytes)
	require.NoError(t, err)
	directory := filepath.Join(t.TempDir(), "runsc-image")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- stage.UploadGrowing(ctx, directory) }()
	require.Eventually(t, func() bool { return len(stage.gate) == 1 }, time.Second, time.Millisecond)
	_, err = os.Stat(directory)
	require.ErrorIs(t, err, os.ErrNotExist, "runsc owns creation of its image directory")
	require.NoError(t, os.Mkdir(directory, 0o700))
	file, err := os.OpenFile(filepath.Join(directory, "pages.img"), os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer file.Close()
	// A preallocated full chunk is speculative, even when its size is stable.
	require.NoError(t, file.Truncate(ChunkBytes))
	zero := make([]byte, ChunkBytes)
	waitChunk := func(data []byte) {
		t.Helper()
		key := stage.prefix + "chunks/" + digest.FromBytes(data).Encoded()
		require.Eventually(t, func() bool { _, err := raw.Head(key); return err == nil }, 5*time.Second, 10*time.Millisecond)
	}
	waitChunk(zero)
	first, second := bytes.Repeat([]byte{1}, ChunkBytes), bytes.Repeat([]byte{2}, ChunkBytes)
	_, err = file.WriteAt(first, 0)
	require.NoError(t, err)
	// Introduce the next file atomically so the expected reusable chunk is
	// deterministic; concurrent in-place rewrites above remain speculative.
	secondPath := filepath.Join(filepath.Dir(directory), "second-ready")
	require.NoError(t, os.WriteFile(secondPath, second, 0o600))
	require.NoError(t, os.Rename(secondPath, filepath.Join(directory, "second.img")))
	waitChunk(second)
	tail := []byte("final tail")
	_, err = file.WriteAt(tail, ChunkBytes)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.EqualValues(t, 2, counted.chunkPuts.Load(), "rewrites and short tails wait for final verification")
	bd, _ := binding.Digest()
	_, err = raw.Head(manifestKey(bd))
	require.True(t, objectstore.IsNotFound(err), "growing capture never publishes a manifest")
	plan, err := stage.PlanLocal(t.Context(), binding, directory)
	require.NoError(t, err)
	ref, err := stage.PublishPlanned(t.Context(), binding, plan, directory)
	require.NoError(t, err)
	require.EqualValues(t, 4, counted.chunkPuts.Load(), "only the rewritten first chunk and final tail are new")
	destination := filepath.Join(t.TempDir(), "downloaded")
	_, err = store.Download(t.Context(), binding, ref, destination)
	require.NoError(t, err)
	got, err := os.ReadFile(filepath.Join(destination, "pages.img"))
	require.NoError(t, err)
	want := append(first, tail...)
	require.Equal(t, want, got)
	got, err = os.ReadFile(filepath.Join(destination, "second.img"))
	require.NoError(t, err)
	require.Equal(t, second, got)
	require.ErrorContains(t, stage.UploadGrowing(t.Context(), directory), "already bound")
}

func TestGrowingCaptureCancellationJoinsBlockedUploadsAndReleasesCustody(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	gate := &capturePublicationGate{gatedPublicationStore: &gatedPublicationStore{
		ContextConditionalStore: raw.(objectstore.ContextConditionalStore),
		entered:                 make(chan struct{}, publicationConcurrency), release: make(chan struct{})},
		cleanup: raw.(objectstore.ContextCleanupStore)}
	defer close(gate.release)
	store, err := New(gate, 4*ChunkBytes)
	require.NoError(t, err)
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 4*ChunkBytes)
	require.NoError(t, err)
	var data []byte
	for _, value := range []byte{1, 2, 3, 4} {
		data = append(data, bytes.Repeat([]byte{value}, ChunkBytes)...)
	}
	directory := privateImage(t, map[string][]byte{"pages.img": data})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- stage.UploadGrowing(ctx, directory) }()
	for range publicationConcurrency {
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal("growing image did not start bounded parallel uploads")
		}
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("growing upload did not join canceled writes")
	}
	require.Zero(t, gate.active.Load())
	require.Empty(t, stage.pending)
	require.Empty(t, stage.gate)
	_, err = stage.PlanLocal(t.Context(), testBinding(), directory)
	require.NoError(t, err, "completed cancellation must release the final publication gate")
}

func TestGrowingCaptureStopsAtBudgetAndRejectsEscapingFiles(t *testing.T) {
	for _, failure := range []string{"budget", "symlink", "image size"} {
		t.Run(failure, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			store, err := New(raw, 2*ChunkBytes)
			require.NoError(t, err)
			scope, err := captureScopeForBinding(testBinding())
			require.NoError(t, err)
			stage, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
			require.NoError(t, err)
			directory := privateImage(t, map[string][]byte{"pages.img": bytes.Repeat([]byte{1}, ChunkBytes)})
			switch failure {
			case "budget":
				require.NoError(t, os.WriteFile(filepath.Join(directory, "other"), bytes.Repeat([]byte{2}, ChunkBytes), 0o600))
			case "symlink":
				require.NoError(t, os.Symlink(t.TempDir(), filepath.Join(directory, "escape")))
			case "image size":
				require.NoError(t, os.Truncate(filepath.Join(directory, "pages.img"), 3*ChunkBytes))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = stage.UploadGrowing(ctx, directory)
			require.Error(t, err)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			require.Empty(t, stage.pending)
			require.LessOrEqual(t, len(stage.uploaded), 1)
			bd, _ := testBinding().Digest()
			_, err = raw.Head(manifestKey(bd))
			require.True(t, objectstore.IsNotFound(err))
		})
	}
}
