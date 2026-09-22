package runtimecheckpoint

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type capturePublicationGate struct {
	*gatedPublicationStore
	cleanup objectstore.ContextCleanupStore
}

func (s *capturePublicationGate) ListContext(ctx context.Context, prefix, after, token, delimiter string, limit int64) ([]objectstore.Info, bool, string, error) {
	return s.cleanup.ListContext(ctx, prefix, after, token, delimiter, limit)
}

func (s *capturePublicationGate) DeleteContext(ctx context.Context, key string) error {
	return s.cleanup.DeleteContext(ctx, key)
}

func TestCapturePlanPreservesParallelPublicationAndIndependentPeerTransfer(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "durable publication"
		if canceled {
			name = "publication canceled after peer completes"
		}
		t.Run(name, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			counted := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
			gate := &capturePublicationGate{gatedPublicationStore: &gatedPublicationStore{
				ContextConditionalStore: counted, entered: make(chan struct{}, publicationConcurrency), release: make(chan struct{})},
				cleanup: raw.(objectstore.ContextCleanupStore)}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			store, err := New(gate, 5*ChunkBytes)
			require.NoError(t, err)
			binding := testBinding()
			scope, err := captureScopeForBinding(binding)
			require.NoError(t, err)
			stage, err := store.OpenCaptureStaging(t.Context(), scope, 4*ChunkBytes)
			require.NoError(t, err)
			var data []byte
			for _, value := range []byte{1, 2, 3, 4, 1} {
				data = append(data, bytes.Repeat([]byte{value}, ChunkBytes)...)
			}
			directory := privateImage(t, map[string][]byte{"pages.img": data})
			plan, err := stage.PlanLocal(t.Context(), binding, directory)
			require.NoError(t, err)
			require.Equal(t, StagedManifestVersion, plan.Manifest.Version)
			_, err = raw.Head(manifestKey(plan.Reference.BindingDigest))
			require.True(t, objectstore.IsNotFound(err))
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			type result struct {
				ref Reference
				err error
			}
			done := make(chan result, 1)
			go func() {
				ref, err := stage.PublishPlanned(ctx, binding, plan, directory)
				done <- result{ref, err}
			}()
			for range publicationConcurrency {
				select {
				case <-gate.entered:
				case <-ctx.Done():
					t.Fatal("independent staged chunks did not upload concurrently")
				}
			}
			require.EqualValues(t, publicationConcurrency, gate.active.Load())
			reader, writer := io.Pipe()
			sent := make(chan error, 1)
			go func() {
				err := store.WritePlannedPeerImage(ctx, binding, plan, directory, writer)
				_ = writer.CloseWithError(err)
				sent <- err
			}()
			destination := filepath.Join(t.TempDir(), "peer")
			manifest, receiveErr := ReceivePeerImage(ctx, binding, plan.Reference, destination, reader, 5*ChunkBytes,
				func(int64, uint64) error { return nil })
			_ = reader.CloseWithError(receiveErr)
			require.NoError(t, receiveErr)
			require.NoError(t, <-sent)
			require.Equal(t, plan.Manifest, manifest)
			_, err = store.VerifyLocal(ctx, binding, plan.Reference, destination)
			require.Error(t, err, "peer completion cannot substitute for a regional receipt")
			if canceled {
				cancel()
				got := <-done
				require.ErrorIs(t, got.err, context.Canceled)
				require.Empty(t, got.ref)
				_, err = raw.Head(manifestKey(plan.Reference.BindingDigest))
				require.True(t, objectstore.IsNotFound(err))
				return
			}
			release.Do(func() { close(gate.release) })
			got := <-done
			require.NoError(t, got.err)
			require.Equal(t, plan.Reference, got.ref)
			require.EqualValues(t, 4, counted.chunkPuts.Load(), "duplicate chunks reuse the same upload")
			require.False(t, gate.overflow.Load())
			_, err = store.VerifyLocal(ctx, binding, got.ref, destination)
			require.NoError(t, err)
		})
	}
}

func TestCapturePlanReusesEarlyUploadAndSurvivesPublicationRetry(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	counted := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
	store, err := New(counted, ChunkBytes)
	require.NoError(t, err)
	binding := testBinding()
	scope, err := captureScopeForBinding(binding)
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	data := []byte("captured state")
	_, err = stage.StageChunk(t.Context(), data)
	require.NoError(t, err)
	directory := privateImage(t, map[string][]byte{"state": data})
	inventory, err := store.InspectLocal(t.Context(), directory)
	require.NoError(t, err)
	plan, err := stage.BindLocalInventory(t.Context(), binding, inventory)
	require.NoError(t, err)
	_, err = stage.StageChunk(t.Context(), []byte("late bytes"))
	require.ErrorContains(t, err, "already bound")
	counted.failManifest = true
	_, err = stage.PublishPlanned(t.Context(), binding, plan, directory)
	require.ErrorContains(t, err, "manifest publication unavailable")
	counted.failManifest = false
	ref, err := stage.PublishPlanned(t.Context(), binding, plan, directory)
	require.NoError(t, err)
	require.Equal(t, plan.Reference, ref)
	require.EqualValues(t, 1, counted.chunkPuts.Load(), "final publication does not repeat early uploads")
	recovered, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	ref, err = recovered.PublishPlanned(t.Context(), binding, plan, directory)
	require.NoError(t, err)
	require.Equal(t, plan.Reference, ref)
	wrong := binding
	wrong.RootFSGenerationID = "another-cut"
	_, err = recovered.BindLocalInventory(t.Context(), wrong, inventory)
	require.ErrorContains(t, err, "another filesystem cut")
	legacy, err := store.BindLocalInventory(binding, inventory)
	require.NoError(t, err)
	_, err = recovered.PublishPlanned(t.Context(), binding, legacy, directory)
	require.ErrorContains(t, err, "staged checkpoint plan")
}

func TestCapturePlanRejectsChangedLocalImageEvenWhenChunksAreAlreadyUploaded(t *testing.T) {
	for _, change := range []string{"same size bytes", "extra file", "missing file", "truncated file", "changed reference", "changed scope"} {
		t.Run(change, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			store, err := New(raw, ChunkBytes)
			require.NoError(t, err)
			binding := testBinding()
			scope, err := captureScopeForBinding(binding)
			require.NoError(t, err)
			stage, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
			require.NoError(t, err)
			_, err = stage.StageChunk(t.Context(), []byte("original"))
			require.NoError(t, err)
			directory := privateImage(t, map[string][]byte{"state": []byte("original")})
			plan, err := stage.PlanLocal(t.Context(), binding, directory)
			require.NoError(t, err)
			key := manifestKey(plan.Reference.BindingDigest)
			switch change {
			case "same size bytes":
				err = os.WriteFile(filepath.Join(directory, "state"), []byte("modified"), 0o600)
			case "extra file":
				err = os.WriteFile(filepath.Join(directory, "extra"), []byte("x"), 0o600)
			case "missing file":
				err = os.Remove(filepath.Join(directory, "state"))
			case "truncated file":
				err = os.Truncate(filepath.Join(directory, "state"), 1)
			case "changed reference":
				plan.Reference.ManifestDigest = binding.SourceBindingDigest
			case "changed scope":
				binding.OperationID = "other-operation"
			}
			require.NoError(t, err)
			_, err = stage.PublishPlanned(t.Context(), binding, plan, directory)
			require.Error(t, err)
			_, err = raw.Head(key)
			require.True(t, objectstore.IsNotFound(err))
		})
	}
}

func TestCaptureInspectionStagesTailWithoutBindingOrPublication(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "changed after inspection"}[changed], func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			counted := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
			store, err := New(counted, 3*ChunkBytes)
			require.NoError(t, err)
			binding := testBinding()
			scope, err := captureScopeForBinding(binding)
			require.NoError(t, err)
			stage, err := store.OpenCaptureStaging(t.Context(), scope, 3*ChunkBytes)
			require.NoError(t, err)
			prefix, err := capturePrefix(scope)
			require.NoError(t, err)
			first := bytes.Repeat([]byte{7}, ChunkBytes)
			_, err = stage.StageChunk(t.Context(), first)
			require.NoError(t, err)
			directory := privateImage(t, map[string][]byte{"pages.img": append(first, []byte("final tail")...)})
			inventory, err := stage.InspectAndStageLocal(t.Context(), directory)
			require.NoError(t, err)
			require.EqualValues(t, 2, counted.chunkPuts.Load(), "only the final tail was missing")
			_, err = raw.Head(prefix + "publication.json")
			require.True(t, objectstore.IsNotFound(err), "inspection cannot bind a not-yet-sealed RootFS")
			bd, err := binding.Digest()
			require.NoError(t, err)
			_, err = raw.Head(manifestKey(bd))
			require.True(t, objectstore.IsNotFound(err), "staged chunks cannot authorize restore")
			plan, err := stage.BindLocalInventory(t.Context(), binding, inventory)
			require.NoError(t, err)
			if changed {
				require.NoError(t, os.WriteFile(filepath.Join(directory, "pages.img"), bytes.Repeat([]byte{8}, len(first)+10), 0o600))
			}
			ref, err := stage.PublishPlanned(t.Context(), binding, plan, directory)
			if changed {
				require.Error(t, err)
				require.Empty(t, ref)
				_, err = raw.Head(manifestKey(bd))
				require.True(t, objectstore.IsNotFound(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, plan.Reference, ref)
				require.EqualValues(t, 2, counted.chunkPuts.Load(), "publication rechecks bytes without uploading them again")
				_, err = store.VerifyLocal(t.Context(), binding, ref, directory)
				require.NoError(t, err)
			}
			_, err = stage.InspectAndStageLocal(t.Context(), directory)
			require.ErrorContains(t, err, "already bound", "inspection cannot reopen final-bound tentative admission")
		})
	}
}

func TestCaptureInspectionCancellationJoinsUploadsAndRetainsCharges(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	gate := &capturePublicationGate{gatedPublicationStore: &gatedPublicationStore{
		ContextConditionalStore: raw.(objectstore.ContextConditionalStore), entered: make(chan struct{}, publicationConcurrency), release: make(chan struct{})},
		cleanup: raw.(objectstore.ContextCleanupStore)}
	defer close(gate.release)
	store, err := New(gate, 2*ChunkBytes)
	require.NoError(t, err)
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 2*ChunkBytes)
	require.NoError(t, err)
	directory := privateImage(t, map[string][]byte{"pages.img": []byte("final short state")})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := stage.InspectAndStageLocal(ctx, directory); done <- err }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("completed inspection did not stage its short final chunk")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, gate.active.Load(), "no upload may outlive source custody")
	require.Len(t, stage.uploaded, 1, "ambiguous writes retain the pre-dispatch budget charge")
	require.Empty(t, stage.pending)
	require.Empty(t, stage.bound)
}
