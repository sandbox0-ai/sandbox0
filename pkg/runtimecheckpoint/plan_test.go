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

func TestPlannedPeerTransferCanCompleteWhileRegionalPublicationIsPending(t *testing.T) {
	for _, failPublication := range []bool{false, true} {
		name := "publication succeeds"
		if failPublication {
			name = "publication canceled after peer finishes"
		}
		t.Run(name, func(t *testing.T) {
			objects := objectstore.NewMemoryStore("")
			gate := &gatedPublicationStore{ContextConditionalStore: objects.(objectstore.ContextConditionalStore),
				entered: make(chan struct{}, publicationConcurrency), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(gate.release) })
			store, err := New(gate, 3*ChunkBytes)
			require.NoError(t, err)
			data := bytes.Repeat([]byte{42}, 2*ChunkBytes+17)
			source := privateImage(t, map[string][]byte{"pages.img": data})
			plan, err := store.PlanLocal(t.Context(), testBinding(), source)
			require.NoError(t, err)
			_, err = objects.Head(manifestKey(plan.Reference.BindingDigest))
			require.Error(t, err, "planning cannot publish a regional receipt")
			for _, chunk := range plan.Manifest.Files[0].Chunks {
				_, err = objects.Head(chunkKey(plan.Reference.BindingDigest, chunk.Digest))
				require.Error(t, err, "planning must not upload image chunks")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			type publication struct {
				ref Reference
				err error
			}
			done := make(chan publication, 1)
			go func() {
				ref, err := store.PublishPlanned(ctx, testBinding(), plan, source)
				done <- publication{ref, err}
			}()
			for range 3 {
				select {
				case <-gate.entered:
				case <-ctx.Done():
					t.Fatal("planned publication did not reach the regional upload gate")
				}
			}
			reader, writer := io.Pipe()
			sent := make(chan error, 1)
			go func() {
				err := store.WritePlannedPeerImage(ctx, testBinding(), plan, source, writer)
				_ = writer.CloseWithError(err)
				sent <- err
			}()
			destination := filepath.Join(t.TempDir(), "image")
			got, receiveErr := ReceivePeerImage(ctx, testBinding(), plan.Reference, destination, reader, 3*ChunkBytes,
				func(int64, uint64) error { return nil })
			_ = reader.CloseWithError(receiveErr)
			require.NoError(t, receiveErr)
			require.NoError(t, <-sent)
			require.Equal(t, plan.Manifest, got)
			copied, err := os.ReadFile(filepath.Join(destination, "pages.img"))
			require.NoError(t, err)
			require.Equal(t, data, copied)
			_, err = objects.Head(manifestKey(plan.Reference.BindingDigest))
			require.Error(t, err, "a completed peer image cannot make regional publication succeed")
			if failPublication {
				cancel()
				result := <-done
				require.ErrorIs(t, result.err, context.Canceled)
				require.Empty(t, result.ref)
				_, err = objects.Head(manifestKey(plan.Reference.BindingDigest))
				require.Error(t, err)
				return
			}
			release.Do(func() { close(gate.release) })
			result := <-done
			require.NoError(t, result.err)
			require.Equal(t, plan.Reference, result.ref)
			_, err = store.VerifyLocal(t.Context(), testBinding(), result.ref, destination)
			require.NoError(t, err)
			retry, err := store.PublishPlanned(t.Context(), testBinding(), plan, source)
			require.NoError(t, err)
			require.Equal(t, result.ref, retry)
		})
	}
}

func TestPlannedPublicationRejectsChangedLocalCut(t *testing.T) {
	for _, mutate := range []string{"same-size bytes", "extra file", "missing file", "truncated file"} {
		t.Run(mutate, func(t *testing.T) {
			objects := objectstore.NewMemoryStore("")
			store, err := New(objects, ChunkBytes)
			require.NoError(t, err)
			source := privateImage(t, map[string][]byte{"state": []byte("original")})
			plan, err := store.PlanLocal(t.Context(), testBinding(), source)
			require.NoError(t, err)
			switch mutate {
			case "same-size bytes":
				err = os.WriteFile(filepath.Join(source, "state"), []byte("modified"), 0o600)
			case "extra file":
				err = os.WriteFile(filepath.Join(source, "extra"), []byte("x"), 0o600)
			case "missing file":
				err = os.Remove(filepath.Join(source, "state"))
			case "truncated file":
				err = os.Truncate(filepath.Join(source, "state"), 2)
			}
			require.NoError(t, err)
			_, err = store.PublishPlanned(t.Context(), testBinding(), plan, source)
			require.Error(t, err)
			require.Error(t, store.WritePlannedPeerImage(t.Context(), testBinding(), plan, source, io.Discard))
			_, err = objects.Head(manifestKey(plan.Reference.BindingDigest))
			require.Error(t, err, "changed source bytes must not publish a different image under the plan")
		})
	}
}

func TestLocalInventoryBindsAfterInspectionWithoutSharingMutablePlan(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"state": []byte("retained state")})
	inventory, err := store.InspectLocal(t.Context(), source)
	require.NoError(t, err)
	binding := testBinding()
	plan, err := store.BindLocalInventory(binding, inventory)
	require.NoError(t, err)
	expected, err := store.PlanLocal(t.Context(), binding, source)
	require.NoError(t, err)
	require.Equal(t, expected, plan)
	plan.Manifest.Files[0].Chunks[0].Digest = "changed"
	again, err := store.BindLocalInventory(binding, inventory)
	require.NoError(t, err)
	require.Equal(t, expected, again, "returned plans cannot mutate the inventory")
	other := binding
	other.OperationID = "later-bound-operation"
	later, err := store.BindLocalInventory(other, inventory)
	require.NoError(t, err)
	require.NotEqual(t, expected.Reference, later.Reference)
	require.Equal(t, expected.Manifest.Files, later.Manifest.Files)
	_, err = store.BindLocalInventory(binding, LocalImageInventory{})
	require.Error(t, err, "an empty cache entry cannot identify a completed image")
	require.NoError(t, os.WriteFile(filepath.Join(source, "state"), []byte("modified state"), 0o600))
	_, err = store.PublishPlanned(t.Context(), binding, again, source)
	require.Error(t, err, "an earlier inventory cannot authorize changed bytes")
	require.Error(t, store.WritePlannedPeerImage(t.Context(), binding, again, source, io.Discard))
}

func TestPlannedImageBindsManifestAndSourceAndMatchesLegacyPublication(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"state": []byte("retained state"), "empty": {}})
	plan, err := store.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
	other := testBinding()
	other.OperationID = "different-operation"
	_, err = store.PublishPlanned(t.Context(), other, plan, source)
	require.Error(t, err)
	require.Error(t, store.WritePlannedPeerImage(t.Context(), other, plan, source, io.Discard))
	changed := plan
	changed.Reference.ManifestDigest = changed.Reference.BindingDigest
	_, err = store.PublishPlanned(t.Context(), testBinding(), changed, source)
	require.Error(t, err)
	require.Error(t, store.WritePlannedPeerImage(t.Context(), testBinding(), changed, source, io.Discard))
	ref, err := store.Publish(t.Context(), testBinding(), source)
	require.NoError(t, err)
	require.Equal(t, ref, plan.Reference)
	var plannedWire, publishedWire bytes.Buffer
	require.NoError(t, store.WritePlannedPeerImage(t.Context(), testBinding(), plan, source, &plannedWire))
	require.NoError(t, store.WritePeerImage(t.Context(), testBinding(), ref, source, &publishedWire))
	require.Equal(t, publishedWire.Bytes(), plannedWire.Bytes())
}
