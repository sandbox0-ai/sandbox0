package runtimecheckpoint

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func newPeerCache(t *testing.T, maxBytes int64) *CapturePeerCache {
	t.Helper()
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	c, err := NewCapturePeerCache(t.Context(), scope, filepath.Join(t.TempDir(), "capture"), maxBytes, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func tentativeWire(t *testing.T, scope CaptureScope, files map[string][]byte) []byte {
	t.Helper()
	var wire bytes.Buffer
	w, err := NewCapturePeerWriter(t.Context(), scope, &wire)
	require.NoError(t, err)
	for name, data := range files {
		for offset := 0; offset+ChunkBytes <= len(data); offset += ChunkBytes {
			require.NoError(t, w.WriteChunk(t.Context(), name, int64(offset), data[offset:offset+ChunkBytes]))
		}
	}
	require.NoError(t, w.Finish(t.Context()))
	return wire.Bytes()
}

func TestCapturePeerRepairsRewrittenPagesInPlaceAndReusesUnchangedRanges(t *testing.T) {
	cache := newPeerCache(t, 4*ChunkBytes)
	first, second := bytes.Repeat([]byte{1}, ChunkBytes), bytes.Repeat([]byte{2}, ChunkBytes)
	// Preallocation and a partial final frame cannot become completion evidence.
	early := append(make([]byte, ChunkBytes), second...)
	require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"nested/pages": early}))))
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	original, err := os.Stat(filepath.Join(cache.directory, "nested/pages"))
	require.NoError(t, err)
	final := append(append(bytes.Clone(first), second...), []byte("short final tail")...)
	source := privateImage(t, map[string][]byte{"nested/pages": final, "state": []byte("process metadata"), "empty": {}})
	store, err := New(objectstore.NewMemoryStore(""), 4*ChunkBytes)
	require.NoError(t, err)
	plan, err := store.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, &wire))
	require.Less(t, wire.Len(), ChunkBytes+8192, "only rewritten chunk and short tails cross the final stream")
	manifest, err := cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, bytes.NewReader(wire.Bytes()))
	require.NoError(t, err)
	require.Equal(t, plan.Manifest, manifest)
	now, err := os.Stat(filepath.Join(cache.directory, "nested/pages"))
	require.NoError(t, err)
	require.True(t, os.SameFile(original, now), "final image reuses the same inode")
	actual, err := os.ReadFile(filepath.Join(cache.directory, "nested/pages"))
	require.NoError(t, err)
	require.Equal(t, final, actual)
	// Peer completion is explicitly not evidence of regional publication.
	_, err = store.VerifyLocal(t.Context(), testBinding(), plan.Reference, cache.directory)
	require.Error(t, err)
	ref, err := store.PublishPlanned(t.Context(), testBinding(), plan, source)
	require.NoError(t, err)
	_, err = store.VerifyLocal(t.Context(), testBinding(), ref, cache.directory)
	require.NoError(t, err)
	require.Error(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(nil)))
}

func TestCapturePeerInterruptedTentativeFrameIsAMiss(t *testing.T) {
	cache := newPeerCache(t, 3*ChunkBytes)
	data := bytes.Repeat([]byte{3}, ChunkBytes)
	wire := tentativeWire(t, cache.scope, map[string][]byte{"pages": append(bytes.Clone(data), data...)})
	require.Error(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(wire[:len(wire)-100])))
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	require.Len(t, inventory.Files, 1)
	require.Len(t, inventory.Files[0].Chunks, 1)
	source := privateImage(t, map[string][]byte{"pages": append(bytes.Clone(data), data...)})
	store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
	require.NoError(t, err)
	plan, err := store.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
	var repair bytes.Buffer
	require.NoError(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, &repair))
	require.Greater(t, repair.Len(), ChunkBytes)
	require.Less(t, repair.Len(), ChunkBytes+8192)
	_, err = cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, &repair)
	require.NoError(t, err)
}

func TestCapturePeerRejectsBadFramingScopeAndQuotaBeforeWrites(t *testing.T) {
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	wire := tentativeWire(t, scope, map[string][]byte{"pages": bytes.Repeat([]byte{1}, ChunkBytes)})
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"other scope", func(b []byte) []byte { b[8] ^= 1; return b }},
		{"unknown version", func(b []byte) []byte { b[7]++; return b }},
		{"path overflow", func(b []byte) []byte { binary.BigEndian.PutUint16(b[40:42], 1025); return b }},
		{"offset overflow", func(b []byte) []byte { binary.BigEndian.PutUint64(b[42:50], ^uint64(0)); return b }},
		{"unaligned offset", func(b []byte) []byte { binary.BigEndian.PutUint64(b[42:50], 1); return b }},
		{"path traversal", func(b []byte) []byte { copy(b[82:87], "../xx"); return b }},
		{"corrupt chunk", func(b []byte) []byte { b[len(b)-3] ^= 1; return b }},
		{"missing terminator", func(b []byte) []byte { return b[:len(b)-2] }},
		{"trailing bytes", func(b []byte) []byte { return append(b, 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := newPeerCache(t, ChunkBytes+4096)
			require.Error(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(test.mutate(bytes.Clone(wire)))))
		})
	}
	directory := filepath.Join(t.TempDir(), "denied")
	denied := errors.New("quota unavailable")
	_, err = NewCapturePeerCache(t.Context(), scope, directory, ChunkBytes, func(int64, uint64) error { return denied })
	require.ErrorIs(t, err, denied)
	require.NoDirExists(t, directory)
	cache := newPeerCache(t, ChunkBytes+4096)
	require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(wire)))
	require.Error(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, scope, map[string][]byte{"second": bytes.Repeat([]byte{2}, ChunkBytes)}))))
	require.NoFileExists(t, filepath.Join(cache.directory, "second"))
}

func TestCapturePeerFinalRejectsFalseHintsCorruptionAndIncompleteStreams(t *testing.T) {
	data := bytes.Repeat([]byte{7}, ChunkBytes)
	source := privateImage(t, map[string][]byte{"pages": data})
	store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
	require.NoError(t, err)
	plan, err := store.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
	for _, name := range []string{"changed destination", "false inventory", "truncated final", "missing completion", "legacy version", "trailing final", "unknown mode", "wrong reference", "wrong source"} {
		t.Run(name, func(t *testing.T) {
			cache := newPeerCache(t, 2*ChunkBytes)
			require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": data}))))
			inventory, err := cache.Inventory(t.Context())
			require.NoError(t, err)
			if name == "changed destination" || name == "false inventory" {
				require.NoError(t, os.WriteFile(filepath.Join(cache.directory, "pages"), bytes.Repeat([]byte{8}, ChunkBytes), 0o600))
			}
			var wire bytes.Buffer
			require.NoError(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, &wire))
			payload := wire.Bytes()
			binding, ref := testBinding(), plan.Reference
			switch name {
			case "truncated final":
				payload = payload[:len(payload)-1]
			case "missing completion":
				payload = payload[:len(payload)-len(captureFinalComplete)]
			case "legacy version":
				payload[7] = 2
			case "trailing final":
				payload = append(payload, 1)
			case "unknown mode":
				payload[len(payload)-len(captureFinalComplete)-1] = 2
			case "wrong reference":
				ref.ManifestDigest = "sha256:" + string(bytes.Repeat([]byte{'a'}, 64))
			case "wrong source":
				binding.OperationID = "another-source"
			}
			_, err = cache.ReceiveFinal(t.Context(), binding, ref, bytes.NewReader(payload))
			require.Error(t, err)
		})
	}
	cache := newPeerCache(t, 2*ChunkBytes)
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	inventory.ScopeDigest = "sha256:" + string(bytes.Repeat([]byte{'b'}, 64))
	require.Error(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, io.Discard))
}

func TestGrowingUploadSharesBuffersWithPeerBeforeFinalRootFSCut(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
	require.NoError(t, err)
	cache := newPeerCache(t, 2*ChunkBytes)
	stage, err := store.OpenCaptureStaging(t.Context(), cache.scope, 2*ChunkBytes)
	require.NoError(t, err)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	received := make(chan error, 1)
	go func() { received <- cache.ReceiveGrowing(t.Context(), reader) }()
	stream, err := NewCapturePeerWriter(t.Context(), cache.scope, writer)
	require.NoError(t, err)
	directory := filepath.Join(t.TempDir(), "producer")
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- stage.UploadGrowingWithPeer(ctx, directory, func(ctx context.Context, name string, offset int64, data []byte) error {
			err := stream.WriteChunk(ctx, name, offset, data)
			if err == nil {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
			return err
		})
	}()
	require.NoError(t, os.Mkdir(directory, 0o700))
	// Atomic introduction avoids testing the deliberately untrusted preallocation
	// path again; the fixture has no final RootFS binding/publication yet.
	fixture := filepath.Join(filepath.Dir(directory), "ready")
	require.NoError(t, os.WriteFile(fixture, bytes.Repeat([]byte{9}, ChunkBytes), 0o600))
	require.NoError(t, os.Rename(fixture, filepath.Join(directory, "pages")))
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("destination did not receive during capture")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, stream.Finish(t.Context()))
	require.NoError(t, writer.Close())
	require.NoError(t, <-received)
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	require.Len(t, inventory.Files, 1)
	require.Len(t, inventory.Files[0].Chunks, 1)
	plan, err := stage.PlanLocal(t.Context(), testBinding(), directory)
	require.NoError(t, err)
	var repair bytes.Buffer
	require.NoError(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, directory, inventory, &repair))
	require.Less(t, repair.Len(), 8192)
	_, err = cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, &repair)
	require.NoError(t, err)
	ref, err := stage.PublishPlanned(t.Context(), testBinding(), plan, directory)
	require.NoError(t, err)
	_, err = store.VerifyLocal(t.Context(), testBinding(), ref, cache.directory)
	require.NoError(t, err)
}

func TestCapturePeerInventoryIsBoundedCanonicalAndOwned(t *testing.T) {
	cache := newPeerCache(t, 2*ChunkBytes)
	require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": bytes.Repeat([]byte{1}, ChunkBytes)}))))
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	payload, err := inventory.Encode(cache.scope, cache.maxBytes)
	require.NoError(t, err)
	decoded, err := DecodeCapturePeerInventory(payload, cache.scope, cache.maxBytes)
	require.NoError(t, err)
	require.Equal(t, inventory, decoded)
	for _, bad := range [][]byte{append(bytes.Clone(payload), ' '), append(bytes.Clone(payload), []byte("{}")...), bytes.Replace(payload, []byte("\"files\":"), []byte("\"unknown\":1,\"files\":"), 1), bytes.Replace(payload, []byte("\"files\":"), []byte("\"files\":[],\"files\":"), 1), bytes.Repeat([]byte{' '}, MaxManifestBytes+1)} {
		_, err := DecodeCapturePeerInventory(bad, cache.scope, cache.maxBytes)
		require.Error(t, err)
	}
	inventory.Files[0].Chunks[0].Digest = "invalid"
	fresh, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	require.Equal(t, decoded, fresh)
	require.Error(t, inventory.validate(cache.scope, cache.maxBytes))
	require.Error(t, fresh.validate(cache.scope, ChunkBytes-1))
}

func TestCapturePeerBoundsRepeatedStreamsAndClosesOnlyAfterReaderExits(t *testing.T) {
	cache := newPeerCache(t, ChunkBytes+4096)
	wire := tentativeWire(t, cache.scope, map[string][]byte{"pages": bytes.Repeat([]byte{1}, ChunkBytes)})
	require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(wire)))
	require.ErrorContains(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(wire)), "frame budget", "reopening a stream does not reset admission")
	blocked := newPeerCache(t, 2*ChunkBytes)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- blocked.ReceiveGrowing(ctx, &notifyReader{Reader: reader, entered: entered}) }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- blocked.Close() }()
	select {
	case <-closed:
		t.Fatal("cache closed while a reader still owns it")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	require.NoError(t, writer.CloseWithError(context.Canceled))
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, <-closed)
}

type notifyReader struct {
	io.Reader
	entered chan struct{}
	once    sync.Once
}

func (r *notifyReader) Read(b []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.Reader.Read(b)
}

func TestGrowingPeerCancellationJoinsBorrowedBufferConsumer(t *testing.T) {
	store, err := New(objectstore.NewMemoryStore(""), ChunkBytes)
	require.NoError(t, err)
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 2*ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"pages": bytes.Repeat([]byte{4}, ChunkBytes)})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- stage.UploadGrowingWithPeer(ctx, source, func(ctx context.Context, _ string, _ int64, data []byte) error {
			close(entered)
			<-ctx.Done()
			<-release
			if data[0] != 4 {
				return errors.New("borrowed source buffer was reused before join")
			}
			return ctx.Err()
		})
	}()
	<-entered
	cancel()
	select {
	case <-done:
		t.Fatal("source released custody before the peer returned")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.ErrorIs(t, <-done, context.Canceled)
	_, err = stage.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
}

func TestCapturePeerCanRepairFromEmptyCacheAndRejectExtraFiles(t *testing.T) {
	for _, extra := range []bool{false, true} {
		cache := newPeerCache(t, 2*ChunkBytes)
		if extra {
			require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"unexpected": bytes.Repeat([]byte{1}, ChunkBytes)}))))
		}
		inventory, err := cache.Inventory(t.Context())
		require.NoError(t, err)
		source := privateImage(t, map[string][]byte{"nested/state": []byte("checkpoint state"), "empty": {}})
		store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
		require.NoError(t, err)
		plan, err := store.PlanLocal(t.Context(), testBinding(), source)
		require.NoError(t, err)
		var wire bytes.Buffer
		require.NoError(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, &wire))
		_, err = cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, &wire)
		if extra {
			require.Error(t, err)
			require.NoFileExists(t, filepath.Join(cache.directory, "nested/state"))
		} else {
			require.NoError(t, err)
		}
	}
}

func TestCapturePeerRetainsWriteDescriptorThroughFinalSyncAndRejectsSwaps(t *testing.T) {
	for _, swap := range []bool{false, true} {
		cache := newPeerCache(t, 2*ChunkBytes)
		data := bytes.Repeat([]byte{2}, ChunkBytes)
		require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": data}))))
		retained := cache.handles["pages"]
		require.NotNil(t, retained)
		inventory, err := cache.Inventory(t.Context())
		require.NoError(t, err)
		source := privateImage(t, map[string][]byte{"pages": data})
		store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
		require.NoError(t, err)
		plan, err := store.PlanLocal(t.Context(), testBinding(), source)
		require.NoError(t, err)
		var wire bytes.Buffer
		require.NoError(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, &wire))
		delayed := errors.New("writeback error on original descriptor")
		if swap {
			replacement := filepath.Join(filepath.Dir(cache.directory), "replacement")
			require.NoError(t, os.WriteFile(replacement, data, 0o600))
			require.NoError(t, os.Rename(replacement, filepath.Join(cache.directory, "pages")))
		} else {
			cache.syncFile = func(file *os.File) error { require.Same(t, retained, file); return delayed }
		}
		_, err = cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, &wire)
		if swap {
			require.ErrorContains(t, err, "identity changed")
		} else {
			require.ErrorIs(t, err, delayed)
		}
		require.NoError(t, cache.Close())
		_, err = retained.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
	}
}

func TestCapturePeerParallelReuseStillRejectsChangedSource(t *testing.T) {
	cache := newPeerCache(t, 2*ChunkBytes)
	data := bytes.Repeat([]byte{5}, ChunkBytes)
	require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": data}))))
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"pages": data})
	store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
	require.NoError(t, err)
	plan, err := store.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "pages"), bytes.Repeat([]byte{6}, ChunkBytes), 0o600))
	var wire bytes.Buffer
	require.ErrorContains(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, &wire), "planned chunk content")
	_, err = cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, &wire)
	require.Error(t, err)
}

func TestCapturePeerWritebackUsesRetainedDescriptorAndFailureInvalidatesCache(t *testing.T) {
	cache := newPeerCache(t, 2*ChunkBytes)
	failed := errors.New("asynchronous writeback unavailable")
	cache.writeback = func(file *os.File, offset, length int64) error {
		require.Same(t, cache.handles["pages"], file)
		require.Zero(t, offset)
		require.EqualValues(t, ChunkBytes, length)
		return failed
	}
	require.ErrorIs(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": bytes.Repeat([]byte{1}, ChunkBytes)}))), failed)
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	require.Empty(t, inventory.Files)
	_, err = cache.ReceiveFinal(t.Context(), testBinding(), Reference{}, bytes.NewReader(nil))
	require.Error(t, err)
	require.Same(t, failed, cache.writeErr)
}

// A fully verified local file is not sufficient when source validation fails
// after sending its reuse frames. Match the node handler's late HTTP abort.
func TestCapturePeerFinalRequiresSuccessfulEOFWithVerifiedCachedFile(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid source", true: "late source corruption"}[corrupt], func(t *testing.T) {
			data := bytes.Repeat([]byte{7}, ChunkBytes)
			source := privateImage(t, map[string][]byte{"pages": data})
			store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
			require.NoError(t, err)
			plan, err := store.PlanLocal(t.Context(), testBinding(), source)
			require.NoError(t, err)
			cache := newPeerCache(t, 2*ChunkBytes)
			require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": data}))))
			inventory, err := cache.Inventory(t.Context())
			require.NoError(t, err)
			if corrupt {
				require.NoError(t, os.WriteFile(filepath.Join(source, "pages"), bytes.Repeat([]byte{8}, ChunkBytes), 0o600))
			}
			verifiedAndSynced := false
			syncFile := cache.syncFile
			cache.syncFile = func(file *os.File) error {
				verifiedAndSynced = true
				return syncFile(file)
			}
			sent := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				err := store.WriteCapturePeerFinal(r.Context(), testBinding(), plan, source, inventory, capturePeerFlushingWriter{w})
				sent <- err
				if err != nil {
					panic(http.ErrAbortHandler)
				}
			}))
			defer server.Close()
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
			require.NoError(t, err)
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			_, receiveErr := cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, response.Body)
			sourceErr := <-sent
			require.True(t, verifiedAndSynced, "cached file verification runs before the final transport outcome")
			if corrupt {
				require.ErrorContains(t, sourceErr, "changed planned chunk content")
				require.ErrorContains(t, receiveErr, "incomplete final capture verification")
				require.True(t, cache.finalizing, "failure retains image custody and excludes new tentative writes")
			} else {
				require.NoError(t, sourceErr)
				require.NoError(t, receiveErr)
			}
		})
	}
}

type capturePeerFlushingWriter struct{ http.ResponseWriter }

func (w capturePeerFlushingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.ResponseWriter.(http.Flusher).Flush()
	return n, err
}

func TestCapturePeerFinalFlushFailureCannotCompleteRetainedImage(t *testing.T) {
	cache := newPeerCache(t, 2*ChunkBytes)
	data := bytes.Repeat([]byte{5}, ChunkBytes)
	require.NoError(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(tentativeWire(t, cache.scope, map[string][]byte{"pages": data}))))
	inventory, err := cache.Inventory(t.Context())
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"pages": data})
	store, err := New(objectstore.NewMemoryStore(""), 2*ChunkBytes)
	require.NoError(t, err)
	plan, err := store.PlanLocal(t.Context(), testBinding(), source)
	require.NoError(t, err)
	failed := errors.New("frame flush failed")
	wire := &capturePeerFailedFlush{failure: failed}
	require.ErrorIs(t, store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, wire), failed)
	_, err = cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, &wire.Buffer)
	require.ErrorContains(t, err, "incomplete final capture verification")
}

type capturePeerFailedFlush struct {
	bytes.Buffer
	failure error
}

func (w *capturePeerFailedFlush) Flush() error { return w.failure }
