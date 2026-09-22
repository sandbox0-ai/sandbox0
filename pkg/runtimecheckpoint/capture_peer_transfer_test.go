package runtimecheckpoint

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

// This opt-in byte-accounting probe uses a growing 128-MiB random image. It
// proves transfer overlap and repair volume, not runsc restore or migration
// latency: pipes are local, and no manager/RootFS transaction participates.
func TestCapturePeerGrowing128MiBTransferVolume(t *testing.T) {
	if os.Getenv("SANDBOX0_CAPTURE_PEER_PROBE") != "1" {
		t.Skip("set SANDBOX0_CAPTURE_PEER_PROBE=1 for the bounded transfer-volume probe")
	}
	const chunks = 16
	for _, rewrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "rewritten"}[rewrite], func(t *testing.T) {
			const budget = int64((chunks + 2) * ChunkBytes)
			objects := objectstore.NewMemoryStore("")
			store, err := New(objects, budget)
			require.NoError(t, err)
			cache := newPeerCache(t, budget)
			stage, err := store.OpenCaptureStaging(t.Context(), cache.scope, (chunks+2)*ChunkBytes)
			require.NoError(t, err)
			source := filepath.Join(t.TempDir(), "growing")
			require.NoError(t, os.Mkdir(source, 0o700))
			sourceFile, err := os.OpenFile(filepath.Join(source, "pages"), os.O_CREATE|os.O_RDWR, 0o600)
			require.NoError(t, err)
			defer sourceFile.Close()
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			earlyReceived := make(chan error, 1)
			go func() { earlyReceived <- cache.ReceiveGrowing(t.Context(), reader) }()
			wireCounter := &captureByteCounter{Writer: writer}
			peer, err := NewCapturePeerWriter(t.Context(), cache.scope, wireCounter)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			uploaded := make(chan error, 1)
			var sent atomic.Int64
			go func() {
				uploaded <- stage.UploadGrowingWithPeer(ctx, source, func(ctx context.Context, name string, offset int64, data []byte) error {
					err := peer.WriteChunk(ctx, name, offset, data)
					if err == nil {
						sent.Add(1)
					}
					return err
				})
			}()
			buffer := make([]byte, ChunkBytes)
			for i := 0; i < chunks; i++ {
				_, err = rand.Read(buffer)
				require.NoError(t, err)
				_, err = sourceFile.Write(buffer)
				require.NoError(t, err)
				if i == 0 {
					require.Eventually(t, func() bool { return sent.Load() >= 1 }, 10*time.Second, 10*time.Millisecond, "target received data before source capture finished")
					info, err := sourceFile.Stat()
					require.NoError(t, err)
					require.EqualValues(t, ChunkBytes, info.Size())
				}
			}
			require.Eventually(t, func() bool { return sent.Load() == chunks }, 20*time.Second, 10*time.Millisecond)
			cancel()
			require.ErrorIs(t, <-uploaded, context.Canceled)
			require.NoError(t, peer.Finish(t.Context()))
			require.NoError(t, writer.Close())
			require.NoError(t, <-earlyReceived)
			if rewrite {
				_, err = rand.Read(buffer)
				require.NoError(t, err)
				_, err = sourceFile.WriteAt(buffer, 7*ChunkBytes)
				require.NoError(t, err)
			}
			require.NoError(t, sourceFile.Sync())
			require.NoError(t, sourceFile.Close())
			inventory, err := cache.Inventory(t.Context())
			require.NoError(t, err)
			encoded, err := inventory.Encode(cache.scope, budget)
			require.NoError(t, err)
			inventory, err = DecodeCapturePeerInventory(encoded, cache.scope, budget)
			require.NoError(t, err)
			plan, err := stage.PlanLocal(t.Context(), testBinding(), source)
			require.NoError(t, err)
			// Count the existing complete-image protocol over the same final source.
			full := &captureByteCounter{Writer: io.Discard}
			require.NoError(t, store.WritePlannedPeerImage(t.Context(), testBinding(), plan, source, full))
			finalReader, finalWriter := io.Pipe()
			defer finalReader.Close()
			defer finalWriter.Close()
			finalCount := &captureByteCounter{Writer: finalWriter}
			finalDone := make(chan error, 1)
			go func() {
				err := store.WriteCapturePeerFinal(t.Context(), testBinding(), plan, source, inventory, finalCount)
				_ = finalWriter.CloseWithError(err)
				finalDone <- err
			}()
			got, err := cache.ReceiveFinal(t.Context(), testBinding(), plan.Reference, finalReader)
			require.NoError(t, err)
			require.NoError(t, <-finalDone)
			require.Equal(t, plan.Manifest, got)
			actual, err := store.InspectLocal(t.Context(), cache.directory)
			require.NoError(t, err)
			require.Equal(t, plan.Manifest.Files, actual.files)
			payloadBytes := int64(0)
			if rewrite {
				payloadBytes = ChunkBytes
			}
			require.GreaterOrEqual(t, finalCount.bytes, payloadBytes)
			require.Less(t, finalCount.bytes, payloadBytes+8192)
			report, _ := json.Marshal(map[string]any{"image_bytes": chunks * ChunkBytes, "rewritten_chunks": map[bool]int{false: 0, true: 1}[rewrite], "early_wire_bytes": wireCounter.bytes, "inventory_bytes": len(encoded), "full_wire_bytes": full.bytes, "final_wire_bytes": finalCount.bytes, "full_hash_verification": true})
			t.Log(string(report))
			// The probe never publishes a regional manifest; collect tentative uploads
			// using its isolated test-only scope, not any live node's migration prefix.
			collector, err := NewCollector(objects)
			require.NoError(t, err)
			done := false
			for attempt := 0; attempt < 5 && !done; attempt++ {
				done, err = collector.CollectCapture(t.Context(), cache.scope)
				require.NoError(t, err)
			}
			require.True(t, done)
		})
	}
}

type captureByteCounter struct {
	io.Writer
	bytes int64
}

func (w *captureByteCounter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.bytes += int64(n)
	return n, err
}

func TestCapturePeerRejectsSymlinkAndRetainsNoUnboundedHoles(t *testing.T) {
	cache := newPeerCache(t, 3*ChunkBytes)
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("untouched"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(cache.directory, "pages")))
	wire := tentativeWire(t, cache.scope, map[string][]byte{"pages": bytes.Repeat([]byte{1}, ChunkBytes)})
	require.Error(t, cache.ReceiveGrowing(t.Context(), bytes.NewReader(wire)))
	got, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, []byte("untouched"), got)
	cache2 := newPeerCache(t, 2*ChunkBytes)
	cache2.mu.Lock()
	err = cache2.put(t.Context(), "pages", MaxImageBytes-ChunkBytes, make([]byte, ChunkBytes))
	cache2.mu.Unlock()
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(cache2.directory, "pages"))
}
