//go:build linux

package gvisorcli

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// This probe measures real runsc's early reusable bytes and verifies restoration
// from the repaired image. It uses a local pipe unless an explicit remote test
// receiver is configured; neither mode measures complete migration latency.
type checkpointPeerUploadProbe struct {
	cache           *runtimecheckpoint.CapturePeerCache
	remote          *checkpointPeerRemote
	directory       string
	reader          *io.PipeReader
	writer          *io.PipeWriter
	peer            *runtimecheckpoint.CapturePeerWriter
	received        chan error
	early           *checkpointPeerByteCounter
	stopOnce        sync.Once
	stopErr         error
	publishDuration time.Duration
	repairDuration  time.Duration
	verifyDuration  time.Duration
}

func newCheckpointPeerUploadProbe(t *testing.T, ctx context.Context, scope runtimecheckpoint.CaptureScope, binding runtimecheckpoint.Binding, directory string) *checkpointPeerUploadProbe {
	if os.Getenv("SANDBOX0_CAPTURE_PEER_REMOTE_CONFIG") != "" {
		return newCheckpointPeerRemoteProbe(t, ctx, scope, binding, directory)
	}
	cache, err := runtimecheckpoint.NewCapturePeerCache(ctx, scope, directory, 512<<20, func(int64, uint64) error { return nil })
	require.NoError(t, err)
	reader, writer := io.Pipe()
	p := &checkpointPeerUploadProbe{cache: cache, directory: directory, reader: reader, writer: writer, received: make(chan error, 1), early: &checkpointPeerByteCounter{Writer: writer}}
	t.Cleanup(func() { _ = p.stop(); _ = reader.Close(); _ = writer.Close(); require.NoError(t, cache.Close()) })
	go func() { p.received <- cache.ReceiveGrowing(ctx, reader) }()
	p.peer, err = runtimecheckpoint.NewCapturePeerWriter(ctx, scope, p.early)
	require.NoError(t, err)
	return p
}

func (p *checkpointPeerUploadProbe) stop() error {
	p.stopOnce.Do(func() {
		if p.remote != nil {
			p.remote.cancel()
		}
		_ = p.writer.CloseWithError(context.Canceled)
		p.stopErr = <-p.received
		if errors.Is(p.stopErr, context.Canceled) {
			p.stopErr = nil
		}
	})
	return p.stopErr
}

func (p *checkpointPeerUploadProbe) finish(ctx context.Context, store *runtimecheckpoint.Store, stage *runtimecheckpoint.CaptureStager,
	binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, source string) (int64, int64, error) {
	if p.remote != nil {
		return p.finishRemote(ctx, store, stage, binding, plan, source)
	}
	inventory, err := p.cache.Inventory(ctx)
	if err != nil {
		return 0, 0, err
	}
	var tentativeBytes int64
	for _, file := range inventory.Files {
		tentativeBytes += file.Size
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	wire := &checkpointPeerByteCounter{Writer: writer}
	group, groupCtx := errgroup.WithContext(ctx)
	stop := context.AfterFunc(groupCtx, func() { _ = reader.CloseWithError(groupCtx.Err()); _ = writer.CloseWithError(groupCtx.Err()) })
	defer stop()
	var ref runtimecheckpoint.Reference
	group.Go(func() error {
		started := time.Now()
		defer func() { p.publishDuration = time.Since(started) }()
		var err error
		ref, err = stage.PublishPlanned(groupCtx, binding, plan, source)
		return err
	})
	group.Go(func() error {
		err := store.WriteCapturePeerFinal(groupCtx, binding, plan, source, inventory, wire)
		_ = writer.CloseWithError(err)
		return err
	})
	group.Go(func() error {
		started := time.Now()
		defer func() { p.repairDuration = time.Since(started) }()
		_, err := p.cache.ReceiveFinal(groupCtx, binding, plan.Reference, reader)
		return err
	})
	if err := group.Wait(); err != nil {
		return tentativeBytes, wire.bytes, err
	}
	if ref != plan.Reference {
		return tentativeBytes, wire.bytes, errors.New("peer probe publication changed reference")
	}
	verifyStarted := time.Now()
	_, err = store.VerifyLocal(ctx, binding, ref, p.directory)
	p.verifyDuration = time.Since(verifyStarted)
	return tentativeBytes, wire.bytes, err
}

type checkpointPeerByteCounter struct {
	io.Writer
	bytes int64
}

func (w *checkpointPeerByteCounter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.bytes += int64(n)
	return n, err
}

func finishCheckpointPeerUploadProbe(t *testing.T, ctx context.Context, p *checkpointPeerUploadProbe, store *runtimecheckpoint.Store,
	stage *runtimecheckpoint.CaptureStager, binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, source string, started, remainingStarted time.Time) string {
	tentativeBytes, finalBytes, err := p.finish(ctx, store, stage, binding, plan, source)
	require.NoError(t, err)
	var size int64
	for _, file := range plan.Manifest.Files {
		size += file.Size
	}
	var timings runtimecheckpoint.CapturePeerTimings
	if p.remote != nil {
		timings = p.remote.timings
	} else {
		timings = p.cache.Timings()
	}
	t.Logf("capture_peer image_bytes=%d early_inventory_bytes=%d early_wire_bytes=%d final_wire_bytes=%d capture_sync_publish_peer_verify_us=%d remaining_publish_peer_verify_us=%d final_publish_us=%d final_repair_us=%d final_verify_us=%d repair_read_us=%d repair_hash_us=%d repair_sync_us=%d", size, tentativeBytes, p.early.bytes, finalBytes, time.Since(started).Microseconds(), time.Since(remainingStarted).Microseconds(), p.publishDuration.Microseconds(), p.repairDuration.Microseconds(), p.verifyDuration.Microseconds(), timings.Read.Microseconds(), timings.Hash.Microseconds(), timings.Sync.Microseconds())
	if p.remote != nil {
		downloaded := time.Now()
		require.NoError(t, p.downloadRemote(ctx, binding, plan.Reference))
		t.Logf("capture_peer_return_copy_us=%d excluded_from_capture_metric=true", time.Since(downloaded).Microseconds())
	}
	return p.directory
}
