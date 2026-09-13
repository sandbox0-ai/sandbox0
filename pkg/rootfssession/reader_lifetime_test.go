package session

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type sessionLifetimeSource struct {
	rootfsblock.RangeSource
	get func(context.Context, string, int64, int64) (io.ReadCloser, error)
}

func (s sessionLifetimeSource) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return s.get(ctx, key, offset, length)
}

func TestSessionReaderOutlivesClaimRequest(t *testing.T) {
	manager, runtime, request := newTestManager(t, "reader-lifetime")
	calls := 0
	manager.source = sessionLifetimeSource{RangeSource: runtime.source, get: func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
		calls++
		require.Equal(t, manager.lifetime, ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return runtime.source.Get(key, offset, length)
	}}
	claim, cancelClaim := context.WithCancel(t.Context())
	defer cancelClaim()
	_, err := manager.Ensure(claim, request)
	require.NoError(t, err)
	cancelClaim()
	manager.mu.Lock()
	branch := manager.live[request.Parent].branch
	manager.mu.Unlock()
	beforeRead := calls
	actual := make([]byte, 1)
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{byte(len("reader-lifetime") + 1)}, actual)
	require.Greater(t, calls, beforeRead, "first file read must reach the context-aware source after claim cancellation")
	require.NoError(t, manager.lifetime.Err())

	current, err := manager.load(request.Parent)
	require.NoError(t, err)
	manager.cancel()
	_, err = manager.reopenBranch(current)
	require.ErrorIs(t, err, context.Canceled, "reopened branches must also use the node lifetime")
}

func TestSessionCloseCancelsActiveReaderTransport(t *testing.T) {
	manager, runtime, request := newTestManager(t, "reader-close")
	started := make(chan struct{})
	manager.source = sessionLifetimeSource{RangeSource: runtime.source, get: func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
		if strings.Contains(key, "/packs/") {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return runtime.source.Get(key, offset, length)
	}}
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	manager.mu.Lock()
	branch := manager.live[request.Parent].branch
	manager.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := branch.ReadAt(make([]byte, 1), 0); done <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("file read did not reach the source")
	}
	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("node shutdown did not cancel active source I/O")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("node shutdown did not finish after source cancellation")
	}
}
