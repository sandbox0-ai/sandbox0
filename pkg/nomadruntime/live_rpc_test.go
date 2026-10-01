package nomadruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
)

type liveBlockedRuntime struct {
	*fakeRootFSRuntime
	entered chan struct{}
	finish  chan struct{}
}

func (r *liveBlockedRuntime) Ensure(context.Context, rootfshandoff.StageRequest, func(error)) (rootfssession.Mount, error) {
	close(r.entered)
	<-r.finish
	return rootfssession.Mount{}, nil
}

func TestLiveRPCQuiescenceJoinsAcceptedOperationsBeforeOwnerTransfer(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "runtime.sock")
	runtime := &liveBlockedRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}, entered: make(chan struct{}), finish: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- serveNodeRuntime(ctx, socket, runtime, nil, nil, nil) }()
	require.Eventually(t, func() bool { _, err := os.Stat(socket); return err == nil }, time.Second, time.Millisecond)
	client, err := NewClient(socket)
	require.NoError(t, err)
	call := make(chan error, 1)
	go func() { _, err := client.Ensure(t.Context(), rootfshandoff.StageRequest{}, nil); call <- err }()
	<-runtime.entered
	cancel()
	select {
	case err := <-stopped:
		t.Fatalf("RPC owner exited with an accepted operation still executing: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(runtime.finish)
	<-call
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("RPC owner did not finish after operation completed")
	}
}
