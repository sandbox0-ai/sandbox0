package session

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

const rustFSIntegrationEndpointEnv = "SANDBOX0_RUSTFS_ENDPOINT"

// TestRustFSDirtyTailPressureRetiresAfterObjectStoreRecovery exercises the
// real S3 client and RustFS conditional-create behavior through a controllable
// outage proxy. It is opt-in because the repository test suite does not own a
// RustFS process.
func TestRustFSDirtyTailPressureRetiresAfterObjectStoreRecovery(t *testing.T) {
	endpoint := os.Getenv(rustFSIntegrationEndpointEnv)
	if endpoint == "" {
		t.Skipf("set %s to a test RustFS endpoint", rustFSIntegrationEndpointEnv)
	}
	upstream, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.NotEmpty(t, upstream.Scheme)
	require.NotEmpty(t, upstream.Host)

	var unavailable atomic.Bool
	reverseProxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if unavailable.Load() {
			http.Error(writer, "injected RustFS outage", http.StatusServiceUnavailable)
			return
		}
		reverseProxy.ServeHTTP(writer, request)
	}))
	t.Cleanup(proxy.Close)

	bucket := "s0-pressure-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	store, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: bucket, Region: "us-east-1", Endpoint: proxy.URL,
		AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"),
		SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY"),
	})
	require.NoError(t, err)
	require.NoError(t, store.Create())
	conditional, ok := store.(objectstore.ConditionalStore)
	require.True(t, ok)
	publisher := rootfsblock.ObjectStorePublisher{Store: conditional}

	memory := newSessionObjectStore()
	request := testStageRequestWithBlocks(t, memory, "rustfs-pressure", 16)
	memory.mu.Lock()
	baseObjects := make(map[string][]byte, len(memory.objects))
	for key, payload := range memory.objects {
		baseObjects[key] = append([]byte(nil), payload...)
	}
	memory.mu.Unlock()
	for key, payload := range baseObjects {
		require.NoError(t, publisher.PutImmutable(t.Context(), key, payload))
	}

	root := t.TempDir()
	runtime := newFakeHostRuntime(memory)
	manager, err := New(Config{
		StatePath: filepath.Join(root, "state", "sessions.db"), BranchRoot: filepath.Join(root, "branches"),
		MountRoot: filepath.Join(root, "mounts"), MaxDirtyTailBytes: 12 * rootfsblock.LogicalBlockSize,
		Source: conditional, Publisher: publisher, Runtime: runtime,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	_, err = manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	manager.mu.Lock()
	branch := manager.live[request.Parent].branch
	manager.mu.Unlock()
	for block := range 12 {
		_, err = branch.WriteAt(
			bytes.Repeat([]byte{byte(block + 1)}, rootfsblock.LogicalBlockSize),
			int64(block*rootfsblock.LogicalBlockSize),
		)
		require.NoError(t, err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := branch.WriteAt([]byte{0xee}, 12*rootfsblock.LogicalBlockSize)
		writeDone <- writeErr
	}()
	select {
	case <-manager.DirtyTailPressureSignal():
	case <-time.After(2 * time.Second):
		t.Fatal("dirty-tail pressure did not wake the owner")
	}
	pressures, err := manager.DirtyTailPressureSessions()
	require.NoError(t, err)
	require.Len(t, pressures, 1)
	operationID := rootfshandoff.PlannedRetireOperationID(
		request.Parent, request.Identity.WriterGrantID, request.Identity.WriterEpoch,
	)
	require.NoError(t, manager.BeginRetire(request.Parent, request.Identity, operationID))
	require.NoError(t, branch.BeginRetirement())
	require.NoError(t, <-writeDone)

	unavailable.Store(true)
	err = manager.Release(t.Context(), request.Identity)
	require.Error(t, err)
	interrupted, loadErr := manager.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateReleasing, interrupted.State)
	require.Equal(t, operationID, interrupted.RetireOperationID)
	require.Greater(t, manager.NodeDirtyTailUsage().UsedBytes, int64(0),
		"unacknowledged journals remain charged during an object-store outage")

	unavailable.Store(false)
	require.NoError(t, manager.ReconcileReleases(t.Context()))
	result, err := manager.RetireResult(request.Parent, request.Identity, operationID)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityS3, result.DurabilityState)
	descriptor, err := rootfsblock.DecodeDescriptor(result.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(conditional, descriptor, 1<<20)
	require.NoError(t, err)
	actual := make([]byte, rootfsblock.LogicalBlockSize)
	_, err = reader.ReadAt(actual, 12*rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, byte(0xee), actual[0])
}
