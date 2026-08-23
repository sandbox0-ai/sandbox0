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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
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

	requestMeter := &recordingRequestObserver{}
	conditional := newRustFSIntegrationStore(t, proxy.URL, requestMeter)
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
	requestMeter.reset()

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
	assertObjectRequestBudget(t, requestMeter.take(), 2, 0, 0)
	_, err = manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.Empty(t, requestMeter.take(), "hot attached claims must not reach object storage")
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
	assertObjectRequestBudget(t, requestMeter.take(), 2, 0, 0)

	unavailable.Store(true)
	err = manager.Release(t.Context(), request.Identity)
	require.Error(t, err)
	outageAttempts := requestMeter.take()
	require.NotEmpty(t, outageAttempts)
	require.LessOrEqual(t, len(outageAttempts), 4, "one SDK operation has a bounded retry budget")
	for _, attempt := range outageAttempts {
		require.Equal(t, http.StatusServiceUnavailable, attempt.StatusCode)
		require.NotContains(t, []string{"HeadBucket", "HeadObject", "ListObjects", "ListObjectsV2"}, attempt.Operation)
	}
	interrupted, loadErr := manager.load(request.Parent)
	require.NoError(t, loadErr)
	require.Equal(t, stateReleasing, interrupted.State)
	require.Equal(t, operationID, interrupted.RetireOperationID)
	require.Greater(t, manager.NodeDirtyTailUsage().UsedBytes, int64(0),
		"unacknowledged journals remain charged during an object-store outage")

	unavailable.Store(false)
	require.NoError(t, manager.ReconcileReleases(t.Context()))
	assertObjectRequestBudget(t, requestMeter.take(), 4, 4, 0)
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
	assertObjectRequestBudget(t, requestMeter.take(), 3, 0, 0)
}

// TestRustFSRootFSMigratesBetweenIndependentNodes models the regional handoff
// boundary over a real S3-compatible store. Node B receives only node A's
// published immutable head and never reuses node A's branch journal.
func TestRustFSRootFSMigratesBetweenIndependentNodes(t *testing.T) {
	endpoint := os.Getenv(rustFSIntegrationEndpointEnv)
	if endpoint == "" {
		t.Skipf("set %s to a test RustFS endpoint", rustFSIntegrationEndpointEnv)
	}
	conditional := newRustFSIntegrationStore(t, endpoint, nil)
	publisher := rootfsblock.ObjectStorePublisher{Store: conditional}

	memory := newSessionObjectStore()
	first := testStageRequestWithBlocks(t, memory, "rustfs-node-a", 16)
	memory.mu.Lock()
	baseObjects := make(map[string][]byte, len(memory.objects))
	for key, payload := range memory.objects {
		baseObjects[key] = append([]byte(nil), payload...)
	}
	memory.mu.Unlock()
	for key, payload := range baseObjects {
		require.NoError(t, publisher.PutImmutable(t.Context(), key, payload))
	}

	nodeARoot := t.TempDir()
	nodeARuntime := newFakeHostRuntime(memory)
	nodeA, err := New(Config{
		StatePath:  filepath.Join(nodeARoot, "state", "sessions.db"),
		BranchRoot: filepath.Join(nodeARoot, "branches"), MountRoot: filepath.Join(nodeARoot, "mounts"),
		Source: conditional, Publisher: publisher, Runtime: nodeARuntime,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, nodeA.Close()) })
	_, err = nodeA.Ensure(t.Context(), first)
	require.NoError(t, err)
	nodeA.mu.Lock()
	nodeABranch := nodeA.live[first.Parent].branch
	nodeA.mu.Unlock()

	expected := bytes.Repeat([]byte{byte(len("rustfs-node-a") + 1)}, 16*rootfsblock.LogicalBlockSize)
	for block := range 13 {
		payload := bytes.Repeat([]byte{byte(0x80 + block)}, rootfsblock.LogicalBlockSize)
		_, err = nodeABranch.WriteAt(payload, int64(block*rootfsblock.LogicalBlockSize))
		require.NoError(t, err)
		copy(expected[block*rootfsblock.LogicalBlockSize:], payload)
	}
	nodeARecord, err := nodeA.load(first.Parent)
	require.NoError(t, err)
	operationID := rootfshandoff.PlannedRetireOperationID(
		first.Parent, first.Identity.WriterGrantID, first.Identity.WriterEpoch,
	)
	require.NoError(t, nodeA.BeginRetire(first.Parent, first.Identity, operationID))
	require.NoError(t, nodeA.Release(t.Context(), first.Identity))
	retired, err := nodeA.RetireResult(first.Parent, first.Identity, operationID)
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DurabilityS3, retired.DurabilityState,
		"the migration fixture must exercise an independently fetchable S3 head")
	require.NoError(t, nodeA.ReclaimTerminalArtifacts(first.Parent, first.Identity))
	_, err = os.Stat(nodeARecord.BranchPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	second := nextNodeStageRequest(first, retired)
	require.NoError(t, second.Validate())
	nodeBRoot := t.TempDir()
	nodeBRuntime := newFakeHostRuntime(memory)
	nodeB, err := New(Config{
		StatePath:  filepath.Join(nodeBRoot, "state", "sessions.db"),
		BranchRoot: filepath.Join(nodeBRoot, "branches"), MountRoot: filepath.Join(nodeBRoot, "mounts"),
		Source: conditional, Publisher: publisher, Runtime: nodeBRuntime,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, nodeB.Close()) })
	_, err = nodeB.Ensure(t.Context(), second)
	require.NoError(t, err)
	nodeB.mu.Lock()
	nodeBBranch := nodeB.live[second.Parent].branch
	nodeB.mu.Unlock()

	actual := make([]byte, len(expected))
	_, err = nodeBBranch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	nodeBRecord, err := nodeB.load(second.Parent)
	require.NoError(t, err)
	require.NotEqual(t, nodeARecord.BranchPath, nodeBRecord.BranchPath)
	require.Equal(t, int64(1), nodeARecord.WriterEpoch)
	require.Equal(t, int64(2), nodeBRecord.WriterEpoch)
	require.Equal(t, "node-b", nodeBRecord.Stage.Identity.NodeUID)
	require.FileExists(t, nodeBRecord.BranchPath)
}

func newRustFSIntegrationStore(
	t *testing.T,
	endpoint string,
	observer objectstore.RequestObserver,
) objectstore.ContextConditionalStore {
	t.Helper()
	bucket := "s0-rootfs-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	store, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey:       os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"),
		SecretKey:       os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY"),
		RequestObserver: observer,
	})
	require.NoError(t, err)
	require.NoError(t, store.Create())
	conditional, ok := store.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	require.True(t, objectstore.SupportsContextConditionalCreate(store))
	return conditional
}

func nextNodeStageRequest(
	previous rootfshandoff.StageRequest,
	retired RetireResult,
) rootfshandoff.StageRequest {
	next := previous
	next.Parent = digest.FromString("parent-rustfs-node-b").String()
	next.InitialGeneration = "generation-rustfs-node-b"
	next.Identity.NodeUID = "node-b"
	next.Identity.BootID = "boot-b"
	next.Identity.RuntimeGeneration = "runtime-b"
	next.Identity.PodUID = "pod-rustfs-node-b"
	next.Identity.PodSandboxID = "sandbox-rustfs-node-b"
	next.Identity.SlotNonce = "slot-rustfs-node-b"
	next.Identity.ClaimID = "claim-rustfs-node-b"
	next.Identity.LaunchAttempt = "attempt-rustfs-node-b"
	next.Identity.WriterEpoch = retired.WriterEpoch + 1
	next.Identity.WriterGrantID = "grant-rustfs-node-b"
	next.Identity.WriterGrantToken = "token-rustfs-node-b"
	next.Identity.WriterGrantTokenDigest = rootfshandoff.WriterGrantTokenDigest(next.Identity.WriterGrantToken)
	next.ExpectedPolicyToken.PodUID = next.Identity.PodUID
	next.ExpectedPolicyToken.PodSandboxID = next.Identity.PodSandboxID
	next.ExpectedPolicyToken.ClaimID = next.Identity.ClaimID
	next.ExpectedPolicyToken.NetworkEpoch = next.Identity.WriterEpoch
	next.ExpectedPolicyToken.PolicyDigest = "policy-rustfs-node-b"
	next.ExpectedPolicyToken.NetNSIdentity = "netns-rustfs-node-b"
	generation := *previous.Generation
	generation.GenerationID = next.InitialGeneration
	generation.CurrentBlockHead = retired.CurrentBlockHead
	generation.WriterEpoch = retired.WriterEpoch
	generation.DurabilityState = retired.DurabilityState
	generation.LocatorVersion++
	generation.Descriptor = append([]byte(nil), retired.Descriptor...)
	next.Generation = &generation
	return next
}

type recordingRequestObserver struct {
	mu       sync.Mutex
	attempts []objectstore.RequestAttempt
}

func (r *recordingRequestObserver) ObserveRequestAttempt(attempt objectstore.RequestAttempt) {
	r.mu.Lock()
	r.attempts = append(r.attempts, attempt)
	r.mu.Unlock()
}

func (r *recordingRequestObserver) reset() {
	r.mu.Lock()
	r.attempts = nil
	r.mu.Unlock()
}

func (r *recordingRequestObserver) take() []objectstore.RequestAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	attempts := append([]objectstore.RequestAttempt(nil), r.attempts...)
	r.attempts = nil
	return attempts
}

func assertObjectRequestBudget(
	t *testing.T,
	attempts []objectstore.RequestAttempt,
	maxGets int,
	maxPuts int,
	maxMetadata int,
) {
	t.Helper()
	gets, puts, metadata := 0, 0, 0
	for _, attempt := range attempts {
		switch attempt.Operation {
		case "GetObject":
			gets++
		case "PutObject":
			puts++
		case "HeadBucket", "HeadObject", "ListObjects", "ListObjectsV2":
			metadata++
		default:
			t.Fatalf("unexpected object-storage operation in RootFS data path: %+v", attempt)
		}
	}
	require.LessOrEqual(t, gets, maxGets, "GET request budget")
	require.LessOrEqual(t, puts, maxPuts, "PUT request budget")
	require.LessOrEqual(t, metadata, maxMetadata, "metadata request budget")
}
