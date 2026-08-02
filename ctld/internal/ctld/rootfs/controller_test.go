package rootfs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerBindsContinuousSyncAndSealsMetadataHead(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(upper, "workspace"), 0o755))
	payload := bytes.Repeat([]byte("sandbox0"), 1<<15)
	require.NoError(t, os.WriteFile(filepath.Join(upper, "workspace", "state.bin"), payload, 0o640))

	store := objectstore.NewMemoryStore(t.Name())
	runtime := &fakeRuntime{upperdir: upper, info: testRootFSInfo()}
	controller := NewController(Config{Runtime: runtime, Store: store, SnapshotDir: t.TempDir()})
	defer controller.Close()
	target := testRootFSTarget()

	bound, status := controller.BindRootFSSync(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.BindRootFSSyncRequest{
		Target: target, SandboxID: "sandbox-1", TeamID: "team-1", FilesystemID: "filesystem-1",
	})
	require.Equal(t, http.StatusOK, status, bound.Error)
	assert.True(t, bound.Bound)

	prepared, status := controller.PrepareRootFSSnapshot(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.PrepareRootFSSnapshotRequest{
		Target: target, HeadID: "head-1", SandboxID: "sandbox-1", TeamID: "team-1", FilesystemID: "filesystem-1",
	})
	require.Equal(t, http.StatusOK, status, prepared.Error)
	require.NoError(t, prepared.Checkpoint.Reference.Validate())
	assert.Equal(t, "head-1", prepared.Checkpoint.Reference.HeadID)
	assert.NotEmpty(t, prepared.Checkpoint.Objects)
	assert.Positive(t, prepared.Checkpoint.CreatedObjectCount)

	headReader, err := store.Get(prepared.Checkpoint.Reference.Manifest.Key, 0, prepared.Checkpoint.Reference.Manifest.Size)
	require.NoError(t, err)
	head, err := rootfshead.DecodeHead(headReader)
	require.NoError(t, err)
	require.NoError(t, headReader.Close())
	assert.Equal(t, "head-1", head.HeadID)
	assert.Equal(t, runtime.info.BaseImageDigest, head.BaseImageDigest)
	assert.Equal(t, runtime.info.SnapshotParent, head.BaseSnapshotKey)

	published, status := controller.PublishRootFSSnapshot(nil, ctldapi.PublishRootFSSnapshotRequest{Handle: prepared.Handle})
	require.Equal(t, http.StatusOK, status, published.Error)
	assert.True(t, published.Published)
	assert.Equal(t, prepared.Checkpoint.Reference, published.Checkpoint.Reference)

	_, status = controller.PublishRootFSSnapshot(nil, ctldapi.PublishRootFSSnapshotRequest{Handle: prepared.Handle})
	assert.Equal(t, http.StatusNotFound, status)
}

func TestControllerPrepareRecoversMissingWatcherSession(t *testing.T) {
	upper := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(upper, "answer"), []byte("42"), 0o644))
	controller := NewController(Config{
		Runtime:     &fakeRuntime{upperdir: upper, info: testRootFSInfo()},
		Store:       objectstore.NewMemoryStore(t.Name()),
		SnapshotDir: t.TempDir(),
	})
	defer controller.Close()

	response, status := controller.PrepareRootFSSnapshot(nil, ctldapi.PrepareRootFSSnapshotRequest{
		Target: testRootFSTarget(), HeadID: "recovered", SandboxID: "sandbox-1", TeamID: "team-1", FilesystemID: "filesystem-1",
	})
	require.Equal(t, http.StatusOK, status, response.Error)
	assert.Equal(t, "recovered", response.Checkpoint.Reference.HeadID)
}

func TestControllerRejectsIncompleteSyncIdentity(t *testing.T) {
	controller := NewController(Config{Runtime: &fakeRuntime{}, Store: objectstore.NewMemoryStore(t.Name())})
	defer controller.Close()
	response, status := controller.BindRootFSSync(nil, ctldapi.BindRootFSSyncRequest{Target: testRootFSTarget()})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, response.Error, "sandbox_id is required")
}

func TestControllerBindRootFSSyncIsIdempotent(t *testing.T) {
	controller := NewController(Config{
		Runtime: &fakeRuntime{upperdir: t.TempDir(), info: testRootFSInfo()},
		Store:   objectstore.NewMemoryStore(t.Name()),
	})
	defer controller.Close()
	req := ctldapi.BindRootFSSyncRequest{
		Target:       testRootFSTarget(),
		SandboxID:    "sandbox-1",
		TeamID:       "team-1",
		FilesystemID: "filesystem-1",
	}

	first, status := controller.BindRootFSSync(nil, req)
	require.Equal(t, http.StatusOK, status, first.Error)
	key := rootFSSessionKey(testRootFSInfo())
	controller.mu.Lock()
	firstSession := controller.sessions[key].session
	controller.mu.Unlock()
	require.NotNil(t, firstSession)

	second, status := controller.BindRootFSSync(nil, req)
	require.Equal(t, http.StatusOK, status, second.Error)
	controller.mu.Lock()
	secondSession := controller.sessions[key].session
	controller.mu.Unlock()
	assert.Same(t, firstSession, secondSession)
}

func TestReadPreparedSnapshotRejectsTamperedCheckpoint(t *testing.T) {
	controller := NewController(Config{SnapshotDir: t.TempDir()})
	defer controller.Close()
	handle := "tampered"
	require.NoError(t, os.WriteFile(controller.preparedSnapshotMetaPath(handle), []byte(`{"handle":"tampered","checkpoint":{}}`), 0o600))
	_, err := controller.readPreparedSnapshot(handle)
	require.Error(t, err)
}

func TestControllerClosesSyncSessionAfterPodLeavesNode(t *testing.T) {
	upper := t.TempDir()
	controller := NewController(Config{
		Runtime:           &fakeRuntime{upperdir: upper, info: testRootFSInfo()},
		Store:             objectstore.NewMemoryStore(t.Name()),
		SnapshotDir:       t.TempDir(),
		SessionSweepEvery: 5 * time.Millisecond,
		ActivePodUIDs: func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{}, nil
		},
	})
	defer controller.Close()

	response, status := controller.BindRootFSSync(nil, ctldapi.BindRootFSSyncRequest{
		Target:       testRootFSTarget(),
		SandboxID:    "sandbox-1",
		TeamID:       "team-1",
		FilesystemID: "filesystem-1",
	})
	require.Equal(t, http.StatusOK, status, response.Error)
	require.Eventually(t, func() bool {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		return len(controller.sessions) == 0
	}, time.Second, 5*time.Millisecond)
}

type fakeRuntime struct {
	info     ctldapi.RootFSInfo
	upperdir string
	err      error
}

func (r *fakeRuntime) Inspect(context.Context, ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	if r.err != nil {
		return ctldapi.RootFSInfo{}, r.err
	}
	return r.info, nil
}

func (r *fakeRuntime) RootFSUpperdir(context.Context, ctldapi.RootFSInfo) (string, error) {
	return r.upperdir, r.err
}

func testRootFSTarget() ctldapi.RootFSContainerRef {
	return ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", PodUID: "pod-uid", ContainerName: "procd"}
}

func testRootFSInfo() ctldapi.RootFSInfo {
	return ctldapi.RootFSInfo{
		Runtime:         "runc",
		Snapshotter:     "sandbox0",
		SnapshotKey:     "active-snapshot",
		SnapshotParent:  "sha256:base-snapshot",
		BaseImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}

func readAllAndClose(t *testing.T, reader io.ReadCloser) []byte {
	t.Helper()
	payload, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return payload
}
