package rootfs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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
	assert.Empty(t, prepared.Info.BaseImageConfig, "base image config must stay inside ctld")
	require.NoError(t, prepared.Checkpoint.Reference.Validate())
	assert.Equal(t, "head-1", prepared.Checkpoint.Reference.HeadID)
	assert.NotEmpty(t, prepared.Checkpoint.Objects)
	require.NoError(t, prepared.Checkpoint.Image.Validate())
	assert.Contains(t, prepared.Checkpoint.Image.Name, "sandbox0.local/rootfs-heads@sha256:")
	assert.Positive(t, prepared.Checkpoint.CreatedObjectCount)
	controller.mu.Lock()
	_, sessionStillBound := controller.sessions[rootFSSessionKey(runtime.info)]
	controller.mu.Unlock()
	assert.False(t, sessionStillBound, "a sealed generation must not retain a closed watcher session")

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
	assert.Empty(t, published.Info.BaseImageConfig, "prepared metadata must not persist the base image config")
	assert.True(t, published.Published)
	assert.Equal(t, prepared.Checkpoint.Reference, published.Checkpoint.Reference)

	materialized, status := controller.MaterializeRootFSHead(nil, ctldapi.MaterializeRootFSHeadRequest{
		Head: prepared.Checkpoint.Reference, Image: prepared.Checkpoint.Image,
	})
	require.Equal(t, http.StatusOK, status, materialized.Error)
	assert.True(t, materialized.Materialized)
	assert.Equal(t, prepared.Checkpoint.Image.Name, materialized.Image)
	assert.Equal(t, prepared.Checkpoint.Reference, runtime.materializedHead)
	assert.Equal(t, runtime.info.SnapshotParent, runtime.materializedBase)

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

func TestPersistHeadImageWritesOnlyBoundedCASMetadataAndDeduplicatesRetry(t *testing.T) {
	base := objectstore.NewMemoryStore(t.Name())
	store := &countingPutStore{Store: base}
	controller := NewController(Config{Runtime: &fakeRuntime{}, Store: store})
	defer controller.Close()
	checkpoint := ctldapi.RootFSCheckpointDescriptor{
		Reference: rootfshead.HeadReference{
			Version: rootfshead.Version,
			HeadID:  "head-idempotent",
			Manifest: rootfshead.Object{
				Key:       "sandbox-rootfs/heads/sha256/head",
				Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Size:      64,
				MediaType: rootfshead.HeadMediaType,
			},
		},
	}

	first, err := controller.persistHeadImage(context.Background(), testRootFSInfo(), checkpoint)
	require.NoError(t, err)
	assert.Equal(t, 2, store.puts, "the OCI marker and envelope are the only S3 writes")
	assert.Equal(t, int64(2), first.CreatedObjectCount)
	assert.Less(t, first.CreatedBytes, int64(128<<10), "marker persistence must not copy base layers or rootfs data")
	require.Len(t, first.Objects, 2)
	assert.Equal(t, rootfshead.MarkerMediaType, first.Objects[0].MediaType)
	assert.Equal(t, rootfshead.ImageEnvelopeMediaType, first.Objects[1].MediaType)

	second, err := controller.persistHeadImage(context.Background(), testRootFSInfo(), first)
	require.NoError(t, err)
	assert.Equal(t, 2, store.puts, "a lifecycle retry must not rewrite an existing marker or envelope")
	assert.Equal(t, first.CreatedObjectCount, second.CreatedObjectCount)
	assert.Equal(t, first.CreatedBytes, second.CreatedBytes)
	assert.Equal(t, first.Objects, second.Objects)
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
	info             ctldapi.RootFSInfo
	upperdir         string
	err              error
	materializedHead rootfshead.HeadReference
	materializedBase string
}

type countingPutStore struct {
	objectstore.Store
	puts int
}

func (s *countingPutStore) Put(key string, reader io.Reader) error {
	s.puts++
	return s.Store.Put(key, reader)
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

func (r *fakeRuntime) MaterializeRootFSHead(_ context.Context, head rootfshead.HeadReference, _ rootfshead.ImageReference, _ rootfshead.ImageEnvelope, base string) error {
	r.materializedHead = head
	r.materializedBase = base
	return r.err
}

func testRootFSTarget() ctldapi.RootFSContainerRef {
	return ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", PodUID: "pod-uid", ContainerName: "procd"}
}

func testRootFSInfo() ctldapi.RootFSInfo {
	baseConfig, _ := json.Marshal(ocispec.Image{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		RootFS:   ocispec.RootFS{Type: "layers"},
	})
	return ctldapi.RootFSInfo{
		Runtime:         "runc",
		Snapshotter:     "sandbox0",
		SnapshotKey:     "active-snapshot",
		SnapshotParent:  "sha256:base-snapshot",
		BaseImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BaseImageConfig: baseConfig,
	}
}
