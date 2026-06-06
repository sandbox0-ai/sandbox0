package rootfs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerInspectRootFSDelegatesToRuntime(t *testing.T) {
	runtime := &fakeRuntime{info: rootFSInfo("runc")}
	controller := NewController(Config{Runtime: runtime})

	resp, status := controller.InspectRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.InspectRootFSRequest{
		Target: rootFSTarget(),
	})

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "runc", resp.Info.Runtime)
	require.Len(t, runtime.inspectTargets, 1)
	assert.Equal(t, rootFSTarget(), runtime.inspectTargets[0])
}

func TestControllerInspectRootFSRejectsInvalidTarget(t *testing.T) {
	controller := NewController(Config{Runtime: &fakeRuntime{}})

	resp, status := controller.InspectRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.InspectRootFSRequest{})

	require.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, resp.Error, "namespace is required")
}

func TestControllerSaveRootFSUploadsDiffWithDefaultObjectKey(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	runtime := &fakeRuntime{
		info: rootFSInfo("gvisor"),
		createDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
		},
		createContent: "rootfs diff",
	}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.SaveRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.SaveRootFSRequest{
		Target:                    rootFSTarget(),
		SandboxID:                 "sandbox-1",
		TeamID:                    "team-1",
		ExpectedRuntimeGeneration: 7,
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.Equal(t, "sandbox-rootfs/team-1/sandbox-1/7/sha256/feedface.tar", resp.Descriptor.ObjectKey)
	assert.Equal(t, rootFSInfo("gvisor"), runtime.createInfo)
	reader, err := store.Get(resp.Descriptor.ObjectKey, 0, -1)
	require.NoError(t, err)
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "rootfs diff", string(payload))
}

func TestControllerSaveRootFSRejectsUnsupportedRuntime(t *testing.T) {
	runtime := &fakeRuntime{
		info:          rootFSInfo("kata"),
		createContent: "rootfs diff",
	}
	controller := NewController(Config{Runtime: runtime, Store: objectstore.NewMemoryStore(t.Name())})

	resp, status := controller.SaveRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.SaveRootFSRequest{
		Target:    rootFSTarget(),
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	})

	require.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, resp.Error, "runtime \"kata\" is not supported")
	assert.False(t, runtime.createCalled)
}

func TestControllerApplyRootFSDownloadsAndAppliesDiff(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, store.Put("rootfs/diff.tar", strings.NewReader("rootfs diff")))
	runtime := &fakeRuntime{
		info: rootFSInfo("runc"),
		applyDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
		},
	}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTarget(),
		ExpectedBaseImageDigest:     "sha256:base",
		ExpectedSnapshotParent:      "parent-1",
		ExpectedSnapshotParentChain: []string{"parent-1", "parent-0"},
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
			ObjectKey: "rootfs/diff.tar",
		},
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.True(t, resp.Applied)
	assert.Equal(t, "rootfs/diff.tar", resp.Descriptor.ObjectKey)
	assert.Equal(t, "rootfs diff", runtime.applyContent)
	assert.Equal(t, rootFSInfo("runc"), runtime.applyInfo)
	assert.Equal(t, "rootfs/diff.tar", runtime.applyInputDesc.ObjectKey)
}

func TestControllerApplyRootFSRejectsBaseMismatch(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, store.Put("rootfs/diff.tar", strings.NewReader("rootfs diff")))
	runtime := &fakeRuntime{info: rootFSInfo("gvisor")}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:                  rootFSTarget(),
		ExpectedBaseImageDigest: "sha256:other-base",
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
			ObjectKey: "rootfs/diff.tar",
		},
	})

	require.Equal(t, http.StatusConflict, status)
	assert.Contains(t, resp.Error, "base image digest mismatch")
	assert.False(t, runtime.applyCalled)
}

func TestControllerApplyRootFSRejectsMissingDescriptorObjectKey(t *testing.T) {
	controller := NewController(Config{Runtime: &fakeRuntime{}, Store: objectstore.NewMemoryStore(t.Name())})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:     rootFSTarget(),
		Descriptor: ctldapi.RootFSDiffDescriptor{Digest: "sha256:feedface"},
	})

	require.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, resp.Error, "descriptor object_key is required")
}

type fakeRuntime struct {
	info          ctldapi.RootFSInfo
	inspectErr    error
	createDesc    ctldapi.RootFSDiffDescriptor
	createContent string
	createErr     error
	applyDesc     ctldapi.RootFSDiffDescriptor
	applyErr      error

	inspectTargets []ctldapi.RootFSContainerRef
	createCalled   bool
	createInfo     ctldapi.RootFSInfo
	applyCalled    bool
	applyInfo      ctldapi.RootFSInfo
	applyInputDesc ctldapi.RootFSDiffDescriptor
	applyContent   string
}

func (r *fakeRuntime) Inspect(_ context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	r.inspectTargets = append(r.inspectTargets, target)
	return r.info, r.inspectErr
}

func (r *fakeRuntime) CreateDiff(_ context.Context, info ctldapi.RootFSInfo) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	r.createCalled = true
	r.createInfo = info
	if r.createErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, r.createErr
	}
	return r.createDesc, readSeekNopCloser{Reader: strings.NewReader(r.createContent)}, nil
}

func (r *fakeRuntime) ApplyDiff(_ context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, content io.Reader) (ctldapi.RootFSDiffDescriptor, error) {
	r.applyCalled = true
	r.applyInfo = info
	r.applyInputDesc = desc
	payload, err := io.ReadAll(content)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}
	r.applyContent = string(payload)
	if r.applyErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, r.applyErr
	}
	return r.applyDesc, nil
}

func rootFSTarget() ctldapi.RootFSContainerRef {
	return ctldapi.RootFSContainerRef{
		Namespace:     "default",
		PodName:       "pod-1",
		PodUID:        "uid-1",
		ContainerName: "sandbox",
	}
}

func rootFSInfo(runtime string) ctldapi.RootFSInfo {
	return ctldapi.RootFSInfo{
		ContainerID:         "container-1",
		ContainerName:       "sandbox",
		PodNamespace:        "default",
		PodName:             "pod-1",
		PodUID:              "uid-1",
		Runtime:             runtime,
		RuntimeHandler:      runtime,
		Snapshotter:         "overlayfs",
		SnapshotKey:         "snapshot-1",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
	}
}

type readSeekNopCloser struct {
	*strings.Reader
}

func (readSeekNopCloser) Close() error {
	return nil
}
