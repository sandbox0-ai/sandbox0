package rootfs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControllerInspectRootFS(t *testing.T) {
	runtime := &fakeRuntime{info: rootFSInfo()}
	controller := NewController(Config{Runtime: runtime})

	resp, status := controller.InspectRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.InspectRootFSRequest{
		Target: rootFSTarget(),
	})

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "container-1", resp.Info.ContainerID)
	require.Len(t, runtime.inspectTargets, 1)
	assert.Equal(t, rootFSTarget(), runtime.inspectTargets[0])
}

func TestControllerSaveRootFSCommitsS0FSHead(t *testing.T) {
	parentHead := rootFSS0FSHead()
	childHead := parentHead
	childHead.ManifestKey = "manifests/00000000000000000002.json"
	childHead.ManifestSeq = 2
	runtime := &fakeRuntime{info: rootFSInfo(), commitHead: childHead}
	store := objectstore.NewMemoryStore("controller-save-s0fs")
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.SaveRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.SaveRootFSRequest{
		Target:       rootFSTarget(),
		SandboxID:    "sandbox-1",
		TeamID:       "team-1",
		FilesystemID: "fs-1",
		ParentHead:   parentHead,
		ExcludedPaths: []string{
			"/workspace/cache",
		},
	})

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, childHead, resp.Head)
	assert.Equal(t, "sandbox-1", runtime.commitReq.SandboxID)
	assert.Equal(t, "team-1", runtime.commitReq.TeamID)
	assert.Equal(t, "fs-1", runtime.commitReq.FilesystemID)
	assert.Equal(t, parentHead, runtime.commitReq.ParentHead)
	assert.Equal(t, []string{"/workspace/cache"}, runtime.commitReq.ExcludedPaths)
	assert.Same(t, store, runtime.commitReq.Store)
}

func TestControllerSaveRootFSUsesResolvedPortalPaths(t *testing.T) {
	head := rootFSS0FSHead()
	runtime := &fakeRuntime{info: rootFSInfo(), commitHead: head}
	controller := NewController(Config{
		Runtime: runtime,
		Store:   objectstore.NewMemoryStore("controller-save-resolved-portals"),
		PortalResolver: fakePortalResolver{paths: []ctldapi.RootFSPortalPath{{
			PortalName:  "workspace",
			MountPath:   "/workspace",
			BackingPath: "/var/lib/sandbox0/portals/pod-uid/workspace",
		}}},
	})

	resp, status := controller.SaveRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.SaveRootFSRequest{
		Target: rootFSTarget(),
		PortalPaths: []ctldapi.RootFSPortalPath{{
			MountPath:   "/workspace",
			BackingPath: "/stale/request/path",
		}},
	})

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, head, resp.Head)
	require.Len(t, runtime.commitReq.PortalPaths, 1)
	assert.Equal(t, "/var/lib/sandbox0/portals/pod-uid/workspace", runtime.commitReq.PortalPaths[0].BackingPath)
}

func TestControllerApplyRootFSAttachesS0FSHead(t *testing.T) {
	head := rootFSS0FSHead()
	runtime := &fakeRuntime{
		info:       rootFSInfo(),
		attachHead: head,
		mountPath:  "/sandbox0/rootfs",
	}
	store := objectstore.NewMemoryStore("controller-apply-s0fs")
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:                 rootFSTarget(),
		TeamID:                 "team-1",
		FilesystemID:           "fs-1",
		ExpectedRuntime:        "runc",
		ExpectedRuntimeHandler: "io.containerd.runc.v2",
		ExpectedSnapshotter:    "overlayfs",
		Head:                   head,
		ExcludedPaths:          []string{"/workspace/cache"},
	})

	require.Equal(t, http.StatusOK, status)
	assert.True(t, resp.Applied)
	assert.Equal(t, head, resp.Head)
	assert.Equal(t, "/sandbox0/rootfs", resp.MountPath)
	assert.Equal(t, head, runtime.attachReq.Head)
	assert.Equal(t, "fs-1", runtime.attachReq.FilesystemID)
	assert.Equal(t, []string{"/workspace/cache"}, runtime.attachReq.ExcludedPaths)
	assert.Same(t, store, runtime.attachReq.Store)
}

func TestControllerApplyRootFSRejectsMissingHead(t *testing.T) {
	controller := NewController(Config{
		Runtime: &fakeRuntime{info: rootFSInfo()},
		Store:   objectstore.NewMemoryStore("controller-apply-missing-head"),
	})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target: rootFSTarget(),
	})

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, resp.Error, "rootfs head volume_id is required")
}

func TestControllerApplyRootFSValidatesRuntimeIdentity(t *testing.T) {
	controller := NewController(Config{
		Runtime: &fakeRuntime{info: rootFSInfo()},
		Store:   objectstore.NewMemoryStore("controller-apply-runtime-mismatch"),
	})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:          rootFSTarget(),
		ExpectedRuntime: "gvisor",
		Head:            rootFSS0FSHead(),
	})

	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, resp.Error, "runtime mismatch")
}

func TestControllerApplyRootFSWeaklyValidatesBaseIdentity(t *testing.T) {
	head := rootFSS0FSHead()
	runtime := &fakeRuntime{
		info:       rootFSInfo(),
		attachHead: head,
		mountPath:  "/sandbox0/rootfs",
	}
	controller := NewController(Config{
		Runtime: runtime,
		Store:   objectstore.NewMemoryStore("controller-apply-base-warning"),
	})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTarget(),
		ExpectedRuntime:             "runc",
		ExpectedRuntimeHandler:      "io.containerd.runc.v2",
		ExpectedSnapshotter:         "overlayfs",
		ExpectedBaseImageDigest:     "sha256:new-base",
		ExpectedSnapshotParent:      "new-parent",
		ExpectedSnapshotParentChain: []string{"new-parent"},
		Head:                        head,
	})

	require.Equal(t, http.StatusOK, status)
	assert.True(t, resp.Applied)
	assert.Len(t, resp.Warnings, 3)
	assert.Contains(t, resp.Warnings[0], "base image digest mismatch")
	assert.Equal(t, head, runtime.attachReq.Head)
}

func TestControllerSaveRootFSMapsRuntimeErrors(t *testing.T) {
	runtime := &fakeRuntime{
		info:      rootFSInfo(),
		commitErr: ErrConflict,
	}
	controller := NewController(Config{
		Runtime: runtime,
		Store:   objectstore.NewMemoryStore("controller-save-error"),
	})

	resp, status := controller.SaveRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.SaveRootFSRequest{
		Target:    rootFSTarget(),
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	})

	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, resp.Error, "commit s0fs rootfs")
}

func TestControllerSaveRootFSRequiresObjectStore(t *testing.T) {
	controller := NewController(Config{Runtime: &fakeRuntime{info: rootFSInfo()}})

	resp, status := controller.SaveRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.SaveRootFSRequest{
		Target:    rootFSTarget(),
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	})

	assert.Equal(t, http.StatusNotImplemented, status)
	assert.Contains(t, resp.Error, "object store")
}

func TestValidateRootFSHeadDescriptorRejectsUnsupportedEngine(t *testing.T) {
	err := validateRootFSHeadDescriptor(ctldapi.RootFSHeadDescriptor{
		Engine:      "other",
		VolumeID:    "fs-1",
		ManifestKey: "manifests/00000000000000000001.json",
		ManifestSeq: 1,
	})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBadRequest))
}

type fakeRuntime struct {
	info           ctldapi.RootFSInfo
	inspectErr     error
	inspectTargets []ctldapi.RootFSContainerRef

	commitReq  S0FSCommitRequest
	commitHead ctldapi.RootFSHeadDescriptor
	commitErr  error

	attachReq  S0FSAttachRequest
	attachHead ctldapi.RootFSHeadDescriptor
	mountPath  string
	attachErr  error
}

func (r *fakeRuntime) Inspect(_ context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	r.inspectTargets = append(r.inspectTargets, target)
	if r.inspectErr != nil {
		return ctldapi.RootFSInfo{}, r.inspectErr
	}
	return r.info, nil
}

func (r *fakeRuntime) CommitS0FSRootFS(_ context.Context, req S0FSCommitRequest) (ctldapi.RootFSHeadDescriptor, error) {
	r.commitReq = req
	if r.commitErr != nil {
		return ctldapi.RootFSHeadDescriptor{}, r.commitErr
	}
	return r.commitHead, nil
}

func (r *fakeRuntime) AttachS0FSRootFS(_ context.Context, req S0FSAttachRequest) (ctldapi.RootFSHeadDescriptor, string, error) {
	r.attachReq = req
	if r.attachErr != nil {
		return ctldapi.RootFSHeadDescriptor{}, "", r.attachErr
	}
	return r.attachHead, r.mountPath, nil
}

func rootFSTarget() ctldapi.RootFSContainerRef {
	return ctldapi.RootFSContainerRef{
		Namespace:     "default",
		PodName:       "pod-1",
		PodUID:        "pod-uid",
		ContainerName: "sandbox",
	}
}

func rootFSInfo() ctldapi.RootFSInfo {
	return ctldapi.RootFSInfo{
		ContainerID:         "container-1",
		ContainerName:       "sandbox",
		PodNamespace:        "default",
		PodName:             "pod-1",
		PodUID:              "pod-uid",
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		Snapshotter:         "overlayfs",
		SnapshotKey:         "snapshot-1",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
	}
}

func rootFSS0FSHead() ctldapi.RootFSHeadDescriptor {
	return ctldapi.RootFSHeadDescriptor{
		Engine:        ctldapi.RootFSStorageEngineS0FS,
		TeamID:        "team-1",
		FilesystemID:  "fs-1",
		VolumeID:      "fs-1",
		ManifestKey:   "manifests/00000000000000000001.json",
		ManifestSeq:   1,
		CheckpointSeq: 1,
	}
}

type fakePortalResolver struct {
	paths []ctldapi.RootFSPortalPath
}

func (r fakePortalResolver) RootFSPortalPaths(string) []ctldapi.RootFSPortalPath {
	return append([]ctldapi.RootFSPortalPath(nil), r.paths...)
}
