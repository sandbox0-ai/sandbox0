package rootfs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	godigest "github.com/opencontainers/go-digest"
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

func TestControllerPrepareAndPublishRootFSUploadsDiffWithDefaultObjectKey(t *testing.T) {
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
	controller := NewController(Config{Runtime: runtime, Store: store, SnapshotDir: t.TempDir()})

	resp := prepareAndPublishRootFSTest(
		t,
		controller,
		ctldapi.PrepareRootFSSnapshotRequest{Target: rootFSTarget()},
		ctldapi.PublishRootFSSnapshotRequest{
			SandboxID:                 "sandbox-1",
			TeamID:                    "team-1",
			ExpectedRuntimeGeneration: 7,
		},
	)
	assert.Equal(t, "sandbox-rootfs/team-1/sandbox-1/7/sha256/feedface.tar", resp.Descriptor.ObjectKey)
	assert.Equal(t, rootFSInfo("gvisor"), runtime.createInfo)
	assert.Empty(t, runtime.createExcludedPaths)
	reader, err := store.Get(resp.Descriptor.ObjectKey, 0, -1)
	require.NoError(t, err)
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "rootfs diff", string(payload))
}

func TestControllerPrepareAndPublishRootFSUsesParentBaseline(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	runtime := &fakeRuntime{
		info: rootFSInfo("runc"),
		createBaselineDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:child",
			Size:      int64(len("child diff")),
		},
		createBaselineContent: "child diff",
	}
	controller := NewController(Config{Runtime: runtime, Store: store, SnapshotDir: t.TempDir()})

	resp := prepareAndPublishRootFSTest(
		t,
		controller,
		ctldapi.PrepareRootFSSnapshotRequest{
			Target:        rootFSTarget(),
			ParentLayerID: "layer-parent",
			ExcludedPaths: []string{"/workspace/data"},
		},
		ctldapi.PublishRootFSSnapshotRequest{
			SandboxID: "sandbox-1",
			TeamID:    "team-1",
		},
	)
	assert.True(t, runtime.createBaselineCalled)
	assert.False(t, runtime.createCalled)
	assert.Equal(t, "layer-parent", runtime.createBaselineLayerID)
	assert.Equal(t, []string{"/workspace/data"}, runtime.createBaselineExcludedPaths)
	assert.Equal(t, "sandbox-rootfs/team-1/sandbox-1/0/sha256/child.tar", resp.Descriptor.ObjectKey)
	reader, err := store.Get(resp.Descriptor.ObjectKey, 0, -1)
	require.NoError(t, err)
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "child diff", string(payload))
}

func TestControllerPrepareRootFSResolvesUnboundPortalPaths(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	runtime := &fakeRuntime{
		info: rootFSInfo("runc"),
		createDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
		},
		createContent: "rootfs diff",
	}
	controller := NewController(Config{
		Runtime:     runtime,
		Store:       store,
		SnapshotDir: t.TempDir(),
		PortalResolver: fakePortalResolver{paths: []ctldapi.RootFSPortalPath{
			{PortalName: "cache", MountPath: "/workspace/cache", BackingPath: "/tmp/cache"},
			{PortalName: "data", MountPath: "/workspace/data", BackingPath: "/tmp/data"},
		}},
	})

	resp, status := controller.PrepareRootFSSnapshot(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.PrepareRootFSSnapshotRequest{
		Target:        rootFSTarget(),
		StageID:       uuid.NewString(),
		TeamID:        "team-1",
		SandboxID:     "sandbox-1",
		ExpiresAt:     time.Now().Add(time.Minute),
		ExcludedPaths: []string{"/workspace/data"},
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.Equal(t, []ctldapi.RootFSPortalPath{{
		PortalName:  "cache",
		MountPath:   "/workspace/cache",
		BackingPath: "/tmp/cache",
	}}, runtime.createPortalPaths)
}

func TestControllerPublishRootFSWarmsObjectCache(t *testing.T) {
	payload := "rootfs diff"
	store := objectstore.NewMemoryStore(t.Name())
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})
	runtime := &fakeRuntime{
		info:          rootFSInfo("runc"),
		createDesc:    rootFSDiffDescriptorForPayload("", payload),
		createContent: payload,
	}
	controller := NewController(Config{Runtime: runtime, Store: store, ObjectCache: cache, SnapshotDir: t.TempDir()})

	resp := prepareAndPublishRootFSTest(
		t,
		controller,
		ctldapi.PrepareRootFSSnapshotRequest{Target: rootFSTarget()},
		ctldapi.PublishRootFSSnapshotRequest{SandboxID: "sandbox-1", TeamID: "team-1"},
	)
	reader, ok, err := cache.Open(resp.Descriptor)
	require.NoError(t, err)
	require.True(t, ok)
	defer reader.Close()
	cached, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, payload, string(cached))
}

func TestControllerPrepareRootFSRejectsUnsupportedRuntime(t *testing.T) {
	runtime := &fakeRuntime{
		info:          rootFSInfo("kata"),
		createContent: "rootfs diff",
	}
	controller := NewController(Config{Runtime: runtime, Store: objectstore.NewMemoryStore(t.Name()), SnapshotDir: t.TempDir()})

	resp, status := controller.PrepareRootFSSnapshot(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.PrepareRootFSSnapshotRequest{
		Target:    rootFSTarget(),
		StageID:   uuid.NewString(),
		TeamID:    "team-1",
		SandboxID: "sandbox-1",
		ExpiresAt: time.Now().Add(time.Minute),
	})

	require.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, resp.Error, "runtime \"kata\" is not supported")
	assert.False(t, runtime.createCalled)
}

func prepareAndPublishRootFSTest(
	t *testing.T,
	controller *Controller,
	prepareReq ctldapi.PrepareRootFSSnapshotRequest,
	publishReq ctldapi.PublishRootFSSnapshotRequest,
) ctldapi.PublishRootFSSnapshotResponse {
	t.Helper()
	if prepareReq.StageID == "" {
		prepareReq.StageID = uuid.NewString()
	}
	if prepareReq.TeamID == "" {
		prepareReq.TeamID = publishReq.TeamID
	}
	if prepareReq.SandboxID == "" {
		prepareReq.SandboxID = publishReq.SandboxID
	}
	prepareReq.ExpectedRuntimeGeneration = publishReq.ExpectedRuntimeGeneration
	if prepareReq.ExpiresAt.IsZero() {
		prepareReq.ExpiresAt = time.Now().Add(time.Minute)
	}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	prepared, status := controller.PrepareRootFSSnapshot(request, prepareReq)
	require.Equal(t, http.StatusOK, status, prepared.Error)
	require.NotEmpty(t, prepared.Handle)
	publishReq.Handle = prepared.Handle
	published, status := controller.PublishRootFSSnapshot(request, publishReq)
	require.Equal(t, http.StatusOK, status, published.Error)
	require.True(t, published.Published)
	return published
}

func TestControllerApplyRootFSUsesObjectCache(t *testing.T) {
	payload := "cached rootfs diff"
	desc := rootFSDiffDescriptorForPayload("rootfs/diff.tar", payload)
	cache := NewObjectCache(ObjectCacheConfig{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, cache.Put(context.Background(), desc, strings.NewReader(payload)))
	runtime := &fakeRuntime{
		info:      rootFSInfo("runc"),
		applyDesc: desc,
	}
	controller := NewController(Config{
		Runtime:     runtime,
		Store:       objectstore.NewMemoryStore(t.Name()),
		ObjectCache: cache,
	})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:     rootFSTarget(),
		Descriptor: desc,
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.True(t, resp.Applied)
	assert.Equal(t, payload, runtime.applyContent)
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
		TeamID:                      "team-1",
		SandboxID:                   "sandbox-1",
		ExpectedRuntime:             "runc",
		ExpectedRuntimeHandler:      "runc",
		ExpectedSnapshotter:         "overlayfs",
		ExpectedBaseImageDigest:     "sha256:base",
		ExpectedSnapshotParent:      "parent-1",
		ExpectedSnapshotParentChain: []string{"parent-1", "parent-0"},
		ExcludedPaths:               []string{"/workspace/data"},
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
	assert.Equal(t, []string{"/workspace/data"}, runtime.applyExcludedPaths)
}

func TestControllerApplyRootFSAppliesLayerChainAndCapturesBaseline(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, store.Put("rootfs/parent.tar", strings.NewReader("parent diff")))
	require.NoError(t, store.Put("rootfs/child.tar", strings.NewReader("child diff")))
	runtime := &fakeRuntime{
		info: rootFSInfo("runc"),
		applyDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:applied",
			Size:      int64(len("applied")),
		},
	}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTarget(),
		TeamID:                      "team-1",
		SandboxID:                   "sandbox-1",
		ExpectedRuntime:             "runc",
		ExpectedRuntimeHandler:      "runc",
		ExpectedSnapshotter:         "overlayfs",
		ExpectedBaseImageDigest:     "sha256:base",
		ExpectedSnapshotParent:      "parent-1",
		ExpectedSnapshotParentChain: []string{"parent-1", "parent-0"},
		BaselineLayerID:             "layer-child",
		ExcludedPaths:               []string{"/workspace/data"},
		Layers: []ctldapi.RootFSLayerDescriptor{
			{
				LayerID: "layer-parent",
				Descriptor: ctldapi.RootFSDiffDescriptor{
					MediaType: "application/vnd.oci.image.layer.v1.tar",
					Digest:    "sha256:parent",
					Size:      int64(len("parent diff")),
					ObjectKey: "rootfs/parent.tar",
				},
			},
			{
				LayerID:       "layer-child",
				ParentLayerID: "layer-parent",
				Descriptor: ctldapi.RootFSDiffDescriptor{
					MediaType: "application/vnd.oci.image.layer.v1.tar",
					Digest:    "sha256:child",
					Size:      int64(len("child diff")),
					ObjectKey: "rootfs/child.tar",
				},
			},
		},
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.True(t, resp.Applied)
	require.Len(t, resp.Layers, 2)
	assert.Equal(t, "rootfs/parent.tar", resp.Layers[0].Descriptor.ObjectKey)
	assert.Equal(t, "rootfs/child.tar", resp.Layers[1].Descriptor.ObjectKey)
	assert.Equal(t, []string{"parent diff", "child diff"}, runtime.applyContents)
	require.Len(t, runtime.applyInputDescs, 2)
	assert.Equal(t, "rootfs/parent.tar", runtime.applyInputDescs[0].ObjectKey)
	assert.Equal(t, "rootfs/child.tar", runtime.applyInputDescs[1].ObjectKey)
	assert.Equal(t, [][]string{{"/workspace/data"}, {"/workspace/data"}}, runtime.applyExcludedPathCalls)
	assert.True(t, runtime.captureBaselineCalled)
	assert.Equal(t, "layer-child", runtime.captureBaselineLayerID)
	assert.Equal(t, []string{"/workspace/data"}, runtime.captureBaselineExcludedPaths)
	assert.Equal(t, rootFSInfo("runc"), runtime.captureInfo)
}

func TestControllerApplyRootFSForceAppliesBaseMismatch(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, store.Put("rootfs/diff.tar", strings.NewReader("rootfs diff")))
	runtime := &fakeRuntime{
		info: rootFSInfo("gvisor"),
		applyDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
		},
	}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:                      rootFSTarget(),
		TeamID:                      "team-1",
		SandboxID:                   "sandbox-1",
		ExpectedBaseImageDigest:     "sha256:other-base",
		ExpectedSnapshotParent:      "other-parent",
		ExpectedSnapshotParentChain: []string{"other-parent"},
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
			ObjectKey: "rootfs/diff.tar",
		},
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.True(t, resp.Applied)
	assert.True(t, runtime.applyCalled)
	assert.Equal(t, "rootfs diff", runtime.applyContent)
}

func TestControllerApplyRootFSLayerChainReplaysBaseMismatch(t *testing.T) {
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
		TeamID:                      "team-1",
		SandboxID:                   "sandbox-1",
		ExpectedBaseImageDigest:     "sha256:other-base",
		ExpectedSnapshotParent:      "other-parent",
		ExpectedSnapshotParentChain: []string{"other-parent"},
		BaselineLayerID:             "layer-1",
		Layers: []ctldapi.RootFSLayerDescriptor{{
			LayerID: "layer-1",
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:feedface",
				Size:      int64(len("rootfs diff")),
				ObjectKey: "rootfs/diff.tar",
			},
		}},
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.True(t, resp.Applied)
	assert.True(t, runtime.applyCalled)
	assert.Equal(t, "rootfs diff", runtime.applyContent)
	assert.True(t, runtime.captureBaselineCalled)
	assert.Equal(t, "layer-1", runtime.captureBaselineLayerID)
}

func TestControllerApplyRootFSDoesNotFailWhenDisposableBaselineCaptureFails(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, store.Put("rootfs/diff.tar", strings.NewReader("rootfs diff")))
	runtime := &fakeRuntime{
		info:               rootFSInfo("runc"),
		captureBaselineErr: errors.New("baseline cache is full"),
		applyDesc: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
		},
	}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:          rootFSTarget(),
		TeamID:          "team-1",
		SandboxID:       "sandbox-1",
		BaselineLayerID: "layer-1",
		Layers: []ctldapi.RootFSLayerDescriptor{{
			LayerID: "layer-1",
			Descriptor: ctldapi.RootFSDiffDescriptor{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Digest:    "sha256:feedface",
				Size:      int64(len("rootfs diff")),
				ObjectKey: "rootfs/diff.tar",
			},
		}},
	})

	require.Equal(t, http.StatusOK, status, resp.Error)
	assert.True(t, resp.Applied)
	assert.True(t, runtime.applyCalled)
	assert.True(t, runtime.captureBaselineCalled)
}

func TestControllerApplyRootFSRejectsRuntimeMismatch(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	require.NoError(t, store.Put("rootfs/diff.tar", strings.NewReader("rootfs diff")))
	runtime := &fakeRuntime{info: rootFSInfo("gvisor")}
	controller := NewController(Config{Runtime: runtime, Store: store})

	resp, status := controller.ApplyRootFS(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ApplyRootFSRequest{
		Target:          rootFSTarget(),
		ExpectedRuntime: "runc",
		Descriptor: ctldapi.RootFSDiffDescriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    "sha256:feedface",
			Size:      int64(len("rootfs diff")),
			ObjectKey: "rootfs/diff.tar",
		},
	})

	require.Equal(t, http.StatusConflict, status)
	assert.Contains(t, resp.Error, "runtime mismatch")
	assert.False(t, runtime.applyCalled)
}

func TestValidateExpectedBaseRejectsRuntimeCompatibilityMismatches(t *testing.T) {
	info := rootFSInfo("gvisor")
	info.RuntimeHandler = "gvisor-rootfs"

	tests := []struct {
		name string
		req  ctldapi.ApplyRootFSRequest
		want string
	}{
		{
			name: "runtime",
			req:  ctldapi.ApplyRootFSRequest{ExpectedRuntime: "runc"},
			want: "runtime mismatch",
		},
		{
			name: "runtime handler",
			req:  ctldapi.ApplyRootFSRequest{ExpectedRuntimeHandler: "runsc-default"},
			want: "runtime handler mismatch",
		},
		{
			name: "snapshotter",
			req:  ctldapi.ApplyRootFSRequest{ExpectedSnapshotter: "devmapper"},
			want: "snapshotter mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExpectedBase(info, tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			assert.True(t, errors.Is(err, ErrConflict))
		})
	}
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
	info                  ctldapi.RootFSInfo
	inspectErr            error
	createDesc            ctldapi.RootFSDiffDescriptor
	createContent         string
	createErr             error
	createBaselineDesc    ctldapi.RootFSDiffDescriptor
	createBaselineContent string
	createBaselineErr     error
	applyDesc             ctldapi.RootFSDiffDescriptor
	applyErr              error
	captureBaselineErr    error

	inspectTargets               []ctldapi.RootFSContainerRef
	createCalled                 bool
	createInfo                   ctldapi.RootFSInfo
	createBaselineCalled         bool
	createBaselineInfo           ctldapi.RootFSInfo
	createBaselineLayerID        string
	createExcludedPaths          []string
	createBaselineExcludedPaths  []string
	createPortalPaths            []ctldapi.RootFSPortalPath
	createBaselinePortalPaths    []ctldapi.RootFSPortalPath
	applyCalled                  bool
	applyInfo                    ctldapi.RootFSInfo
	applyInputDesc               ctldapi.RootFSDiffDescriptor
	applyContent                 string
	applyInputDescs              []ctldapi.RootFSDiffDescriptor
	applyContents                []string
	applyExcludedPaths           []string
	applyExcludedPathCalls       [][]string
	applyPortalPaths             []ctldapi.RootFSPortalPath
	applyPortalPathCalls         [][]ctldapi.RootFSPortalPath
	captureBaselineCalled        bool
	captureInfo                  ctldapi.RootFSInfo
	captureBaselineLayerID       string
	captureBaselineExcludedPaths []string
	captureBaselinePortalPaths   []ctldapi.RootFSPortalPath
}

func (r *fakeRuntime) Inspect(_ context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	r.inspectTargets = append(r.inspectTargets, target)
	return r.info, r.inspectErr
}

func (r *fakeRuntime) CreateDiff(_ context.Context, info ctldapi.RootFSInfo, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	r.createCalled = true
	r.createInfo = info
	r.createExcludedPaths = append([]string(nil), excludedPaths...)
	r.createPortalPaths = append([]ctldapi.RootFSPortalPath(nil), portalPaths...)
	if r.createErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, r.createErr
	}
	return r.createDesc, readSeekNopCloser{Reader: strings.NewReader(r.createContent)}, nil
}

func (r *fakeRuntime) CreateDiffFromBaseline(_ context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	r.createBaselineCalled = true
	r.createBaselineInfo = info
	r.createBaselineLayerID = baselineLayerID
	r.createBaselineExcludedPaths = append([]string(nil), excludedPaths...)
	r.createBaselinePortalPaths = append([]ctldapi.RootFSPortalPath(nil), portalPaths...)
	if r.createBaselineErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, nil, r.createBaselineErr
	}
	return r.createBaselineDesc, readSeekNopCloser{Reader: strings.NewReader(r.createBaselineContent)}, nil
}

func (r *fakeRuntime) ApplyDiff(_ context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, content io.Reader, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, error) {
	r.applyCalled = true
	r.applyInfo = info
	r.applyInputDesc = desc
	r.applyInputDescs = append(r.applyInputDescs, desc)
	r.applyExcludedPaths = append([]string(nil), excludedPaths...)
	r.applyExcludedPathCalls = append(r.applyExcludedPathCalls, append([]string(nil), excludedPaths...))
	r.applyPortalPaths = append([]ctldapi.RootFSPortalPath(nil), portalPaths...)
	r.applyPortalPathCalls = append(r.applyPortalPathCalls, append([]ctldapi.RootFSPortalPath(nil), portalPaths...))
	payload, err := io.ReadAll(content)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, err
	}
	r.applyContent = string(payload)
	r.applyContents = append(r.applyContents, string(payload))
	if r.applyErr != nil {
		return ctldapi.RootFSDiffDescriptor{}, r.applyErr
	}
	return r.applyDesc, nil
}

func (r *fakeRuntime) CaptureBaseline(_ context.Context, info ctldapi.RootFSInfo, _, _ string, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) error {
	r.captureBaselineCalled = true
	r.captureInfo = info
	r.captureBaselineLayerID = baselineLayerID
	r.captureBaselineExcludedPaths = append([]string(nil), excludedPaths...)
	r.captureBaselinePortalPaths = append([]ctldapi.RootFSPortalPath(nil), portalPaths...)
	return r.captureBaselineErr
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

func rootFSDiffDescriptorForPayload(objectKey, payload string) ctldapi.RootFSDiffDescriptor {
	return ctldapi.RootFSDiffDescriptor{
		MediaType: "application/vnd.oci.image.layer.v1.tar",
		Digest:    godigest.FromBytes([]byte(payload)).String(),
		Size:      int64(len(payload)),
		ObjectKey: objectKey,
	}
}

type readSeekNopCloser struct {
	*strings.Reader
}

func (readSeekNopCloser) Close() error {
	return nil
}

type fakePortalResolver struct {
	paths []ctldapi.RootFSPortalPath
}

func (r fakePortalResolver) RootFSPortalPaths(string) []ctldapi.RootFSPortalPath {
	return append([]ctldapi.RootFSPortalPath(nil), r.paths...)
}
