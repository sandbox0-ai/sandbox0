package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtldHealthEndpoints(t *testing.T) {
	server := newHTTPServer(":0", nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())

	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
}

func TestCtldPauseResumeStubsReturnNotImplemented(t *testing.T) {
	server := newHTTPServer(":0", nil)

	t.Run("custom controller", func(t *testing.T) {
		server := newHTTPServer(":0", ctldserver.NotImplementedController{})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotImplemented, rec.Code)
	})

	t.Run("pause", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotImplemented, rec.Code)
		var resp ctldapi.PauseResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.False(t, resp.Paused)
		assert.Equal(t, "ctld pause not implemented", resp.Error)
	})

	t.Run("resume", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/resume", nil)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusNotImplemented, rec.Code)
		var resp ctldapi.ResumeResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.False(t, resp.Resumed)
		assert.Equal(t, "ctld resume not implemented", resp.Error)
	})
}

func TestCombinedControllerRoutesMountedVolumeAPIToPortalHandler(t *testing.T) {
	portal := fakeVolumePortalHandler{
		mountedHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/sandboxvolumes/vol-1/files", r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal:     portal,
	})

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/files?path=/hello.txt", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestCombinedControllerRoutesPodProbeToPowerController(t *testing.T) {
	controller := &fakeProbeController{}
	server := newHTTPServer(":0", combinedController{
		Controller: controller,
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/pods/tpl-default/pod-1/probes/readiness", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "tpl-default", controller.namespace)
	assert.Equal(t, "pod-1", controller.name)
	assert.Equal(t, sandboxprobe.KindReadiness, controller.kind)

	var resp sandboxprobe.Response
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, sandboxprobe.StatusPassed, resp.Status)
}

func TestPrepareVolumePortalHandoffReturnsConflictForActivePortal(t *testing.T) {
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal: fakeVolumePortalHandler{
			prepareErr: fmt.Errorf("volume vol-1 is actively bound to a portal"),
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/volume-portals/handoffs/prepare", strings.NewReader(`{"sandboxvolume_id":"vol-1"}`))
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestBindVolumePortalReturnsConflictForActiveOwner(t *testing.T) {
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal: fakeVolumePortalHandler{
			bindErr: fmt.Errorf("volume vol-1 already has an active owner on cluster-a/pod-a"),
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/volume-portals/bind", strings.NewReader(`{"sandboxvolume_id":"vol-1","pod_uid":"pod-1","team_id":"team-1","portal_name":"workspace","mount_path":"/workspace"}`))
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestBindVolumePortalReturnsConflictForAlreadyBoundPortal(t *testing.T) {
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal: fakeVolumePortalHandler{
			bindErr: fmt.Errorf("volume vol-1 is already bound to /workspace"),
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/volume-portals/bind", strings.NewReader(`{"sandboxvolume_id":"vol-1","pod_uid":"pod-1","team_id":"team-1","portal_name":"workspace","mount_path":"/workspace"}`))
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestReleaseVolumeOwnerReturnsConflictForBusyOwner(t *testing.T) {
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal: fakeVolumePortalHandler{
			releaseErr:  fmt.Errorf("volume vol-1 is actively bound to a portal"),
			releaseResp: ctldapi.ReleaseVolumeOwnerResponse{Busy: true},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/volume-portals/owners/release", strings.NewReader(`{"sandboxvolume_id":"vol-1"}`))
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestRootFSPrepareRouteForwardsToPortalHandler(t *testing.T) {
	portal := fakeVolumePortalHandler{
		rootfsPrepareResp: ctldapi.PrepareRootFSResponse{
			Prepared:       true,
			SandboxID:      "sandbox-1",
			RootFSVolumeID: "vol-rootfs",
			UpperDir:       "/var/lib/sandbox0/ctld/rootfs/team/sandbox/vol/s0fs/upper",
			WorkDir:        "/var/lib/sandbox0/ctld/rootfs/team/sandbox/vol/s0fs/work",
		},
	}
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal:     portal,
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/prepare", strings.NewReader(`{"sandbox_id":"sandbox-1","team_id":"team-a","rootfs_volume_id":"vol-rootfs"}`))
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp ctldapi.PrepareRootFSResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Prepared)
	assert.Equal(t, "vol-rootfs", resp.RootFSVolumeID)
	assert.NotEmpty(t, resp.UpperDir)
	assert.NotEmpty(t, resp.WorkDir)
}

func TestLocalRootFSPrepareClientCallsPortal(t *testing.T) {
	client := localRootFSPrepareClient{
		portal: fakeVolumePortalHandler{
			rootfsPrepareResp: ctldapi.PrepareRootFSResponse{
				Prepared:       true,
				SandboxID:      "sandbox-a",
				RootFSVolumeID: "rootfs-a",
				UpperDir:       "/s0fs/upper",
				WorkDir:        "/s0fs/work",
			},
		},
	}

	resp, err := client.PrepareRootFS(context.Background(), "", ctldapi.PrepareRootFSRequest{
		SandboxID:      "sandbox-a",
		TeamID:         "team-a",
		RootFSVolumeID: "rootfs-a",
	})
	require.NoError(t, err)
	assert.True(t, resp.Prepared)
	assert.Equal(t, "/s0fs/upper", resp.UpperDir)
}

func TestNormalizeRootFSCRIEndpoint(t *testing.T) {
	assert.Equal(t, "unix:///host-run/containerd/containerd.sock", normalizeRootFSCRIEndpoint(""))
	assert.Equal(t, "unix:///host-run/containerd/containerd.sock", normalizeRootFSCRIEndpoint("/host-run/containerd/containerd.sock"))
	assert.Equal(t, "unix:///custom.sock", normalizeRootFSCRIEndpoint("unix:///custom.sock"))
	assert.Equal(t, "dns:///cri.example", normalizeRootFSCRIEndpoint("dns:///cri.example"))
}

type fakeProbeController struct {
	ctldserver.NotImplementedController
	namespace string
	name      string
	kind      sandboxprobe.Kind
}

func (f *fakeProbeController) ProbePod(_ *http.Request, namespace, name string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	f.namespace = namespace
	f.name = name
	f.kind = kind
	return sandboxprobe.Passed(kind, "ProbePassed", "ok", nil), http.StatusOK
}

type fakeVolumePortalHandler struct {
	mountedHandler    http.Handler
	bindErr           error
	prepareErr        error
	releaseResp       ctldapi.ReleaseVolumeOwnerResponse
	releaseErr        error
	rootfsPrepareResp ctldapi.PrepareRootFSResponse
	rootfsPrepareErr  error
}

func (f fakeVolumePortalHandler) Bind(_ context.Context, _ ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, error) {
	if f.bindErr != nil {
		return ctldapi.BindVolumePortalResponse{}, f.bindErr
	}
	return ctldapi.BindVolumePortalResponse{}, nil
}

func (f fakeVolumePortalHandler) Unbind(_ context.Context, _ ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, error) {
	return ctldapi.UnbindVolumePortalResponse{}, nil
}

func (f fakeVolumePortalHandler) CheckPublished(_ context.Context, _ ctldapi.CheckVolumePortalsRequest) (ctldapi.CheckVolumePortalsResponse, error) {
	return ctldapi.CheckVolumePortalsResponse{Ready: true}, nil
}

func (f fakeVolumePortalHandler) AttachOwner(_ context.Context, _ ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, error) {
	return ctldapi.AttachVolumeOwnerResponse{Attached: true}, nil
}

func (f fakeVolumePortalHandler) ReleaseOwner(_ context.Context, _ ctldapi.ReleaseVolumeOwnerRequest) (ctldapi.ReleaseVolumeOwnerResponse, error) {
	if f.releaseErr != nil {
		return f.releaseResp, f.releaseErr
	}
	if f.releaseResp.Released || f.releaseResp.Busy || f.releaseResp.Error != "" {
		return f.releaseResp, nil
	}
	return ctldapi.ReleaseVolumeOwnerResponse{Released: true}, nil
}

func (f fakeVolumePortalHandler) PrepareHandoff(_ context.Context, _ ctldapi.PrepareVolumePortalHandoffRequest) (ctldapi.PrepareVolumePortalHandoffResponse, error) {
	if f.prepareErr != nil {
		return ctldapi.PrepareVolumePortalHandoffResponse{}, f.prepareErr
	}
	return ctldapi.PrepareVolumePortalHandoffResponse{Prepared: true}, nil
}

func (f fakeVolumePortalHandler) CompleteHandoff(_ context.Context, _ ctldapi.CompleteVolumePortalHandoffRequest) (ctldapi.CompleteVolumePortalHandoffResponse, error) {
	return ctldapi.CompleteVolumePortalHandoffResponse{Completed: true}, nil
}

func (f fakeVolumePortalHandler) AbortHandoff(_ context.Context, _ ctldapi.AbortVolumePortalHandoffRequest) (ctldapi.AbortVolumePortalHandoffResponse, error) {
	return ctldapi.AbortVolumePortalHandoffResponse{Aborted: true}, nil
}

func (f fakeVolumePortalHandler) PrepareSnapshotCheckpoint(_ context.Context, _ ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, error) {
	return ctldapi.PrepareVolumeSnapshotCheckpointResponse{Prepared: true}, nil
}

func (f fakeVolumePortalHandler) CompleteSnapshotCheckpoint(_ context.Context, _ ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, error) {
	return ctldapi.CompleteVolumeSnapshotCheckpointResponse{Completed: true}, nil
}

func (f fakeVolumePortalHandler) AbortSnapshotCheckpoint(_ context.Context, _ ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, error) {
	return ctldapi.AbortVolumeSnapshotCheckpointResponse{Aborted: true}, nil
}

func (f fakeVolumePortalHandler) PrepareRootFS(_ context.Context, _ ctldapi.PrepareRootFSRequest) (ctldapi.PrepareRootFSResponse, error) {
	if f.rootfsPrepareErr != nil {
		return ctldapi.PrepareRootFSResponse{}, f.rootfsPrepareErr
	}
	if f.rootfsPrepareResp.Prepared || f.rootfsPrepareResp.Error != "" {
		return f.rootfsPrepareResp, nil
	}
	return ctldapi.PrepareRootFSResponse{Prepared: true}, nil
}

func (f fakeVolumePortalHandler) CheckpointRootFS(context.Context, ctldapi.CheckpointRootFSRequest) (ctldapi.CheckpointRootFSResponse, error) {
	return ctldapi.CheckpointRootFSResponse{Checkpointed: true}, nil
}

func (f fakeVolumePortalHandler) ReleaseRootFS(context.Context, ctldapi.ReleaseRootFSRequest) (ctldapi.ReleaseRootFSResponse, error) {
	return ctldapi.ReleaseRootFSResponse{Released: true}, nil
}

func (f fakeVolumePortalHandler) MountedVolumeHandler() http.Handler {
	return f.mountedHandler
}
