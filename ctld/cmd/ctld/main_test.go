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

func TestBindSandboxRootFSRoutesToPortalHandler(t *testing.T) {
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		Portal: fakeVolumePortalHandler{
			rootFSBindResp: ctldapi.BindSandboxRootFSResponse{
				FilesystemID: "fs-1",
				MountPoint:   "/sandbox0/rootfs",
			},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandbox-rootfs/bind", strings.NewReader(`{"filesystem_id":"fs-1","team_id":"team-1","sandbox_id":"sandbox-1","pod_uid":"pod-1","runtime_generation":2}`))
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp ctldapi.BindSandboxRootFSResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "fs-1", resp.FilesystemID)
	assert.Equal(t, "/sandbox0/rootfs", resp.MountPoint)
}

type fakeVolumePortalHandler struct {
	mountedHandler http.Handler
	bindErr        error
	prepareErr     error
	releaseResp    ctldapi.ReleaseVolumeOwnerResponse
	releaseErr     error
	rootFSBindResp ctldapi.BindSandboxRootFSResponse
	rootFSBindErr  error
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

func (f fakeVolumePortalHandler) BindSandboxRootFS(_ context.Context, req ctldapi.BindSandboxRootFSRequest) (ctldapi.BindSandboxRootFSResponse, error) {
	if f.rootFSBindErr != nil {
		return ctldapi.BindSandboxRootFSResponse{}, f.rootFSBindErr
	}
	if f.rootFSBindResp.FilesystemID != "" || f.rootFSBindResp.MountPoint != "" {
		return f.rootFSBindResp, nil
	}
	return ctldapi.BindSandboxRootFSResponse{FilesystemID: req.FilesystemID}, nil
}

func (f fakeVolumePortalHandler) FlushSandboxRootFS(_ context.Context, req ctldapi.FlushSandboxRootFSRequest) (ctldapi.FlushSandboxRootFSResponse, error) {
	return ctldapi.FlushSandboxRootFSResponse{Flushed: true, FilesystemID: req.FilesystemID}, nil
}

func (f fakeVolumePortalHandler) ReleaseSandboxRootFS(_ context.Context, req ctldapi.ReleaseSandboxRootFSRequest) (ctldapi.ReleaseSandboxRootFSResponse, error) {
	return ctldapi.ReleaseSandboxRootFSResponse{Released: true, FilesystemID: req.FilesystemID}, nil
}

func (f fakeVolumePortalHandler) MountedVolumeHandler() http.Handler {
	return f.mountedHandler
}
