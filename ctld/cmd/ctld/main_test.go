package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	storagedb "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtldShutdownBudgetFitsDeploymentGracePeriod(t *testing.T) {
	const deployedTerminationGrace = 45 * time.Second

	shutdownBudget := httpShutdownTimeout + runtimeMetricsShutdownTimeout + portalShutdownTimeout
	assert.LessOrEqual(t, shutdownBudget+shutdownGraceMargin, deployedTerminationGrace)
	assert.LessOrEqual(t, max(shutdownBudget, networkRuntimeShutdownTimeout)+shutdownGraceMargin, deployedTerminationGrace)
	assert.Equal(t, minimumTerminationGrace, shutdownBudget+shutdownGraceMargin)
}

func TestNewPortalStorageObserverRequiresEnabledMeteringDependencies(t *testing.T) {
	var pool pgxpool.Pool
	repo := storagedb.NewRepository(&pool)

	assert.Nil(t, newPortalStorageObserver(&apiconfig.StorageProxyConfig{}, repo, &pool))
	assert.Nil(t, newPortalStorageObserver(&apiconfig.StorageProxyConfig{
		Metering: apiconfig.MeteringConfig{Enabled: true},
	}, nil, &pool))

	observer := newPortalStorageObserver(&apiconfig.StorageProxyConfig{
		RegionID:         "region-1",
		DefaultClusterId: "cluster-1",
		Metering:         apiconfig.MeteringConfig{Enabled: true},
	}, repo, &pool)
	assert.NotNil(t, observer)
}

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

func TestCombinedControllerRoutesRootFSSnapshotAPI(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	validatorConfig := internalauth.DefaultValidatorConfig(internalauth.ServiceCtld, publicKey)
	validatorConfig.AllowedCallers = internalauth.CtldAllowedCallers()
	validator := internalauth.NewValidator(validatorConfig)
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: internalauth.ServiceManager, PrivateKey: privateKey,
	})
	token, err := generator.Generate(
		internalauth.ServiceCtld,
		"team-1",
		"",
		internalauth.GenerateOptions{SandboxID: "sandbox-1"},
	)
	require.NoError(t, err)
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		RootFS:     fakeRootFSHandler{},
	}, validator)

	t.Run("prepare", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/snapshots/prepare", strings.NewReader(`{"target":{"namespace":"ns","pod_name":"pod","container_name":"sandbox"},"stage_id":"stage-1","team_id":"team-1","sandbox_id":"sandbox-1"}`))
		req.Header.Set(internalauth.DefaultTokenHeader, token)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp ctldapi.PrepareRootFSSnapshotResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "snapshot-handle", resp.Handle)
	})

	t.Run("publish", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/snapshots/publish", strings.NewReader(`{"handle":"snapshot-handle","sandbox_id":"sandbox-1","team_id":"team-1"}`))
		req.Header.Set(internalauth.DefaultTokenHeader, token)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp ctldapi.PublishRootFSSnapshotResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Published)
		assert.Equal(t, "sha256:test", resp.Descriptor.Digest)
	})

	t.Run("abort", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/snapshots/abort", strings.NewReader(`{"handle":"snapshot-handle","sandbox_id":"sandbox-1","team_id":"team-1"}`))
		req.Header.Set(internalauth.DefaultTokenHeader, token)
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp ctldapi.AbortRootFSSnapshotResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Aborted)
	})
}

type fakeRootFSHandler struct{}

func (fakeRootFSHandler) InspectRootFS(_ *http.Request, _ ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
	return ctldapi.InspectRootFSResponse{}, http.StatusOK
}

func (fakeRootFSHandler) PrepareRootFSSnapshot(_ *http.Request, _ ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int) {
	return ctldapi.PrepareRootFSSnapshotResponse{Handle: "snapshot-handle"}, http.StatusOK
}

func (fakeRootFSHandler) PublishRootFSSnapshot(_ *http.Request, _ ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
	return ctldapi.PublishRootFSSnapshotResponse{
		Published:  true,
		Descriptor: ctldapi.RootFSDiffDescriptor{Digest: "sha256:test"},
	}, http.StatusOK
}

func (fakeRootFSHandler) AbortRootFSSnapshot(_ *http.Request, _ ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
	return ctldapi.AbortRootFSSnapshotResponse{Aborted: true}, http.StatusOK
}

func (fakeRootFSHandler) ApplyRootFS(_ *http.Request, _ ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	return ctldapi.ApplyRootFSResponse{}, http.StatusOK
}

type fakeVolumePortalHandler struct {
	mountedHandler http.Handler
	bindErr        error
	releaseResp    ctldapi.ReleaseVolumeOwnerResponse
	releaseErr     error
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

func (f fakeVolumePortalHandler) PrepareSnapshotCheckpoint(_ context.Context, _ ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, error) {
	return ctldapi.PrepareVolumeSnapshotCheckpointResponse{Prepared: true}, nil
}

func (f fakeVolumePortalHandler) CompleteSnapshotCheckpoint(_ context.Context, _ ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, error) {
	return ctldapi.CompleteVolumeSnapshotCheckpointResponse{Completed: true}, nil
}

func (f fakeVolumePortalHandler) AbortSnapshotCheckpoint(_ context.Context, _ ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, error) {
	return ctldapi.AbortVolumeSnapshotCheckpointResponse{Aborted: true}, nil
}

func (f fakeVolumePortalHandler) MountedVolumeHandler() http.Handler {
	return f.mountedHandler
}
