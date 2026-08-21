package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCtldShutdownBudgetFitsDeploymentGracePeriod(t *testing.T) {
	const deployedTerminationGrace = 45 * time.Second

	shutdownBudget := httpShutdownTimeout + runtimeMetricsShutdownTimeout +
		networkRuntimeShutdownTimeout + nomadRuntimeShutdownTimeout
	assert.LessOrEqual(t, shutdownBudget+shutdownGraceMargin, deployedTerminationGrace)
	assert.Equal(t, minimumTerminationGrace, shutdownBudget+shutdownGraceMargin)
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

func TestCombinedControllerExposesPrimaryHealth(t *testing.T) {
	healthy := false
	server := newHTTPServer(":0", combinedController{
		Controller:  ctldserver.NotImplementedController{},
		ReadyCheck:  func() bool { return healthy },
		HealthCheck: func() bool { return healthy },
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	healthy = true
	rec = httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCtldDoesNotExposeRemovedPauseResumeRoutes(t *testing.T) {
	server := newHTTPServer(":0", nil)
	for _, path := range []string{
		"/api/v1/sandboxes/sandbox-1/pause",
		"/api/v1/sandboxes/sandbox-1/resume",
	} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, nil)
			rec := httptest.NewRecorder()
			server.Handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestCombinedControllerRoutesRootFSSnapshotAPI(t *testing.T) {
	server := newHTTPServer(":0", combinedController{
		Controller: ctldserver.NotImplementedController{},
		RootFS:     fakeRootFSHandler{},
	})

	t.Run("prepare", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/snapshots/prepare", strings.NewReader(`{"target":{"namespace":"ns","pod_name":"pod","container_name":"sandbox"}}`))
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp ctldapi.PrepareRootFSSnapshotResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "snapshot-handle", resp.Handle)
	})

	t.Run("publish", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/snapshots/publish", strings.NewReader(`{"handle":"snapshot-handle","sandbox_id":"sandbox-1","team_id":"team-1"}`))
		rec := httptest.NewRecorder()
		server.Handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp ctldapi.PublishRootFSSnapshotResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Published)
		assert.Equal(t, "sha256:test", resp.Descriptor.Digest)
	})

	t.Run("abort", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/snapshots/abort", strings.NewReader(`{"handle":"snapshot-handle"}`))
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

func (fakeRootFSHandler) SaveRootFS(_ *http.Request, _ ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
	return ctldapi.SaveRootFSResponse{}, http.StatusOK
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
