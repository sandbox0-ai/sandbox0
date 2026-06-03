package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingController struct {
	pausedSandbox  string
	resumedSandbox string
	probedSandbox  string
	probedPodNS    string
	probedPodName  string
	probedKind     sandboxprobe.Kind
	rootfsBind     ctldapi.BindRootfsRequest
	rootfsCommit   ctldapi.CommitRootfsRequest
	rootfsUnbind   ctldapi.UnbindRootfsRequest
}

func (c *recordingController) Pause(_ *http.Request, sandboxID string) (ctldapi.PauseResponse, int) {
	c.pausedSandbox = sandboxID
	return ctldapi.PauseResponse{Paused: true}, http.StatusOK
}

func (c *recordingController) Resume(_ *http.Request, sandboxID string) (ctldapi.ResumeResponse, int) {
	c.resumedSandbox = sandboxID
	return ctldapi.ResumeResponse{Resumed: true}, http.StatusOK
}

func (c *recordingController) Probe(_ *http.Request, sandboxID string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	c.probedSandbox = sandboxID
	c.probedKind = kind
	return sandboxprobe.Passed(kind, "SandboxProbePassed", "sandbox probe passed", nil), http.StatusOK
}

func (c *recordingController) ProbePod(_ *http.Request, namespace, name string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	c.probedPodNS = namespace
	c.probedPodName = name
	c.probedKind = kind
	return sandboxprobe.Passed(kind, "SandboxProbePassed", "sandbox probe passed", nil), http.StatusOK
}

func (c *recordingController) BindRootfs(_ *http.Request, req ctldapi.BindRootfsRequest) (ctldapi.BindRootfsResponse, int) {
	c.rootfsBind = req
	return ctldapi.BindRootfsResponse{SandboxFilesystemID: req.SandboxFilesystemID, RootPath: req.MountPath}, http.StatusOK
}

func (c *recordingController) CommitRootfs(_ *http.Request, req ctldapi.CommitRootfsRequest) (ctldapi.CommitRootfsResponse, int) {
	c.rootfsCommit = req
	return ctldapi.CommitRootfsResponse{SandboxFilesystemID: req.SandboxFilesystemID, Committed: true}, http.StatusOK
}

func (c *recordingController) UnbindRootfs(_ *http.Request, req ctldapi.UnbindRootfsRequest) (ctldapi.UnbindRootfsResponse, int) {
	c.rootfsUnbind = req
	return ctldapi.UnbindRootfsResponse{SandboxFilesystemID: req.SandboxFilesystemID, Unbound: true}, http.StatusOK
}

func TestNewMuxRoutesPauseResume(t *testing.T) {
	controller := &recordingController{}
	handler := NewMux(controller)

	t.Run("pause", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "sandbox-1", controller.pausedSandbox)
	})

	t.Run("resume", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-2/resume", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "sandbox-2", controller.resumedSandbox)
	})

	t.Run("probe", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-3/probes/readiness", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "sandbox-3", controller.probedSandbox)
		assert.Equal(t, sandboxprobe.KindReadiness, controller.probedKind)
	})

	t.Run("pod probe", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/pods/tpl-default/pod-1/probes/liveness", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "tpl-default", controller.probedPodNS)
		assert.Equal(t, "pod-1", controller.probedPodName)
		assert.Equal(t, sandboxprobe.KindLiveness, controller.probedKind)
	})
}

func TestNewMuxRoutesRootfsLifecycle(t *testing.T) {
	controller := &recordingController{}
	handler := NewMux(controller)

	t.Run("bind", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/bind", strings.NewReader(`{"sandboxfilesystem_id":"fs-1","pod_uid":"pod-1","mount_path":"/rootfs"}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "fs-1", controller.rootfsBind.SandboxFilesystemID)
		assert.Equal(t, "pod-1", controller.rootfsBind.PodUID)
	})

	t.Run("commit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/commit", strings.NewReader(`{"sandboxfilesystem_id":"fs-2","pod_uid":"pod-2"}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "fs-2", controller.rootfsCommit.SandboxFilesystemID)
	})

	t.Run("unbind", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/unbind", strings.NewReader(`{"sandboxfilesystem_id":"fs-3","pod_uid":"pod-3"}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "fs-3", controller.rootfsUnbind.SandboxFilesystemID)
	})
}

func TestNewMuxDefaultsToNotImplementedController(t *testing.T) {
	handler := NewMux(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/pause", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)
	var resp ctldapi.PauseResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.False(t, resp.Paused)
}
