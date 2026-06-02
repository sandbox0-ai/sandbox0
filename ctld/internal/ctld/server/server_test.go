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
}

type recordingRootFSController struct {
	recordingController
	prepareReq ctldapi.PrepareRootFSRequest
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

func (c *recordingRootFSController) PrepareRootFS(_ *http.Request, req ctldapi.PrepareRootFSRequest) (ctldapi.PrepareRootFSResponse, int) {
	c.prepareReq = req
	return ctldapi.PrepareRootFSResponse{
		Prepared:       true,
		SandboxID:      req.SandboxID,
		RootFSVolumeID: req.RootFSVolumeID,
		UpperDir:       "/rootfs/upper",
		WorkDir:        "/rootfs/work",
	}, http.StatusOK
}

func (c *recordingRootFSController) CheckpointRootFS(_ *http.Request, _ ctldapi.CheckpointRootFSRequest) (ctldapi.CheckpointRootFSResponse, int) {
	return ctldapi.CheckpointRootFSResponse{Checkpointed: true}, http.StatusOK
}

func (c *recordingRootFSController) ReleaseRootFS(_ *http.Request, _ ctldapi.ReleaseRootFSRequest) (ctldapi.ReleaseRootFSResponse, int) {
	return ctldapi.ReleaseRootFSResponse{Released: true}, http.StatusOK
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

func TestNewMuxRoutesRootFSPrepare(t *testing.T) {
	controller := &recordingRootFSController{}
	handler := NewMux(controller)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/prepare", strings.NewReader(`{"sandbox_id":"sandbox-a","team_id":"team-a","rootfs_volume_id":"rootfs-a"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "sandbox-a", controller.prepareReq.SandboxID)
	assert.Equal(t, "team-a", controller.prepareReq.TeamID)
	assert.Equal(t, "rootfs-a", controller.prepareReq.RootFSVolumeID)
	var resp ctldapi.PrepareRootFSResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Prepared)
	assert.Equal(t, "/rootfs/upper", resp.UpperDir)
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
