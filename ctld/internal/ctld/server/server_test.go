package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	attachSandbox  string
	attachReq      ctldapi.VolumeAttachRequest
	detachSandbox  string
	detachReq      ctldapi.VolumeDetachRequest
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

func (c *recordingController) AttachVolume(_ *http.Request, sandboxID string, req ctldapi.VolumeAttachRequest) (ctldapi.VolumeAttachResponse, int) {
	c.attachSandbox = sandboxID
	c.attachReq = req
	return ctldapi.VolumeAttachResponse{Attached: true, AttachmentID: "attach-1", MountSessionID: "session-1"}, http.StatusOK
}

func (c *recordingController) DetachVolume(_ *http.Request, sandboxID string, req ctldapi.VolumeDetachRequest) (ctldapi.VolumeDetachResponse, int) {
	c.detachSandbox = sandboxID
	c.detachReq = req
	return ctldapi.VolumeDetachResponse{Detached: true}, http.StatusOK
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

	t.Run("volume attach", func(t *testing.T) {
		body := []byte(`{"sandboxvolume_id":"vol-1","mount_point":"/workspace/data"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-4/volumes/attach", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "sandbox-4", controller.attachSandbox)
		assert.Equal(t, "vol-1", controller.attachReq.SandboxVolumeID)
		assert.Equal(t, "/workspace/data", controller.attachReq.MountPoint)
	})

	t.Run("volume detach", func(t *testing.T) {
		body := []byte(`{"sandboxvolume_id":"vol-1","mount_point":"/workspace/data","attachment_id":"attach-1"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-4/volumes/detach", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "sandbox-4", controller.detachSandbox)
		assert.Equal(t, "attach-1", controller.detachReq.AttachmentID)
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
