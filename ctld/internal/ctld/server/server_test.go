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
	rootFSTarget   ctldapi.RootFSContainerRef
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

func (c *recordingController) InspectRootFS(_ *http.Request, req ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
	c.rootFSTarget = req.Target
	return ctldapi.InspectRootFSResponse{Info: ctldapi.RootFSInfo{Runtime: "runc"}}, http.StatusOK
}

func (c *recordingController) SaveRootFS(_ *http.Request, req ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
	c.rootFSTarget = req.Target
	return ctldapi.SaveRootFSResponse{
		Head: ctldapi.RootFSHeadDescriptor{
			Engine:      ctldapi.RootFSStorageEngineS0FS,
			VolumeID:    "sandbox-1",
			ManifestKey: "manifests/00000000000000000001.json",
			ManifestSeq: 1,
		},
	}, http.StatusOK
}

func (c *recordingController) ApplyRootFS(_ *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	c.rootFSTarget = req.Target
	return ctldapi.ApplyRootFSResponse{Applied: true}, http.StatusOK
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

func TestNewMuxRoutesRootFS(t *testing.T) {
	controller := &recordingController{}
	handler := NewMux(controller)

	target := ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", PodUID: "uid-1", ContainerName: "sandbox"}
	tests := []struct {
		name string
		path string
		body any
		want func(*testing.T, []byte)
	}{
		{
			name: "inspect",
			path: "/api/v1/rootfs/inspect",
			body: ctldapi.InspectRootFSRequest{Target: target},
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.InspectRootFSResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "runc", resp.Info.Runtime)
			},
		},
		{
			name: "save",
			path: "/api/v1/rootfs/save",
			body: ctldapi.SaveRootFSRequest{Target: target, SandboxID: "sandbox-1", TeamID: "team-1"},
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.SaveRootFSResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "manifests/00000000000000000001.json", resp.Head.ManifestKey)
			},
		},
		{
			name: "apply",
			path: "/api/v1/rootfs/apply",
			body: ctldapi.ApplyRootFSRequest{
				Target: target,
				Head: ctldapi.RootFSHeadDescriptor{
					Engine:      ctldapi.RootFSStorageEngineS0FS,
					VolumeID:    "sandbox-1",
					ManifestKey: "manifests/00000000000000000001.json",
					ManifestSeq: 1,
				},
			},
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.ApplyRootFSResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.True(t, resp.Applied)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(tt.body)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, tt.path, bytes.NewReader(payload))
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, target, controller.rootFSTarget)
			tt.want(t, rec.Body.Bytes())
		})
	}
}
