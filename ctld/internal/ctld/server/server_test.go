package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
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
	rootFSAction   string
	rootFSSandbox  string
	rootFSHead     string
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

func (c *recordingController) BindRootFSSync(_ *http.Request, req ctldapi.BindRootFSSyncRequest) (ctldapi.BindRootFSSyncResponse, int) {
	c.rootFSAction = "bind"
	c.rootFSTarget = req.Target
	c.rootFSSandbox = req.SandboxID
	return ctldapi.BindRootFSSyncResponse{Status: ctldapi.RootFSSyncStatus{SandboxID: req.SandboxID}}, http.StatusOK
}

func (c *recordingController) GetRootFSSyncStatus(_ *http.Request, req ctldapi.GetRootFSSyncStatusRequest) (ctldapi.GetRootFSSyncStatusResponse, int) {
	c.rootFSAction = "status"
	c.rootFSSandbox = req.SandboxID
	return ctldapi.GetRootFSSyncStatusResponse{Status: ctldapi.RootFSSyncStatus{SandboxID: req.SandboxID}}, http.StatusOK
}

func (c *recordingController) SealRootFSHead(_ *http.Request, req ctldapi.SealRootFSHeadRequest) (ctldapi.SealRootFSHeadResponse, int) {
	c.rootFSAction = "seal"
	c.rootFSSandbox = req.SandboxID
	c.rootFSHead = req.HeadID
	return ctldapi.SealRootFSHeadResponse{}, http.StatusOK
}

func (c *recordingController) AcknowledgeRootFSHead(_ *http.Request, req ctldapi.AcknowledgeRootFSHeadRequest) (ctldapi.AcknowledgeRootFSHeadResponse, int) {
	c.rootFSAction = "acknowledge"
	c.rootFSSandbox = req.SandboxID
	c.rootFSHead = req.HeadID
	return ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true}, http.StatusOK
}

func (c *recordingController) MaterializeRootFSHead(_ *http.Request, req ctldapi.MaterializeRootFSHeadRequest) (ctldapi.MaterializeRootFSHeadResponse, int) {
	c.rootFSAction = "materialize"
	c.rootFSHead = req.Reference.HeadID
	return ctldapi.MaterializeRootFSHeadResponse{ImageName: req.Image.Name, Materialized: true}, http.StatusOK
}

func (c *recordingController) ImportRootFSImage(_ *http.Request, req ctldapi.ImportRootFSImageRequest) (ctldapi.ImportRootFSImageResponse, int) {
	c.rootFSAction = "import"
	c.rootFSHead = req.HeadID
	return ctldapi.ImportRootFSImageResponse{}, http.StatusOK
}

func (c *recordingController) ReleaseCarrierGate(_ *http.Request, req ctldapi.ReleaseCarrierGateRequest) (ctldapi.ReleaseCarrierGateResponse, int) {
	c.rootFSAction = "release"
	c.rootFSHead = req.Slot
	return ctldapi.ReleaseCarrierGateResponse{Released: true}, http.StatusOK
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

func TestNewMuxJSONPostRouteFailureResponses(t *testing.T) {
	handler := NewMux(NotImplementedController{})

	t.Run("unsupported volume portal controller", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/volume-portals/bind", bytes.NewBufferString(`{}`))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotImplemented, rec.Code)
		assert.JSONEq(t, `{"sandboxvolume_id":"","mount_point":"","mounted_at":""}`, rec.Body.String())
	})

	t.Run("invalid rootfs request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/v1/rootfs/sync/bind", bytes.NewBufferString(`{"target":`))
		rec := httptest.NewRecorder()

		NewMux(&recordingController{}).ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		var resp ctldapi.BindRootFSSyncResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.NotEmpty(t, resp.Error)
	})

	t.Run("wrong method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/rootfs/sync/bind", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})
}

func TestNewMuxDoesNotExposeRuntimeWatchOnControlPort(t *testing.T) {
	handler := NewMux(&recordingController{})
	req := httptest.NewRequest(http.MethodGet, runtimecontrol.WatchPath, nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestNewMuxReadinessIncludesControllerState(t *testing.T) {
	controller := &readinessTestController{healthy: true}
	handler := NewMux(controller)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	controller.ready = true
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestNewMuxHealthIncludesControllerState(t *testing.T) {
	controller := &readinessTestController{ready: true}
	handler := NewMux(controller)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	controller.healthy = true
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthy status = %d, want %d", rec.Code, http.StatusOK)
	}
}

type readinessTestController struct {
	NotImplementedController
	ready   bool
	healthy bool
}

func (c *readinessTestController) Ready() bool { return c.ready }
func (c *readinessTestController) Healthy() bool {
	return c.healthy
}

func TestNewMuxRoutesRootFS(t *testing.T) {
	controller := &recordingController{}
	handler := NewMux(controller)

	target := ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", PodUID: "uid-1", ContainerName: "sandbox"}
	tests := []struct {
		name       string
		method     string
		path       string
		body       any
		wantAction string
		want       func(*testing.T, []byte)
	}{
		{
			name:       "bind",
			method:     http.MethodPut,
			path:       "/api/v1/rootfs/sync/bind",
			body:       ctldapi.BindRootFSSyncRequest{Target: target, SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 2},
			wantAction: "bind",
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.BindRootFSSyncResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "sandbox-1", resp.Status.SandboxID)
			},
		},
		{
			name:       "status",
			method:     http.MethodPost,
			path:       "/api/v1/rootfs/sync/status",
			body:       ctldapi.GetRootFSSyncStatusRequest{SandboxID: "sandbox-1", RuntimeGeneration: 2},
			wantAction: "status",
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.GetRootFSSyncStatusResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "sandbox-1", resp.Status.SandboxID)
			},
		},
		{
			name:       "seal",
			method:     http.MethodPut,
			path:       "/api/v1/rootfs/heads/seal",
			body:       ctldapi.SealRootFSHeadRequest{SandboxID: "sandbox-1", TeamID: "team-1", HeadID: "head-1", ExpectedRuntimeGeneration: 2},
			wantAction: "seal",
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.SealRootFSHeadResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Empty(t, resp.Error)
			},
		},
		{
			name:       "materialize",
			method:     http.MethodPut,
			path:       "/api/v1/rootfs/heads/materialize",
			body:       ctldapi.MaterializeRootFSHeadRequest{},
			wantAction: "materialize",
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.MaterializeRootFSHeadResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.True(t, resp.Materialized)
			},
		},
		{
			name:       "acknowledge",
			method:     http.MethodPut,
			path:       "/api/v1/rootfs/heads/acknowledge",
			body:       ctldapi.AcknowledgeRootFSHeadRequest{SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 2, HeadID: "head-1", RuntimeContinues: true},
			wantAction: "acknowledge",
			want: func(t *testing.T, body []byte) {
				t.Helper()
				var resp ctldapi.AcknowledgeRootFSHeadResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.True(t, resp.Acknowledged)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(tt.body)
			require.NoError(t, err)
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(payload))
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantAction, controller.rootFSAction)
			tt.want(t, rec.Body.Bytes())
		})
	}
}

func TestNewMuxDoesNotExposeLegacyRootFSRoutes(t *testing.T) {
	handler := NewMux(&recordingController{})
	for _, path := range []string{
		"/api/v1/rootfs/inspect",
		"/api/v1/rootfs/save",
		"/api/v1/rootfs/snapshots/prepare",
		"/api/v1/rootfs/snapshots/publish",
		"/api/v1/rootfs/snapshots/abort",
		"/api/v1/rootfs/apply",
	} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{}`))
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
