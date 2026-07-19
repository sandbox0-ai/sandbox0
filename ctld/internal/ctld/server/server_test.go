package server

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
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

func (c *recordingController) ApplyRootFS(_ *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	c.rootFSTarget = req.Target
	return ctldapi.ApplyRootFSResponse{Applied: true}, http.StatusOK
}

func (c *recordingController) PrepareRootFSSnapshot(_ *http.Request, req ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int) {
	c.rootFSTarget = req.Target
	return ctldapi.PrepareRootFSSnapshotResponse{Handle: req.StageID}, http.StatusOK
}

func (c *recordingController) PublishRootFSSnapshot(_ *http.Request, _ ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
	return ctldapi.PublishRootFSSnapshotResponse{Published: true}, http.StatusOK
}

func (c *recordingController) AbortRootFSSnapshot(_ *http.Request, _ ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
	return ctldapi.AbortRootFSSnapshotResponse{Aborted: true}, http.StatusOK
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

func TestNewMuxReadinessIncludesControllerState(t *testing.T) {
	controller := &readinessTestController{}
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

type readinessTestController struct {
	NotImplementedController
	ready bool
}

func (c *readinessTestController) Ready() bool { return c.ready }

func TestNewMuxRoutesRootFS(t *testing.T) {
	controller := &recordingController{}
	validator, manager := rootFSAuthFixture(t)
	handler := NewMux(controller, validator)
	token, err := manager.Generate(internalauth.ServiceCtld, "team-1", "", internalauth.GenerateOptions{SandboxID: "sandbox-1"})
	require.NoError(t, err)

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
			name: "apply",
			path: "/api/v1/rootfs/apply",
			body: ctldapi.ApplyRootFSRequest{
				Target:    target,
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
				Descriptor: ctldapi.RootFSDiffDescriptor{
					Digest: "sha256:abc", ObjectKey: "rootfs/diff.tar",
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
			if tt.name == "apply" {
				req.Header.Set(internalauth.DefaultTokenHeader, token)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, target, controller.rootFSTarget)
			tt.want(t, rec.Body.Bytes())
		})
	}
}

func TestNewMuxAuthenticatesRootFSMutationsAndBindsOwnerClaims(t *testing.T) {
	// Use the same trust key for the wrong caller so rejection proves the
	// allowed-caller check rather than only signature verification.
	validator, manager, privateKey := rootFSAuthFixtureWithKey(t)
	wrongCaller := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceCtld,
		PrivateKey: privateKey,
	})
	handler := NewMux(&recordingController{}, validator)

	mutations := []struct {
		name string
		path string
		body any
	}{
		{
			name: "prepare",
			path: "/api/v1/rootfs/snapshots/prepare",
			body: ctldapi.PrepareRootFSSnapshotRequest{
				Target: rootFSAuthTarget(), StageID: "stage-1", TeamID: "team-1", SandboxID: "sandbox-1",
			},
		},
		{
			name: "publish",
			path: "/api/v1/rootfs/snapshots/publish",
			body: ctldapi.PublishRootFSSnapshotRequest{Handle: "stage-1", TeamID: "team-1", SandboxID: "sandbox-1"},
		},
		{
			name: "abort",
			path: "/api/v1/rootfs/snapshots/abort",
			body: ctldapi.AbortRootFSSnapshotRequest{Handle: "stage-1", TeamID: "team-1", SandboxID: "sandbox-1"},
		},
		{
			name: "apply",
			path: "/api/v1/rootfs/apply",
			body: ctldapi.ApplyRootFSRequest{Target: rootFSAuthTarget(), TeamID: "team-1", SandboxID: "sandbox-1"},
		},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			payload, marshalErr := json.Marshal(mutation.body)
			require.NoError(t, marshalErr)
			request := func(token string) *http.Request {
				req := httptest.NewRequest(http.MethodPost, mutation.path, bytes.NewReader(payload))
				if token != "" {
					req.Header.Set(internalauth.DefaultTokenHeader, token)
				}
				return req
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, request(""))
			assert.Equal(t, http.StatusUnauthorized, rec.Code)

			wrongToken, tokenErr := wrongCaller.Generate(
				internalauth.ServiceCtld, "team-1", "", internalauth.GenerateOptions{SandboxID: "sandbox-1"},
			)
			require.NoError(t, tokenErr)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, request(wrongToken))
			assert.Equal(t, http.StatusUnauthorized, rec.Code)

			spoofedTeam, tokenErr := manager.Generate(
				internalauth.ServiceCtld, "team-other", "", internalauth.GenerateOptions{SandboxID: "sandbox-1"},
			)
			require.NoError(t, tokenErr)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, request(spoofedTeam))
			assert.Equal(t, http.StatusForbidden, rec.Code)

			spoofedSandbox, tokenErr := manager.Generate(
				internalauth.ServiceCtld, "team-1", "", internalauth.GenerateOptions{SandboxID: "sandbox-other"},
			)
			require.NoError(t, tokenErr)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, request(spoofedSandbox))
			assert.Equal(t, http.StatusForbidden, rec.Code)

			validToken, tokenErr := manager.Generate(
				internalauth.ServiceCtld, "team-1", "", internalauth.GenerateOptions{SandboxID: "sandbox-1"},
			)
			require.NoError(t, tokenErr)
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, request(validToken))
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func rootFSAuthTarget() ctldapi.RootFSContainerRef {
	return ctldapi.RootFSContainerRef{Namespace: "default", PodName: "pod-1", ContainerName: "sandbox"}
}

func rootFSAuthFixture(t *testing.T) (*internalauth.Validator, *internalauth.Generator) {
	t.Helper()
	validator, generator, _ := rootFSAuthFixtureWithKey(t)
	return validator, generator
}

func rootFSAuthFixtureWithKey(t *testing.T) (*internalauth.Validator, *internalauth.Generator, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	config := internalauth.DefaultValidatorConfig(internalauth.ServiceCtld, publicKey)
	config.AllowedCallers = internalauth.CtldAllowedCallers()
	return internalauth.NewValidator(config), internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceManager,
		PrivateKey: privateKey,
	}), privateKey
}

func TestNewMuxDoesNotExposeSingleStepRootFSPublish(t *testing.T) {
	handler := NewMux(&recordingController{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/rootfs/save", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}
