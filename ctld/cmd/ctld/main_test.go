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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestActivePodPortalListerBuildsRecoveryBindings(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tpl-default",
			Name:      "sandbox-a",
			UID:       types.UID("pod-uid"),
			Labels: map[string]string{
				controller.LabelPoolType: controller.PoolTypeActive,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:               "team-a",
				controller.AnnotationSandboxID:            "sandbox-a",
				controller.AnnotationMounts:               `[{"sandboxvolume_id":"vol-workspace","mount_point":"/workspace"}]`,
				controller.AnnotationWebhookStateVolumeID: "vol-state",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Volumes: []corev1.Volume{
				{
					Name: "sandbox0-volume-0-sandbox0-webhook-state",
					VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver: volumeportal.DriverName,
						VolumeAttributes: map[string]string{
							volumeportal.AttributePortalName: volumeportal.WebhookStatePortalName,
							volumeportal.AttributeMountPath:  volumeportal.WebhookStateMountPath,
						},
					}},
				},
				{
					Name: "sandbox0-volume-1-workspace",
					VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
						Driver: volumeportal.DriverName,
						VolumeAttributes: map[string]string{
							volumeportal.AttributePortalName: "workspace",
							volumeportal.AttributeMountPath:  "/workspace",
						},
					}},
				},
			},
		},
	}
	lister := activePodPortalLister(fake.NewSimpleClientset(pod), "node-a")
	require.NotNil(t, lister)

	active, err := lister(context.Background())
	require.NoError(t, err)
	require.Contains(t, active, "pod-uid")
	portals := active["pod-uid"].Portals
	require.Len(t, portals, 2)
	assert.Equal(t, "vol-workspace", portals["sandbox0-volume-1-workspace"].SandboxVolumeID)
	assert.Equal(t, "vol-state", portals["sandbox0-volume-0-sandbox0-webhook-state"].SandboxVolumeID)
	assert.Equal(t, "team-a", portals["sandbox0-volume-1-workspace"].TeamID)
	require.NotNil(t, active["pod-uid"].RuntimeRecovery)
	assert.Equal(t, "procd", active["pod-uid"].RuntimeRecovery.ContainerName)
	assert.Equal(t, "sandbox0-volume-0-sandbox0-webhook-state", active["pod-uid"].RuntimeRecovery.StateVolumeName)
	assert.True(t, active["pod-uid"].RuntimeRecovery.ReplayProcesses)
}

func TestActivePodPortalListerDoesNotRecoverIdlePodRuntime(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tpl-default",
			Name:      "sandbox-idle",
			UID:       types.UID("pod-idle"),
			Labels:    map[string]string{controller.LabelPoolType: controller.PoolTypeIdle},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Volumes: []corev1.Volume{{
				Name: "sandbox0-volume-0-sandbox0-webhook-state",
				VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{
					Driver: volumeportal.DriverName,
					VolumeAttributes: map[string]string{
						volumeportal.AttributePortalName: volumeportal.WebhookStatePortalName,
						volumeportal.AttributeMountPath:  volumeportal.WebhookStateMountPath,
					},
				}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	active, err := activePodPortalLister(fake.NewSimpleClientset(pod), "node-a")(context.Background())
	require.NoError(t, err)
	require.Contains(t, active, "pod-idle")
	require.NotNil(t, active["pod-idle"].RuntimeRecovery)
	assert.False(t, active["pod-idle"].RuntimeRecovery.ReplayProcesses)
}

func TestActivePodPortalListerRejectsMalformedRecoveryBindings(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "tpl-default",
			Name:        "sandbox-a",
			UID:         types.UID("pod-uid"),
			Annotations: map[string]string{controller.AnnotationMounts: "{"},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}
	active, err := activePodPortalLister(fake.NewSimpleClientset(pod), "node-a")(context.Background())
	require.NoError(t, err)
	require.Contains(t, active, "pod-uid")
	require.Error(t, active["pod-uid"].RecoveryError)
	assert.Contains(t, active["pod-uid"].RecoveryError.Error(), controller.AnnotationMounts)
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
