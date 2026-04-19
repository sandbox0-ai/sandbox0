package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/volume"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
)

func TestVolumeMountUsesSandboxIdentityFromInitialize(t *testing.T) {
	dispatcher := webhook.NewDispatcher(webhook.Options{}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})

	client := &fakeHandlerCtldVolumeClient{}
	volumeManager := volume.NewManager(&volume.Config{}, bootstrapTestTokenProvider("token"), zap.NewNop())
	volumeManager.SetCtldVolumeClient(client)

	initializeHandler := NewInitializeHandler(dispatcher, nil, volumeManager, 8080, zap.NewNop())
	initializeBody, err := json.Marshal(InitializeRequest{
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
	})
	if err != nil {
		t.Fatalf("marshal initialize request: %v", err)
	}
	initializeReq := httptest.NewRequest(http.MethodPost, "/api/v1/initialize", bytes.NewReader(initializeBody))
	initializeRecorder := httptest.NewRecorder()
	initializeHandler.Initialize(initializeRecorder, initializeReq)
	if initializeRecorder.Code != http.StatusOK {
		t.Fatalf("Initialize() status = %d, want %d body=%s", initializeRecorder.Code, http.StatusOK, initializeRecorder.Body.String())
	}

	volumeHandler := NewVolumeHandler(volumeManager, zap.NewNop())
	mountBody, err := json.Marshal(volume.MountRequest{
		SandboxVolumeID: "vol-1",
		MountPoint:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("marshal mount request: %v", err)
	}
	mountReq := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxvolumes/mount", bytes.NewReader(mountBody))
	mountRecorder := httptest.NewRecorder()
	volumeHandler.Mount(mountRecorder, mountReq)
	if mountRecorder.Code != http.StatusOK {
		t.Fatalf("Mount() status = %d, want %d body=%s", mountRecorder.Code, http.StatusOK, mountRecorder.Body.String())
	}

	if client.attachReq == nil {
		t.Fatal("Attach() was not called")
	}
	if client.attachReq.SandboxID != "sandbox-1" {
		t.Fatalf("attach sandbox_id = %q, want sandbox-1", client.attachReq.SandboxID)
	}
	if client.attachReq.TeamID != "team-1" {
		t.Fatalf("attach team_id = %q, want team-1", client.attachReq.TeamID)
	}
}

type fakeHandlerCtldVolumeClient struct {
	attachReq *struct {
		SandboxID       string
		TeamID          string
		SandboxVolumeID string
		MountPoint      string
	}
}

func (f *fakeHandlerCtldVolumeClient) Attach(_ context.Context, req *volumeAttachRequestAlias) (*volumeAttachResponseAlias, error) {
	f.attachReq = &struct {
		SandboxID       string
		TeamID          string
		SandboxVolumeID string
		MountPoint      string
	}{
		SandboxID:       req.SandboxID,
		TeamID:          req.TeamID,
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      req.MountPoint,
	}
	return &volumeAttachResponseAlias{
		Attached:       true,
		AttachmentID:   "attach-1",
		MountSessionID: "session-1",
	}, nil
}

func (f *fakeHandlerCtldVolumeClient) Detach(context.Context, *volumeDetachRequestAlias) error {
	return nil
}

type volumeAttachRequestAlias = ctldapi.VolumeAttachRequest
type volumeAttachResponseAlias = ctldapi.VolumeAttachResponse
type volumeDetachRequestAlias = ctldapi.VolumeDetachRequest
