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
	"go.uber.org/zap"
)

func TestInitializeReturnsBootstrapMountStatus(t *testing.T) {
	dispatcher := webhook.NewDispatcher(webhook.Options{}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	volumeManager := volume.NewManager(&volume.Config{}, bootstrapTestTokenProvider("token"), zap.NewNop())
	handler := NewInitializeHandler(dispatcher, nil, volumeManager, 8080, zap.NewNop())

	body, err := json.Marshal(InitializeRequest{
		SandboxID:     "sandbox-1",
		TeamID:        "team-1",
		WaitForMounts: true,
		Mounts: []InitializeMount{{
			SandboxVolumeID: "vol-1",
			MountPoint:      t.TempDir(),
		}},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/initialize", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.Initialize(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("Initialize() status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var resp struct {
		Success bool               `json:"success"`
		Data    InitializeResponse `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response: %s", recorder.Body.String())
	}
	if len(resp.Data.BootstrapMounts) != 1 {
		t.Fatalf("bootstrap mounts = %d, want 1", len(resp.Data.BootstrapMounts))
	}
	if resp.Data.BootstrapMounts[0].State != volume.MountStateFailed {
		t.Fatalf("bootstrap state = %q, want %q", resp.Data.BootstrapMounts[0].State, volume.MountStateFailed)
	}
	if resp.Data.BootstrapMounts[0].ErrorCode != "mount_failed" {
		t.Fatalf("bootstrap error code = %q, want %q", resp.Data.BootstrapMounts[0].ErrorCode, "mount_failed")
	}
}

type bootstrapTestTokenProvider string

func (p bootstrapTestTokenProvider) GetInternalToken() string {
	return string(p)
}
