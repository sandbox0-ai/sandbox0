package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"go.uber.org/zap"
)

func TestInitializeConfiguresSandboxIdentity(t *testing.T) {
	dispatcher := webhook.NewDispatcher(webhook.Options{}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	handler := NewInitializeHandler(dispatcher, nil, 8080, zap.NewNop())

	body, err := json.Marshal(InitializeRequest{
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
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
	if resp.Data.SandboxID != "sandbox-1" || resp.Data.TeamID != "team-1" {
		t.Fatalf("response data = %+v", resp.Data)
	}
}
