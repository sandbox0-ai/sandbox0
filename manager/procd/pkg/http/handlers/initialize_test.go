package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"go.uber.org/zap"
)

func TestInitializeConfiguresSandboxIdentity(t *testing.T) {
	dispatcher := webhook.NewDispatcher(webhook.Options{}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	contextManager := ctxpkg.NewManager()
	handler := NewInitializeHandler(dispatcher, nil, contextManager, 8080, zap.NewNop())

	body, err := json.Marshal(InitializeRequest{
		SandboxID: "sandbox-1",
		TeamID:    "team-1",
		EnvVars: map[string]string{
			"APP_ENV": "test",
		},
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
	if got := contextManager.SandboxEnvVars()["APP_ENV"]; got != "test" {
		t.Fatalf("sandbox env APP_ENV = %q, want test", got)
	}
}

func TestInitializeClearsSandboxEnvVars(t *testing.T) {
	dispatcher := webhook.NewDispatcher(webhook.Options{}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	contextManager := ctxpkg.NewManager()
	contextManager.SetSandboxEnvVars(map[string]string{"APP_ENV": "test"})
	handler := NewInitializeHandler(dispatcher, nil, contextManager, 8080, zap.NewNop())

	body, err := json.Marshal(InitializeRequest{SandboxID: "sandbox-1"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/initialize", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.Initialize(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("Initialize() status = %d, want %d body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if envVars := contextManager.SandboxEnvVars(); len(envVars) != 0 {
		t.Fatalf("sandbox env vars = %#v, want empty", envVars)
	}
}
