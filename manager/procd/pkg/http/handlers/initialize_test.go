package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	filepkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
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

func TestInitializeConfiguresRootPath(t *testing.T) {
	dispatcher := webhook.NewDispatcher(webhook.Options{}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	hostRoot := t.TempDir()
	rootfs := t.TempDir()
	fileManager, err := filepkg.NewManager(hostRoot)
	if err != nil {
		t.Fatalf("new file manager: %v", err)
	}
	contextManager := ctxpkg.NewManager()
	handler := NewInitializeHandler(dispatcher, fileManager, contextManager, 8080, zap.NewNop())

	body, err := json.Marshal(InitializeRequest{
		SandboxID: "sandbox-1",
		RootPath:  rootfs,
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
	if got := fileManager.GetRootPath(); got != rootfs {
		t.Fatalf("file root path = %q, want %q", got, rootfs)
	}
	if _, err := os.Stat(filepath.Join(rootfs, "bin", "sh")); !os.IsNotExist(err) {
		t.Fatalf("initialize copied base image content into rootfs, err=%v", err)
	}
	if err := fileManager.WriteFile("/etc/sandbox0-root.txt", []byte("ok"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(rootfs, "etc", "sandbox0-root.txt"))
	if err != nil {
		t.Fatalf("read file under rootfs: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("rootfs file = %q, want ok", string(data))
	}

	var processRootPath string
	contextManager.SetStartHandler(func(event process.StartEvent) {
		processRootPath = event.Config.RootPath
	})
	ctx, err := contextManager.CreateContext(process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		Command: []string{"/bin/sh", "-c", "true"},
	})
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	defer func() {
		_ = ctx.Stop()
	}()
	if processRootPath != "" {
		t.Fatalf("process root path = %q, want empty", processRootPath)
	}
}
