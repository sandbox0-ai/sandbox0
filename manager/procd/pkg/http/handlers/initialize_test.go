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
	handler.rootfsBootstrapSource = writeBootstrapSource(t)

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
}

func TestBootstrapRootfsCopiesBaseContent(t *testing.T) {
	source := writeBootstrapSource(t)
	target := t.TempDir()

	if err := bootstrapRootfsFrom(source, target); err != nil {
		t.Fatalf("bootstrapRootfsFrom() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(target, "bin", "sh"))
	if err != nil {
		t.Fatalf("read copied shell: %v", err)
	}
	if string(data) != "#!/bin/sh\n" {
		t.Fatalf("copied shell = %q", string(data))
	}
	link, err := os.Readlink(filepath.Join(target, "usr", "bin", "sh"))
	if err != nil {
		t.Fatalf("read copied symlink: %v", err)
	}
	if link != "../../bin/sh" {
		t.Fatalf("copied symlink = %q, want ../../bin/sh", link)
	}
	if _, err := os.Stat(filepath.Join(target, "proc", "ignored")); !os.IsNotExist(err) {
		t.Fatalf("proc content was copied, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(target, rootfsBootstrapMarker)); err != nil {
		t.Fatalf("bootstrap marker missing: %v", err)
	}
}

func TestBootstrapRootfsSkipsExistingMarker(t *testing.T) {
	source := writeBootstrapSource(t)
	target := t.TempDir()
	if err := os.MkdirAll(filepath.Join(target, ".sandbox0"), 0o755); err != nil {
		t.Fatalf("create marker dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, rootfsBootstrapMarker), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if err := bootstrapRootfsFrom(source, target); err != nil {
		t.Fatalf("bootstrapRootfsFrom() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "bin", "sh")); !os.IsNotExist(err) {
		t.Fatalf("bootstrap copied content despite marker, err=%v", err)
	}
}

func TestBootstrapRootfsCompletesNonEmptyTarget(t *testing.T) {
	source := writeBootstrapSource(t)
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "custom.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write custom file: %v", err)
	}

	if err := bootstrapRootfsFrom(source, target); err != nil {
		t.Fatalf("bootstrapRootfsFrom() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "bin", "sh"))
	if err != nil {
		t.Fatalf("read copied shell: %v", err)
	}
	if string(data) != "#!/bin/sh\n" {
		t.Fatalf("copied shell = %q", string(data))
	}
	custom, err := os.ReadFile(filepath.Join(target, "custom.txt"))
	if err != nil {
		t.Fatalf("read custom file: %v", err)
	}
	if string(custom) != "keep" {
		t.Fatalf("custom file = %q, want keep", string(custom))
	}
	if _, err := os.Stat(filepath.Join(target, rootfsBootstrapMarker)); err != nil {
		t.Fatalf("bootstrap marker missing: %v", err)
	}
}

func TestBootstrapRootfsDoesNotOverwriteExistingFiles(t *testing.T) {
	source := writeBootstrapSource(t)
	target := t.TempDir()
	if err := os.MkdirAll(filepath.Join(target, "bin"), 0o755); err != nil {
		t.Fatalf("create target bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "bin", "sh"), []byte("custom shell\n"), 0o755); err != nil {
		t.Fatalf("write custom shell: %v", err)
	}

	if err := bootstrapRootfsFrom(source, target); err != nil {
		t.Fatalf("bootstrapRootfsFrom() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "bin", "sh"))
	if err != nil {
		t.Fatalf("read target shell: %v", err)
	}
	if string(data) != "custom shell\n" {
		t.Fatalf("target shell = %q, want custom shell", string(data))
	}
	if _, err := os.Stat(filepath.Join(target, rootfsBootstrapMarker)); err != nil {
		t.Fatalf("bootstrap marker missing: %v", err)
	}
}

func writeBootstrapSource(t *testing.T) string {
	t.Helper()
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "bin"), 0o755); err != nil {
		t.Fatalf("create bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "bin", "sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write shell: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(source, "usr", "bin"), 0o755); err != nil {
		t.Fatalf("create usr bin: %v", err)
	}
	if err := os.Symlink("../../bin/sh", filepath.Join(source, "usr", "bin", "sh")); err != nil {
		t.Fatalf("write symlink: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(source, "proc"), 0o755); err != nil {
		t.Fatalf("create proc: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "proc", "ignored"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("write proc file: %v", err)
	}
	return source
}
