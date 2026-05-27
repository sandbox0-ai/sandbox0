package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPythonBootstrapInvokesHandler(t *testing.T) {
	if _, err := osexec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}

	modulePath := filepath.Join(t.TempDir(), "main.py")
	if err := os.WriteFile(modulePath, []byte(`
def handler(request):
    print("log line")
    return {
        "status": 201,
        "headers": {"x-value": "ok"},
        "body": {"path": request["path"]},
    }
`), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	cmd := osexec.Command("python3", "-c", pythonBootstrap, modulePath, "handler")
	cmd.Stdin = strings.NewReader(`{"path":"/hello"}`)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run bootstrap: %v", err)
	}

	var response struct {
		Status     int                 `json:"status"`
		Headers    map[string][]string `json:"headers"`
		BodyBase64 string              `json:"body_base64"`
	}
	if err := json.Unmarshal(out, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != 201 {
		t.Fatalf("status = %d, want 201", response.Status)
	}
	if got := response.Headers["x-value"]; len(got) != 1 || got[0] != "ok" {
		t.Fatalf("headers[x-value] = %#v, want [ok]", got)
	}

	body, err := base64.StdEncoding.DecodeString(response.BodyBase64)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if string(body) != `{"path":"/hello"}` {
		t.Fatalf("body = %q, want JSON path response", string(body))
	}
}

func TestPythonBootstrapSupportsModuleDirectoryImports(t *testing.T) {
	if _, err := osexec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "helper.py"), []byte(`
def message():
    return "from helper"
`), 0o644); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	modulePath := filepath.Join(dir, "main.py")
	if err := os.WriteFile(modulePath, []byte(`
import helper

def handler(request):
    return [helper.message()]
`), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	cmd := osexec.Command("python3", "-c", pythonBootstrap, modulePath, "handler")
	cmd.Stdin = strings.NewReader(`{}`)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run bootstrap: %v", err)
	}

	var response struct {
		Headers    map[string][]string `json:"headers"`
		BodyBase64 string              `json:"body_base64"`
	}
	if err := json.Unmarshal(out, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := response.Headers["content-type"]; len(got) != 1 || got[0] != "application/json" {
		t.Fatalf("headers[content-type] = %#v, want [application/json]", got)
	}
	body, err := base64.StdEncoding.DecodeString(response.BodyBase64)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if string(body) != `["from helper"]` {
		t.Fatalf("body = %q, want helper response", string(body))
	}
}

func TestPythonBootstrapFailureExitsNonZero(t *testing.T) {
	if _, err := osexec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}

	modulePath := filepath.Join(t.TempDir(), "main.py")
	if err := os.WriteFile(modulePath, []byte(`
def handler(request):
    raise RuntimeError("boom")
`), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	cmd := osexec.Command("python3", "-c", pythonBootstrap, modulePath, "handler")
	cmd.Stdin = strings.NewReader(`{}`)
	out, err := cmd.Output()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	if len(out) != 0 {
		t.Fatalf("stdout = %q, want empty stdout on failure", string(out))
	}
}
