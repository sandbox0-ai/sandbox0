package main

import (
	"bufio"
	"bytes"
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

func TestPythonBootstrapStreamsGeneratorBody(t *testing.T) {
	if _, err := osexec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}

	modulePath := filepath.Join(t.TempDir(), "main.py")
	if err := os.WriteFile(modulePath, []byte(`
def handler(request):
    def events():
        yield "data: one\n\n"
        yield b"data: two\n\n"
    return {
        "status": 200,
        "headers": {"content-type": "text/event-stream"},
        "body": events(),
    }
`), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	cmd := osexec.Command("python3", "-c", pythonBootstrap, "--stream", modulePath, "handler")
	cmd.Stdin = strings.NewReader(`{"path":"/events"}`)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run bootstrap: %v", err)
	}

	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	var frames []map[string]any
	for scanner.Scan() {
		var frame map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		frames = append(frames, frame)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan frames: %v", err)
	}
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3: %s", len(frames), string(out))
	}
	if frames[0]["type"] != "start" {
		t.Fatalf("first frame = %#v, want start", frames[0])
	}
	chunk, err := base64.StdEncoding.DecodeString(frames[1]["body_base64"].(string))
	if err != nil {
		t.Fatalf("decode first chunk: %v", err)
	}
	if string(chunk) != "data: one\n\n" {
		t.Fatalf("first chunk = %q, want SSE data", string(chunk))
	}
}

func TestPythonBootstrapSupportsWebSocketHandler(t *testing.T) {
	if _, err := osexec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}

	modulePath := filepath.Join(t.TempDir(), "main.py")
	if err := os.WriteFile(modulePath, []byte(`
async def handler(request, ws):
    message = await ws.receive()
    await ws.send("echo:" + message)
`), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	cmd := osexec.Command("python3", "-c", pythonBootstrap, "--websocket", modulePath, "handler")
	cmd.Stdin = strings.NewReader(
		`{"path":"/ws"}` + "\n" +
			`{"type":"message","message_type":"text","data":"hello"}` + "\n",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run bootstrap: %v", err)
	}

	var frame struct {
		Type        string `json:"type"`
		MessageType string `json:"message_type"`
		Data        string `json:"data"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out), &frame); err != nil {
		t.Fatalf("decode frame: %v output=%s", err, string(out))
	}
	if frame.Type != "message" || frame.MessageType != "text" || frame.Data != "echo:hello" {
		t.Fatalf("frame = %+v, want echoed text message", frame)
	}
}
