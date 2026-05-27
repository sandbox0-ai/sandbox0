package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"go.uber.org/zap"
)

func TestFunctionHandlerExecuteRunsRunnerWithInlineSource(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell runner fixture requires POSIX")
	}
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
module="$1"
if [ ! -s "$module" ]; then
  exit 2
fi
cat >/dev/null
printf '{"status":202,"headers":{"x-runner":["ok"]},"body_base64":"aGVsbG8="}\n'
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}

	handler := newFunctionHandler(functionHandlerConfig{
		runnerPath: runnerPath,
		cacheRoot:  filepath.Join(dir, "cache"),
	}, zap.NewNop())
	req := sandboxfunction.ExecuteRequest{
		ServiceID: "webhook",
		RouteID:   "root",
		Runtime:   sandboxfunction.RuntimePython,
		Handler:   sandboxfunction.DefaultHandler,
		Source: sandboxfunction.Source{
			Type: sandboxfunction.SourceTypeInline,
			Code: "def handler(request):\n    return {'status': 204}\n",
		},
		Request: sandboxfunction.HTTPRequest{
			Method: "POST",
			Path:   "/events",
		},
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	rec := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", bytes.NewReader(body))
	handler.Execute(rec, httpReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var envelope struct {
		Success bool                            `json:"success"`
		Data    sandboxfunction.ExecuteResponse `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !envelope.Success {
		t.Fatal("response success = false")
	}
	if envelope.Data.Status != http.StatusAccepted {
		t.Fatalf("function status = %d, want 202", envelope.Data.Status)
	}
	if got := envelope.Data.Headers["x-runner"]; len(got) != 1 || got[0] != "ok" {
		t.Fatalf("x-runner header = %#v, want [ok]", got)
	}
}

func TestFunctionHandlerRejectsMismatchedDigest(t *testing.T) {
	handler := newFunctionHandler(functionHandlerConfig{
		runnerPath: "/bin/false",
		cacheRoot:  t.TempDir(),
	}, zap.NewNop())
	req := sandboxfunction.ExecuteRequest{
		Runtime: sandboxfunction.RuntimePython,
		Handler: sandboxfunction.DefaultHandler,
		Source: sandboxfunction.Source{
			Type:   sandboxfunction.SourceTypeInline,
			Code:   "def handler(request):\n    return None\n",
			Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Request: sandboxfunction.HTTPRequest{
			Method: "POST",
			Path:   "/",
		},
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	rec := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", bytes.NewReader(body))
	handler.Execute(rec, httpReq)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var envelope spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != spec.CodeBadRequest {
		t.Fatalf("error = %#v, want bad_request", envelope.Error)
	}
}

func TestFunctionHandlerStreamRunsRunner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell runner fixture requires POSIX")
	}
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
cat >/dev/null
printf '{"type":"start","status":202,"headers":{"content-type":["text/event-stream"]}}\n'
printf '{"type":"chunk","body_base64":"ZGF0YTogb25lCgo="}\n'
printf '{"type":"chunk","body_base64":"ZGF0YTogdHdvCgo="}\n'
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}

	handler := newFunctionHandler(functionHandlerConfig{
		runnerPath: runnerPath,
		cacheRoot:  filepath.Join(dir, "cache"),
	}, zap.NewNop())
	req := sandboxfunction.ExecuteRequest{
		Runtime: sandboxfunction.RuntimePython,
		Handler: sandboxfunction.DefaultHandler,
		Source: sandboxfunction.Source{
			Type: sandboxfunction.SourceTypeInline,
			Code: "def handler(request):\n    return None\n",
		},
		Request: sandboxfunction.HTTPRequest{Method: "GET", Path: "/events"},
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	rec := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/functions/stream", bytes.NewReader(body))
	handler.Stream(rec, httpReq)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}
	if rec.Body.String() != "data: one\n\ndata: two\n\n" {
		t.Fatalf("body = %q, want SSE stream", rec.Body.String())
	}
}

func TestFunctionHandlerWebSocketRunsRunner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell runner fixture requires POSIX")
	}
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
read init
read message
printf '{"type":"message","message_type":"text","data":"echo"}\n'
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}

	handler := newFunctionHandler(functionHandlerConfig{
		runnerPath: runnerPath,
		cacheRoot:  filepath.Join(dir, "cache"),
	}, zap.NewNop())
	server := httptest.NewServer(http.HandlerFunc(handler.WebSocket))
	defer server.Close()

	req := sandboxfunction.ExecuteRequest{
		Runtime: sandboxfunction.RuntimePython,
		Handler: sandboxfunction.DefaultHandler,
		Source: sandboxfunction.Source{
			Type: sandboxfunction.SourceTypeInline,
			Code: "def handler(request):\n    return None\n",
		},
		Request: sandboxfunction.HTTPRequest{Method: "GET", Path: "/ws"},
	}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write init: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write message: %v", err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil && err != io.EOF {
		t.Fatalf("read message: %v", err)
	}
	if string(data) != "echo" {
		t.Fatalf("data = %q, want echo", string(data))
	}
}
