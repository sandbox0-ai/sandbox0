package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"go.uber.org/zap"
)

func TestFunctionHandlerExecuteMissingRunnerReturnsRuntimeUnavailable(t *testing.T) {
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner")
	handler := newFunctionHandler(functionHandlerConfig{runnerPath: runnerPath}, zap.NewNop())

	req := sandboxfunction.ExecuteRequest{
		Runtime: sandboxfunction.RuntimePython,
		Handler: sandboxfunction.DefaultHandler,
		Source: sandboxfunction.Source{
			Type: sandboxfunction.SourceTypeInline,
			Code: "def handler(request):\n    return {'status': 201}\n",
		},
		Request: sandboxfunction.HTTPRequest{Method: "POST", Path: "/events"},
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	rec := httptest.NewRecorder()
	handler.Execute(rec, httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", bytes.NewReader(body)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var envelope spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != spec.CodeUnavailable || envelope.Error.Message != "function runtime unavailable" {
		t.Fatalf("error = %#v, want runtime unavailable", envelope.Error)
	}
}

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

func TestFunctionHandlerExecuteMergesSandboxEnvVars(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell runner fixture requires POSIX")
	}
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
cat >/dev/null
if [ "$SANDBOX_ENV" != "sandbox" ]; then
  echo "missing sandbox env" >&2
  exit 3
fi
if [ "$OVERRIDE_ENV" != "request" ]; then
  echo "env precedence failed" >&2
  exit 4
fi
printf '{"status":200,"body_base64":"b2s="}\n'
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}

	handler := newFunctionHandler(functionHandlerConfig{
		runnerPath: runnerPath,
		cacheRoot:  filepath.Join(dir, "cache"),
	}, zap.NewNop())
	handler.SetSandboxEnvVarsProvider(func() map[string]string {
		return map[string]string{
			"SANDBOX_ENV":  "sandbox",
			"OVERRIDE_ENV": "sandbox",
		}
	})
	req := sandboxfunction.ExecuteRequest{
		Runtime: sandboxfunction.RuntimePython,
		Handler: sandboxfunction.DefaultHandler,
		EnvVars: map[string]string{
			"OVERRIDE_ENV": "request",
		},
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
		t.Fatalf("response success = false: %s", rec.Body.String())
	}
	if envelope.Data.BodyBase64 != "b2s=" {
		t.Fatalf("body_base64 = %q, want b2s=", envelope.Data.BodyBase64)
	}
}

func TestFunctionHandlerEnforcesFunctionMaxConcurrency(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell runner fixture requires POSIX")
	}
	dir := t.TempDir()
	startedPath := filepath.Join(dir, "started")
	releasePath := filepath.Join(dir, "release")
	runnerPath := filepath.Join(dir, "runner.sh")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
cat >/dev/null
touch "$RUNNER_STARTED"
while [ ! -f "$RUNNER_RELEASE" ]; do
  sleep 0.05
done
printf '{"status":200,"body_base64":"b2s="}\n'
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}

	handler := newFunctionHandler(functionHandlerConfig{
		runnerPath: runnerPath,
		cacheRoot:  filepath.Join(dir, "cache"),
	}, zap.NewNop())
	functionReq := sandboxfunction.ExecuteRequest{
		ServiceID:      "webhook",
		Runtime:        sandboxfunction.RuntimePython,
		Handler:        sandboxfunction.DefaultHandler,
		MaxConcurrency: 1,
		EnvVars: map[string]string{
			"RUNNER_STARTED": startedPath,
			"RUNNER_RELEASE": releasePath,
		},
		Source: sandboxfunction.Source{
			Type: sandboxfunction.SourceTypeInline,
			Code: "def handler(request):\n    return None\n",
		},
		Request: sandboxfunction.HTTPRequest{
			Method: "POST",
			Path:   "/events",
		},
	}
	body, err := json.Marshal(functionReq)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	firstDone := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.Execute(rec, httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", bytes.NewReader(body)))
		firstDone <- rec.Code
	}()
	waitForFile(t, startedPath)

	rec := httptest.NewRecorder()
	handler.Execute(rec, httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", bytes.NewReader(body)))
	assertFunctionConcurrencyExceeded(t, rec.Code, rec.Body.Bytes())

	rec = httptest.NewRecorder()
	handler.Stream(rec, httptest.NewRequest(http.MethodPost, "/api/v1/functions/stream", bytes.NewReader(body)))
	assertFunctionConcurrencyExceeded(t, rec.Code, rec.Body.Bytes())

	server := httptest.NewServer(http.HandlerFunc(handler.WebSocket))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	if err := conn.WriteJSON(functionReq); err != nil {
		t.Fatalf("write websocket init: %v", err)
	}
	_, _, err = conn.ReadMessage()
	_ = conn.Close()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("websocket read error = %v, want close error", err)
	}
	if closeErr.Code != websocket.CloseTryAgainLater || closeErr.Text != functionConcurrencyExceededMessage {
		t.Fatalf("websocket close error = %#v, want concurrency exceeded", closeErr)
	}

	if err := os.WriteFile(releasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("write release file: %v", err)
	}
	select {
	case code := <-firstDone:
		if code != http.StatusOK {
			t.Fatalf("first status = %d, want 200", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first function execution")
	}

	rec = httptest.NewRecorder()
	handler.Execute(rec, httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status after release = %d body=%s, want 200", rec.Code, rec.Body.String())
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

func assertFunctionConcurrencyExceeded(t *testing.T, status int, body []byte) {
	t.Helper()
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d body=%s, want 429", status, string(body))
	}
	var envelope spec.Response
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != functionConcurrencyExceededCode || envelope.Error.Message != functionConcurrencyExceededMessage {
		t.Fatalf("error = %#v, want concurrency exceeded", envelope.Error)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for file %s", path)
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

func TestFunctionHandlerStreamMissingRunnerReturnsRuntimeUnavailable(t *testing.T) {
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner")
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

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var envelope spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != spec.CodeUnavailable || envelope.Error.Message != "function runtime unavailable" {
		t.Fatalf("error = %#v, want runtime unavailable", envelope.Error)
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

func TestFunctionHandlerWebSocketMissingRunnerClosesRuntimeUnavailable(t *testing.T) {
	dir := t.TempDir()
	runnerPath := filepath.Join(dir, "runner")
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
	_, _, err = conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("read error = %v, want close error", err)
	}
	if closeErr.Code != websocket.CloseInternalServerErr || closeErr.Text != "function runtime unavailable" {
		t.Fatalf("close error = %#v, want runtime unavailable", closeErr)
	}
}
