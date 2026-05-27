package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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
			Type:     sandboxfunction.SourceTypeInline,
			Filename: "main.py",
			Code:     "def handler(request):\n    return {'status': 204}\n",
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
			Type:     sandboxfunction.SourceTypeInline,
			Filename: "main.py",
			Code:     "def handler(request):\n    return None\n",
			Digest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
