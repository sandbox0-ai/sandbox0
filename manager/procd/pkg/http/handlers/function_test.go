package handlers

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func TestFunctionInvokeRunsConfiguredRunner(t *testing.T) {
	root := filepath.Join(t.TempDir(), "functions")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}
	modulePath := filepath.Join(root, "main.py")
	if err := os.WriteFile(modulePath, []byte("def handler(request):\n    return None\n"), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	runnerPath := filepath.Join(t.TempDir(), "python-runner")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
if [ "$1" != "$FUNCTION_MODULE" ]; then
  echo "unexpected module: $1" >&2
  exit 7
fi
if [ "$2" != "custom_handler" ]; then
  echo "unexpected handler: $2" >&2
  exit 8
fi
cat >/dev/null
printf '{"status":202,"headers":{"x-runner":["ok"]},"body_base64":"b2s="}\n'
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}
	t.Setenv("FUNCTION_MODULE", modulePath)

	handler := newTestFunctionHandler(root, runnerPath)
	rec := invokeFunction(t, handler, "main", `{"path":"/hello","handler":"custom_handler"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	response, apiErr, err := spec.DecodeResponse[FunctionInvokeResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("api error: %v", apiErr)
	}
	if response.Status != 202 {
		t.Fatalf("function status = %d, want 202", response.Status)
	}
	if got := response.Headers["x-runner"]; len(got) != 1 || got[0] != "ok" {
		t.Fatalf("x-runner = %#v, want [ok]", got)
	}
	if response.BodyBase64 != "b2s=" {
		t.Fatalf("body_base64 = %q, want b2s=", response.BodyBase64)
	}
}

func TestFunctionInvokeRejectsInvalidFunctionName(t *testing.T) {
	handler := newTestFunctionHandler(t.TempDir(), filepath.Join(t.TempDir(), "python-runner"))
	rec := invokeFunction(t, handler, "main.py", `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestFunctionInvokeReturnsRunnerFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "functions")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "main.py"), []byte("def handler(request):\n    return None\n"), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}

	runnerPath := filepath.Join(t.TempDir(), "python-runner")
	if err := os.WriteFile(runnerPath, []byte(`#!/bin/sh
echo "boom" >&2
exit 42
`), 0o755); err != nil {
		t.Fatalf("write runner: %v", err)
	}

	handler := newTestFunctionHandler(root, runnerPath)
	rec := invokeFunction(t, handler, "main", `{}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("body = %s, want runner stderr", rec.Body.String())
	}
}

func TestDecodeFunctionInvokeResponseDefaultsStatus(t *testing.T) {
	response, err := decodeFunctionInvokeResponse([]byte(`{"body_base64":"b2s="}`))
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Status)
	}
}

func newTestFunctionHandler(root, runner string) *FunctionHandler {
	return newFunctionHandler(functionHandlerConfig{
		functionRoot:   root,
		runnerPath:     runner,
		defaultTimeout: time.Second,
		maxTimeout:     time.Second,
	}, zap.NewNop())
}

func invokeFunction(t *testing.T, handler *FunctionHandler, name string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/functions/"+name+"/invoke", strings.NewReader(body))
	rec := httptest.NewRecorder()
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/functions/{name}/invoke", handler.Invoke).Methods(http.MethodPost)
	router.ServeHTTP(rec, req)
	return rec
}
