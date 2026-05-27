package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	osexec "os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

const (
	defaultFunctionRoot       = "/workspace/functions"
	defaultFunctionRunnerPath = "/procd/runtimes/python-runner"
	defaultFunctionTimeout    = 30 * time.Second
	maxFunctionTimeout        = 120 * time.Second
	maxFunctionRequestBytes   = 8 << 20
	maxFunctionStdoutBytes    = 4 << 20
	maxFunctionStderrBytes    = 64 << 10
)

var (
	functionNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	functionHandlerPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

type functionHandlerConfig struct {
	runnerPath     string
	functionRoot   string
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

// FunctionHandler invokes sandbox functions through platform-provided runtimes.
type FunctionHandler struct {
	logger         *zap.Logger
	runnerPath     string
	functionRoot   string
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

// FunctionInvokeRequest is passed to the sandbox function handler.
type FunctionInvokeRequest struct {
	Method     string              `json:"method,omitempty"`
	Path       string              `json:"path,omitempty"`
	Query      map[string][]string `json:"query,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	BodyBase64 string              `json:"body_base64,omitempty"`
	Handler    string              `json:"handler,omitempty"`
	TimeoutMS  int                 `json:"timeout_ms,omitempty"`
}

// FunctionInvokeResponse is returned by the sandbox function handler.
type FunctionInvokeResponse struct {
	Status     int                 `json:"status"`
	Headers    map[string][]string `json:"headers,omitempty"`
	BodyBase64 string              `json:"body_base64,omitempty"`
}

// NewFunctionHandler creates a function invocation handler.
func NewFunctionHandler(logger *zap.Logger) *FunctionHandler {
	return newFunctionHandler(functionHandlerConfig{}, logger)
}

func newFunctionHandler(config functionHandlerConfig, logger *zap.Logger) *FunctionHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	if config.runnerPath == "" {
		config.runnerPath = defaultFunctionRunnerPath
	}
	if config.functionRoot == "" {
		config.functionRoot = defaultFunctionRoot
	}
	if config.defaultTimeout <= 0 {
		config.defaultTimeout = defaultFunctionTimeout
	}
	if config.maxTimeout <= 0 {
		config.maxTimeout = maxFunctionTimeout
	}
	return &FunctionHandler{
		logger:         logger,
		runnerPath:     config.runnerPath,
		functionRoot:   filepath.Clean(config.functionRoot),
		defaultTimeout: config.defaultTimeout,
		maxTimeout:     config.maxTimeout,
	}
}

// Invoke executes /workspace/functions/{name}.py with the requested handler.
func (h *FunctionHandler) Invoke(w http.ResponseWriter, r *http.Request) {
	functionName := mux.Vars(r)["name"]
	modulePath, err := h.modulePath(functionName)
	if err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if _, err := os.Stat(modulePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusNotFound, spec.CodeNotFound, "function not found")
			return
		}
		writeError(w, http.StatusInternalServerError, spec.CodeInternal, "function module unavailable")
		return
	}
	if _, err := os.Stat(h.runnerPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "function runtime unavailable")
			return
		}
		writeError(w, http.StatusInternalServerError, spec.CodeInternal, "function runtime unavailable")
		return
	}

	req, err := decodeFunctionInvokeRequest(r.Body)
	if err != nil {
		if errors.Is(err, errFunctionRequestTooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "function request is too large")
			return
		}
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	h.applyRequestDefaults(&req)
	if err := validateFunctionRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	timeout, err := h.requestTimeout(req.TimeoutMS)
	if err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	payload, err := json.Marshal(req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, spec.CodeInternal, "failed to encode function request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	stdout, stderr, truncated, err := h.run(ctx, modulePath, req.Handler, payload)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		writeError(w, http.StatusGatewayTimeout, spec.CodeUnavailable, "function invocation timed out")
		return
	}
	if truncated.stdout {
		writeError(w, http.StatusBadGateway, spec.CodeUnavailable, "function response is too large")
		return
	}
	if err != nil {
		message := strings.TrimSpace(stderr)
		if message == "" {
			message = err.Error()
		}
		if truncated.stderr {
			message += "\n[stderr truncated]"
		}
		h.logger.Warn("Function invocation failed",
			zap.String("function", functionName),
			zap.String("handler", req.Handler),
			zap.Error(err),
		)
		writeError(w, http.StatusInternalServerError, "function_failed", message)
		return
	}

	response, err := decodeFunctionInvokeResponse(stdout)
	if err != nil {
		writeError(w, http.StatusBadGateway, spec.CodeUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *FunctionHandler) modulePath(name string) (string, error) {
	if !functionNamePattern.MatchString(name) {
		return "", fmt.Errorf("function name must match %s", functionNamePattern.String())
	}
	root := filepath.Clean(h.functionRoot)
	modulePath := filepath.Join(root, name+".py")
	rel, err := filepath.Rel(root, modulePath)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", errors.New("function path escapes function root")
	}
	return modulePath, nil
}

func (h *FunctionHandler) applyRequestDefaults(req *FunctionInvokeRequest) {
	req.Method = strings.ToUpper(strings.TrimSpace(req.Method))
	if req.Method == "" {
		req.Method = http.MethodPost
	}
	req.Path = strings.TrimSpace(req.Path)
	if req.Path == "" {
		req.Path = "/"
	}
	req.Handler = strings.TrimSpace(req.Handler)
	if req.Handler == "" {
		req.Handler = "handler"
	}
}

func (h *FunctionHandler) requestTimeout(timeoutMS int) (time.Duration, error) {
	if timeoutMS == 0 {
		return h.defaultTimeout, nil
	}
	if timeoutMS < 0 {
		return 0, errors.New("timeout_ms must be >= 0")
	}
	timeout := time.Duration(timeoutMS) * time.Millisecond
	if timeout > h.maxTimeout {
		return 0, fmt.Errorf("timeout_ms must be <= %d", int(h.maxTimeout/time.Millisecond))
	}
	return timeout, nil
}

type functionRunTruncation struct {
	stdout bool
	stderr bool
}

func (h *FunctionHandler) run(ctx context.Context, modulePath, handler string, payload []byte) ([]byte, string, functionRunTruncation, error) {
	cmd := osexec.CommandContext(ctx, h.runnerPath, modulePath, handler)
	cmd.Dir = filepath.Dir(h.functionRoot)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = 5 * time.Second

	stdout := newLimitedBuffer(maxFunctionStdoutBytes)
	stderr := newLimitedBuffer(maxFunctionStderrBytes)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	return stdout.Bytes(), stderr.String(), functionRunTruncation{
		stdout: stdout.Truncated(),
		stderr: stderr.Truncated(),
	}, err
}

var errFunctionRequestTooLarge = errors.New("function request too large")

func decodeFunctionInvokeRequest(body io.Reader) (FunctionInvokeRequest, error) {
	limited := io.LimitReader(body, maxFunctionRequestBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return FunctionInvokeRequest{}, err
	}
	if len(data) > maxFunctionRequestBytes {
		return FunctionInvokeRequest{}, errFunctionRequestTooLarge
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return FunctionInvokeRequest{}, nil
	}

	var req FunctionInvokeRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return FunctionInvokeRequest{}, fmt.Errorf("invalid request body: %w", err)
	}
	return req, nil
}

func validateFunctionRequest(req FunctionInvokeRequest) error {
	if req.BodyBase64 != "" {
		if _, err := base64.StdEncoding.DecodeString(req.BodyBase64); err != nil {
			return errors.New("body_base64 must be valid base64")
		}
	}
	if !strings.HasPrefix(req.Path, "/") {
		return errors.New("path must start with /")
	}
	if !functionHandlerPattern.MatchString(req.Handler) {
		return fmt.Errorf("handler must match %s", functionHandlerPattern.String())
	}
	return nil
}

func decodeFunctionInvokeResponse(data []byte) (FunctionInvokeResponse, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return FunctionInvokeResponse{}, errors.New("function returned an empty response")
	}

	var response FunctionInvokeResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return FunctionInvokeResponse{}, fmt.Errorf("function returned invalid JSON: %w", err)
	}
	if response.Status == 0 {
		response.Status = http.StatusOK
	}
	if response.Status < 100 || response.Status > 599 {
		return FunctionInvokeResponse{}, errors.New("function response status must be between 100 and 599")
	}
	if response.BodyBase64 != "" {
		if _, err := base64.StdEncoding.DecodeString(response.BodyBase64); err != nil {
			return FunctionInvokeResponse{}, errors.New("function response body_base64 must be valid base64")
		}
	}
	if response.Headers == nil {
		response.Headers = map[string][]string{}
	}
	return response, nil
}

type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func newLimitedBuffer(limit int) *limitedBuffer {
	return &limitedBuffer{limit: limit}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) <= remaining {
			_, _ = b.buf.Write(p)
		} else {
			_, _ = b.buf.Write(p[:remaining])
			b.truncated = true
		}
	} else if len(p) > 0 {
		b.truncated = true
	}
	return len(p), nil
}

func (b *limitedBuffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *limitedBuffer) String() string {
	return b.buf.String()
}

func (b *limitedBuffer) Truncated() bool {
	return b.truncated
}
