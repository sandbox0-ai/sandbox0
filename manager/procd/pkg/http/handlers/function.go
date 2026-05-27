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

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"go.uber.org/zap"
)

const (
	defaultFunctionRunnerPath = "/procd/bin/python-runner"
	defaultFunctionCacheRoot  = "/tmp/sandbox0-functions"
	defaultFunctionTimeout    = 30 * time.Second
	maxFunctionTimeout        = 120 * time.Second
	maxFunctionExecuteBytes   = sandboxfunction.MaxHTTPRequestBytes + sandboxfunction.MaxInlineSourceBytes + (1 << 20)
	maxFunctionStdoutBytes    = 4 << 20
	maxFunctionStderrBytes    = 64 << 10
)

var (
	functionHandlerPattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	functionFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	functionDigestPattern   = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

type functionHandlerConfig struct {
	runnerPath     string
	cacheRoot      string
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

// FunctionHandler executes gateway-provided function source inside the sandbox.
type FunctionHandler struct {
	logger         *zap.Logger
	runnerPath     string
	cacheRoot      string
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

type functionHandlerRequest struct {
	ServiceID  string              `json:"service_id,omitempty"`
	RouteID    string              `json:"route_id,omitempty"`
	Method     string              `json:"method,omitempty"`
	Path       string              `json:"path,omitempty"`
	RawQuery   string              `json:"raw_query,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	BodyBase64 string              `json:"body_base64,omitempty"`
}

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
	if config.cacheRoot == "" {
		config.cacheRoot = defaultFunctionCacheRoot
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
		cacheRoot:      filepath.Clean(config.cacheRoot),
		defaultTimeout: config.defaultTimeout,
		maxTimeout:     config.maxTimeout,
	}
}

func (h *FunctionHandler) Execute(w http.ResponseWriter, r *http.Request) {
	req, err := decodeFunctionExecuteRequest(r.Body)
	if err != nil {
		if errors.Is(err, errFunctionRequestTooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "function execution request is too large")
			return
		}
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if err := validateFunctionExecuteRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
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
	timeout, err := h.requestTimeout(req.TimeoutMS)
	if err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	modulePath, err := h.materializeSource(req.Source)
	if err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	payload, err := json.Marshal(functionHandlerRequest{
		ServiceID:  req.ServiceID,
		RouteID:    req.RouteID,
		Method:     req.Request.Method,
		Path:       req.Request.Path,
		RawQuery:   req.Request.RawQuery,
		Headers:    req.Request.Headers,
		BodyBase64: req.Request.BodyBase64,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, spec.CodeInternal, "failed to encode function request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	stdout, stderr, truncated, err := h.run(ctx, modulePath, req.Handler, req.EnvVars, payload)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		writeError(w, http.StatusGatewayTimeout, spec.CodeUnavailable, "function execution timed out")
		return
	}
	if truncated.stdout {
		writeError(w, http.StatusBadGateway, spec.CodeUnavailable, "function response is too large")
		return
	}
	if err != nil {
		h.logger.Warn("Function execution failed",
			zap.String("service_id", req.ServiceID),
			zap.String("route_id", req.RouteID),
			zap.String("handler", req.Handler),
			zap.String("stderr", trimLoggedStderr(stderr, truncated.stderr)),
			zap.Error(err),
		)
		writeError(w, http.StatusInternalServerError, "function_failed", "function execution failed")
		return
	}

	response, err := decodeFunctionExecuteResponse(stdout)
	if err != nil {
		writeError(w, http.StatusBadGateway, spec.CodeUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
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

func (h *FunctionHandler) materializeSource(source sandboxfunction.Source) (string, error) {
	digest := source.Digest
	if digest == "" {
		digest = sandboxfunction.InlineDigest(source.Filename, source.Code)
	}
	if !functionDigestPattern.MatchString(digest) {
		return "", errors.New("source.digest must be a sha256 digest")
	}
	expected := sandboxfunction.InlineDigest(source.Filename, source.Code)
	if source.Code != "" && digest != expected {
		return "", errors.New("source.digest does not match source code")
	}
	dirName := strings.TrimPrefix(digest, "sha256:")
	dir := filepath.Join(h.cacheRoot, dirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("prepare function source cache: %w", err)
	}
	modulePath := filepath.Join(dir, source.Filename)
	rel, err := filepath.Rel(dir, modulePath)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", errors.New("source.filename escapes source cache")
	}
	if source.Code != "" {
		if err := os.WriteFile(modulePath, []byte(source.Code), 0o600); err != nil {
			return "", fmt.Errorf("write function source: %w", err)
		}
	}
	return modulePath, nil
}

func (h *FunctionHandler) run(ctx context.Context, modulePath, handler string, envVars map[string]string, payload []byte) ([]byte, string, functionRunTruncation, error) {
	cmd := osexec.CommandContext(ctx, h.runnerPath, modulePath, handler)
	cmd.Dir = filepath.Dir(modulePath)
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Env = mergeFunctionEnv(os.Environ(), envVars)
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

var errFunctionRequestTooLarge = errors.New("function execution request too large")

func decodeFunctionExecuteRequest(body io.Reader) (sandboxfunction.ExecuteRequest, error) {
	limited := io.LimitReader(body, maxFunctionExecuteBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return sandboxfunction.ExecuteRequest{}, err
	}
	if len(data) > maxFunctionExecuteBytes {
		return sandboxfunction.ExecuteRequest{}, errFunctionRequestTooLarge
	}
	var req sandboxfunction.ExecuteRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return sandboxfunction.ExecuteRequest{}, fmt.Errorf("invalid request body: %w", err)
	}
	return req, nil
}

func validateFunctionExecuteRequest(req sandboxfunction.ExecuteRequest) error {
	if req.Runtime != sandboxfunction.RuntimePython {
		return fmt.Errorf("runtime must be %q", sandboxfunction.RuntimePython)
	}
	if !functionHandlerPattern.MatchString(req.Handler) {
		return fmt.Errorf("handler must match %s", functionHandlerPattern.String())
	}
	if req.Source.Type != sandboxfunction.SourceTypeInline {
		return fmt.Errorf("source.type must be %q", sandboxfunction.SourceTypeInline)
	}
	if req.Source.Filename == "" {
		return errors.New("source.filename is required")
	}
	if strings.Contains(req.Source.Filename, "/") || strings.Contains(req.Source.Filename, "\\") || strings.HasPrefix(req.Source.Filename, ".") {
		return errors.New("source.filename must be a relative file name")
	}
	if !functionFilenamePattern.MatchString(req.Source.Filename) {
		return errors.New("source.filename contains unsupported characters")
	}
	if strings.TrimSpace(req.Source.Code) == "" {
		return errors.New("source.code is required")
	}
	if len([]byte(req.Source.Code)) > sandboxfunction.MaxInlineSourceBytes {
		return fmt.Errorf("source.code exceeds limit %d bytes", sandboxfunction.MaxInlineSourceBytes)
	}
	if req.Request.Path == "" {
		return errors.New("request.path is required")
	}
	if !strings.HasPrefix(req.Request.Path, "/") {
		return errors.New("request.path must start with /")
	}
	if req.Request.BodyBase64 != "" {
		if _, err := base64.StdEncoding.DecodeString(req.Request.BodyBase64); err != nil {
			return errors.New("request.body_base64 must be valid base64")
		}
	}
	return nil
}

func decodeFunctionExecuteResponse(data []byte) (sandboxfunction.ExecuteResponse, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return sandboxfunction.ExecuteResponse{}, errors.New("function returned an empty response")
	}

	var response sandboxfunction.ExecuteResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return sandboxfunction.ExecuteResponse{}, fmt.Errorf("function returned invalid JSON: %w", err)
	}
	if response.Status == 0 {
		response.Status = http.StatusOK
	}
	if response.Status < 100 || response.Status > 599 {
		return sandboxfunction.ExecuteResponse{}, errors.New("function response status must be between 100 and 599")
	}
	if response.BodyBase64 != "" {
		if _, err := base64.StdEncoding.DecodeString(response.BodyBase64); err != nil {
			return sandboxfunction.ExecuteResponse{}, errors.New("function response body_base64 must be valid base64")
		}
	}
	if response.Headers == nil {
		response.Headers = map[string][]string{}
	}
	return response, nil
}

func mergeFunctionEnv(base []string, envVars map[string]string) []string {
	if len(envVars) == 0 {
		return base
	}
	out := make([]string, 0, len(base)+len(envVars))
	overrides := make(map[string]string, len(envVars))
	for key, value := range envVars {
		key = strings.TrimSpace(key)
		if key == "" || strings.Contains(key, "=") {
			continue
		}
		overrides[key] = value
	}
	for _, item := range base {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		if value, exists := overrides[key]; exists {
			out = append(out, key+"="+value)
			delete(overrides, key)
			continue
		}
		out = append(out, item)
	}
	for key, value := range overrides {
		out = append(out, key+"="+value)
	}
	return out
}

func trimLoggedStderr(stderr string, truncated bool) string {
	stderr = strings.TrimSpace(stderr)
	if truncated {
		stderr += "\n[stderr truncated]"
	}
	return stderr
}

type functionRunTruncation struct {
	stdout bool
	stderr bool
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
