package handlers

import (
	"bufio"
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
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"go.uber.org/zap"
)

const (
	defaultFunctionRunnerPath = "/procd-image/usr/local/bin/python-runner"
	defaultFunctionCacheRoot  = "/tmp/sandbox0-functions"
	defaultFunctionTimeout    = 30 * time.Second
	maxFunctionTimeout        = 120 * time.Second
	maxFunctionExecuteBytes   = sandboxfunction.MaxHTTPRequestBytes + sandboxfunction.MaxInlineSourceBytes + (1 << 20)
	maxFunctionStdoutBytes    = 4 << 20
	maxFunctionStderrBytes    = 64 << 10
	maxFunctionStreamFrame    = 4 << 20

	functionConcurrencyExceededCode    = "function_concurrency_exceeded"
	functionConcurrencyExceededMessage = "function concurrency limit exceeded"
)

var (
	functionHandlerPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	functionDigestPattern  = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

type functionHandlerConfig struct {
	runnerPath     string
	cacheRoot      string
	defaultTimeout time.Duration
	maxTimeout     time.Duration
}

// FunctionHandler executes gateway-provided function source inside the sandbox.
type FunctionHandler struct {
	logger             *zap.Logger
	runnerPath         string
	cacheRoot          string
	defaultTimeout     time.Duration
	maxTimeout         time.Duration
	upgrader           websocket.Upgrader
	sandboxEnvProvider func() map[string]string
	concurrency        *functionConcurrencyLimiter
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
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		concurrency: newFunctionConcurrencyLimiter(),
	}
}

// SetSandboxEnvVarsProvider sets the provider for sandbox-level default environment variables.
func (h *FunctionHandler) SetSandboxEnvVarsProvider(provider func() map[string]string) {
	h.sandboxEnvProvider = provider
}

func (h *FunctionHandler) sandboxEnvVars() map[string]string {
	if h.sandboxEnvProvider == nil {
		return nil
	}
	return h.sandboxEnvProvider()
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
	timeout, err := h.requestTimeout(req.TimeoutMS)
	if err != nil {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	release, ok := h.acquireFunctionSlot(req)
	if !ok {
		writeError(w, http.StatusTooManyRequests, functionConcurrencyExceededCode, functionConcurrencyExceededMessage)
		return
	}
	defer release()
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

	stdout, stderr, truncated, err := h.run(ctx, modulePath, req.Handler, req.EnvVars, payload)
	if isFunctionRunnerStartError(err) {
		h.writeRunnerStartError(w, err)
		return
	}
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

func (h *FunctionHandler) Stream(w http.ResponseWriter, r *http.Request) {
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
	ctx := r.Context()
	if req.TimeoutMS < 0 {
		writeError(w, http.StatusBadRequest, spec.CodeBadRequest, "timeout_ms must be >= 0")
		return
	}
	if req.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	release, ok := h.acquireFunctionSlot(req)
	if !ok {
		writeError(w, http.StatusTooManyRequests, functionConcurrencyExceededCode, functionConcurrencyExceededMessage)
		return
	}
	defer release()
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
	if err := proxy.DisableResponseWriteDeadline(w); err != nil {
		h.logger.Debug("Failed to disable function stream response deadline", zap.Error(err))
	}
	tracker := &trackingResponseWriter{ResponseWriter: w}
	if err := h.runStream(ctx, tracker, modulePath, req.Handler, req.EnvVars, payload); err != nil {
		if isFunctionRunnerStartError(err) {
			if !tracker.written {
				h.writeRunnerStartError(w, err)
			}
			return
		}
		if !tracker.written {
			writeError(w, http.StatusInternalServerError, "function_failed", "function stream failed")
		}
		h.logger.Warn("Function stream failed",
			zap.String("service_id", req.ServiceID),
			zap.String("route_id", req.RouteID),
			zap.String("handler", req.Handler),
			zap.Error(err),
		)
	}
}

func (h *FunctionHandler) WebSocket(w http.ResponseWriter, r *http.Request) {
	if err := proxy.DisableResponseDeadlines(w); err != nil {
		h.logger.Debug("Failed to disable function websocket response deadlines", zap.Error(err))
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("Function websocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()
	if err := proxy.DisableConnectionDeadlines(conn.UnderlyingConn()); err != nil {
		h.logger.Debug("Failed to clear function websocket connection deadlines", zap.Error(err))
	}

	_, initData, err := conn.ReadMessage()
	if err != nil {
		return
	}
	var req sandboxfunction.ExecuteRequest
	if err := json.Unmarshal(initData, &req); err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "invalid function request"), time.Now().Add(time.Second))
		return
	}
	if err := validateFunctionExecuteRequest(req); err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(time.Second))
		return
	}
	release, ok := h.acquireFunctionSlot(req)
	if !ok {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, functionConcurrencyExceededMessage), time.Now().Add(time.Second))
		return
	}
	defer release()
	modulePath, err := h.materializeSource(req.Source)
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(time.Second))
		return
	}
	payload, err := json.Marshal(functionHandlerRequest{
		ServiceID: req.ServiceID,
		RouteID:   req.RouteID,
		Method:    req.Request.Method,
		Path:      req.Request.Path,
		RawQuery:  req.Request.RawQuery,
		Headers:   req.Request.Headers,
	})
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "failed to encode function request"), time.Now().Add(time.Second))
		return
	}
	if err := h.runWebSocket(r.Context(), conn, modulePath, req.Handler, req.EnvVars, payload); err != nil {
		if isFunctionRunnerStartError(err) {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "function runtime unavailable"), time.Now().Add(time.Second))
			return
		}
		h.logger.Warn("Function websocket failed",
			zap.String("service_id", req.ServiceID),
			zap.String("route_id", req.RouteID),
			zap.String("handler", req.Handler),
			zap.Error(err),
		)
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

func (h *FunctionHandler) materializeSource(source sandboxfunction.Source) (string, error) {
	digest := source.Digest
	if digest == "" {
		digest = sandboxfunction.InlineDigest(source.Code)
	}
	if !functionDigestPattern.MatchString(digest) {
		return "", errors.New("source.digest must be a sha256 digest")
	}
	expected := sandboxfunction.InlineDigest(source.Code)
	if source.Code != "" && digest != expected && digest != sandboxfunction.LegacyInlineDigest(source.Filename, source.Code) {
		return "", errors.New("source.digest does not match source code")
	}
	dirName := strings.TrimPrefix(digest, "sha256:")
	dir := filepath.Join(h.cacheRoot, dirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("prepare function source cache: %w", err)
	}
	modulePath := filepath.Join(dir, sandboxfunction.DefaultFilename)
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
	cmd.Env = process.MergeEnvironment(os.Environ(), h.sandboxEnvVars(), envVars)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = killFunctionProcessGroup(cmd)
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

func killFunctionProcessGroup(cmd *osexec.Cmd) func() error {
	return func() error {
		if cmd.Process == nil {
			return nil
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
}

func (h *FunctionHandler) runStream(ctx context.Context, w http.ResponseWriter, modulePath, handler string, envVars map[string]string, payload []byte) error {
	cmd := osexec.CommandContext(ctx, h.runnerPath, "--stream", modulePath, handler)
	cmd.Dir = filepath.Dir(modulePath)
	cmd.Env = process.MergeEnvironment(os.Environ(), h.sandboxEnvVars(), envVars)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = killFunctionProcessGroup(cmd)
	cmd.WaitDelay = 5 * time.Second

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	_, _ = stdin.Write(payload)
	_, _ = stdin.Write([]byte("\n"))
	_ = stdin.Close()

	stderrBuf := newLimitedBuffer(maxFunctionStderrBytes)
	stderrDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(stderrBuf, stderr)
		close(stderrDone)
	}()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), maxFunctionStreamFrame)
	flusher, _ := w.(http.Flusher)
	started := false
	for scanner.Scan() {
		var frame sandboxfunction.StreamFrame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			_ = cmd.Process.Kill()
			<-stderrDone
			return fmt.Errorf("decode stream frame: %w", err)
		}
		switch frame.Type {
		case sandboxfunction.StreamFrameStart:
			if started {
				continue
			}
			for key, values := range frame.Headers {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			status := frame.Status
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			if flusher != nil {
				flusher.Flush()
			}
			started = true
		case sandboxfunction.StreamFrameChunk:
			if !started {
				w.WriteHeader(http.StatusOK)
				started = true
			}
			chunk, err := base64.StdEncoding.DecodeString(frame.BodyBase64)
			if err != nil {
				_ = cmd.Process.Kill()
				<-stderrDone
				return errors.New("function stream chunk body_base64 must be valid base64")
			}
			if len(chunk) > 0 {
				if _, err := w.Write(chunk); err != nil {
					_ = cmd.Process.Kill()
					<-stderrDone
					return err
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		case sandboxfunction.StreamFrameError:
			_ = cmd.Process.Kill()
			<-stderrDone
			if frame.Error != "" {
				return errors.New(frame.Error)
			}
			return errors.New("function stream failed")
		default:
			_ = cmd.Process.Kill()
			<-stderrDone
			return fmt.Errorf("unsupported stream frame type %q", frame.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		<-stderrDone
		return err
	}
	err = cmd.Wait()
	<-stderrDone
	if err != nil {
		return fmt.Errorf("function stream process failed: %w: %s", err, trimLoggedStderr(stderrBuf.String(), stderrBuf.Truncated()))
	}
	return nil
}

func (h *FunctionHandler) runWebSocket(ctx context.Context, conn *websocket.Conn, modulePath, handler string, envVars map[string]string, payload []byte) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := osexec.CommandContext(ctx, h.runnerPath, "--websocket", modulePath, handler)
	cmd.Dir = filepath.Dir(modulePath)
	cmd.Env = process.MergeEnvironment(os.Environ(), h.sandboxEnvVars(), envVars)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = killFunctionProcessGroup(cmd)
	cmd.WaitDelay = 5 * time.Second

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	_, _ = stdin.Write(payload)
	_, _ = stdin.Write([]byte("\n"))

	stderrBuf := newLimitedBuffer(maxFunctionStderrBytes)
	go func() { _, _ = io.Copy(stderrBuf, stderr) }()

	var writeMu sync.Mutex
	closeBoth := sync.OnceFunc(func() {
		cancel()
		_ = stdin.Close()
		_ = conn.Close()
	})
	errCh := make(chan functionWebSocketResult, 2)

	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64*1024), maxFunctionStreamFrame)
		for scanner.Scan() {
			var frame sandboxfunction.WebSocketFrame
			if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
				errCh <- functionWebSocketResult{err: fmt.Errorf("decode websocket frame: %w", err)}
				return
			}
			switch frame.Type {
			case sandboxfunction.WebSocketFrameMessage:
				messageType, data, err := decodeWebSocketRunnerMessage(frame)
				if err != nil {
					errCh <- functionWebSocketResult{err: err}
					return
				}
				writeMu.Lock()
				err = conn.WriteMessage(messageType, data)
				writeMu.Unlock()
				if err != nil {
					errCh <- functionWebSocketResult{err: err}
					return
				}
			case sandboxfunction.WebSocketFrameClose:
				writeMu.Lock()
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, frame.Reason), time.Now().Add(time.Second))
				writeMu.Unlock()
				errCh <- functionWebSocketResult{}
				return
			default:
				errCh <- functionWebSocketResult{err: fmt.Errorf("unsupported websocket frame type %q", frame.Type)}
				return
			}
		}
		errCh <- functionWebSocketResult{err: scanner.Err(), processExited: true}
	}()

	go func() {
		encoder := json.NewEncoder(stdin)
		for {
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				_ = encoder.Encode(sandboxfunction.WebSocketFrame{Type: sandboxfunction.WebSocketFrameClose})
				errCh <- functionWebSocketResult{}
				return
			}
			frame := encodeWebSocketClientMessage(messageType, data)
			if err := encoder.Encode(frame); err != nil {
				errCh <- functionWebSocketResult{err: err}
				return
			}
		}
	}()

	result := <-errCh
	err = result.err
	if result.processExited {
		waitErr := cmd.Wait()
		if err == nil {
			err = waitErr
		}
		closeBoth()
	} else {
		closeBoth()
		_ = cmd.Wait()
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %s", err, trimLoggedStderr(stderrBuf.String(), stderrBuf.Truncated()))
	}
	return nil
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
	if req.TimeoutMS < 0 {
		return errors.New("timeout_ms must be >= 0")
	}
	if req.MaxConcurrency < 0 {
		return errors.New("max_concurrency must be >= 0")
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

func isFunctionRunnerStartError(err error) bool {
	return errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, osexec.ErrNotFound) ||
		errors.Is(err, os.ErrPermission) ||
		errors.Is(err, syscall.ENOTDIR) ||
		errors.Is(err, syscall.EISDIR) ||
		errors.Is(err, syscall.ENOEXEC)
}

func (h *FunctionHandler) writeRunnerStartError(w http.ResponseWriter, err error) {
	if errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, osexec.ErrNotFound) ||
		errors.Is(err, syscall.ENOTDIR) {
		writeError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "function runtime unavailable")
		return
	}
	writeError(w, http.StatusInternalServerError, spec.CodeInternal, "function runtime unavailable")
}

func encodeWebSocketClientMessage(messageType int, data []byte) sandboxfunction.WebSocketFrame {
	if messageType == websocket.BinaryMessage {
		return sandboxfunction.WebSocketFrame{
			Type:        sandboxfunction.WebSocketFrameMessage,
			MessageType: sandboxfunction.WebSocketMessageBytes,
			DataBase64:  base64.StdEncoding.EncodeToString(data),
		}
	}
	return sandboxfunction.WebSocketFrame{
		Type:        sandboxfunction.WebSocketFrameMessage,
		MessageType: sandboxfunction.WebSocketMessageText,
		Data:        string(data),
	}
}

func decodeWebSocketRunnerMessage(frame sandboxfunction.WebSocketFrame) (int, []byte, error) {
	switch frame.MessageType {
	case "", sandboxfunction.WebSocketMessageText:
		return websocket.TextMessage, []byte(frame.Data), nil
	case sandboxfunction.WebSocketMessageBytes:
		data, err := base64.StdEncoding.DecodeString(frame.DataBase64)
		if err != nil {
			return 0, nil, errors.New("websocket data_base64 must be valid base64")
		}
		return websocket.BinaryMessage, data, nil
	default:
		return 0, nil, fmt.Errorf("unsupported websocket message_type %q", frame.MessageType)
	}
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

type functionWebSocketResult struct {
	err           error
	processExited bool
}

type functionConcurrencyLimiter struct {
	mu       sync.Mutex
	inFlight map[string]int
}

func newFunctionConcurrencyLimiter() *functionConcurrencyLimiter {
	return &functionConcurrencyLimiter{inFlight: map[string]int{}}
}

func (h *FunctionHandler) acquireFunctionSlot(req sandboxfunction.ExecuteRequest) (func(), bool) {
	if h.concurrency == nil {
		return func() {}, true
	}
	return h.concurrency.acquire(functionConcurrencyKey(req), req.MaxConcurrency)
}

func (l *functionConcurrencyLimiter) acquire(key string, limit int) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if limit > 0 && l.inFlight[key] >= limit {
		return nil, false
	}
	l.inFlight[key]++
	return sync.OnceFunc(func() {
		l.release(key)
	}), true
}

func (l *functionConcurrencyLimiter) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	current := l.inFlight[key]
	if current <= 1 {
		delete(l.inFlight, key)
		return
	}
	l.inFlight[key] = current - 1
}

func functionConcurrencyKey(req sandboxfunction.ExecuteRequest) string {
	serviceID := strings.TrimSpace(req.ServiceID)
	if serviceID != "" {
		return "service:" + serviceID
	}
	digest := req.Source.Digest
	if digest == "" && req.Source.Code != "" {
		digest = sandboxfunction.InlineDigest(req.Source.Code)
	}
	return "source:" + req.Runtime + ":" + req.Handler + ":" + digest
}

type trackingResponseWriter struct {
	http.ResponseWriter
	written bool
}

func (w *trackingResponseWriter) WriteHeader(statusCode int) {
	w.written = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *trackingResponseWriter) Write(data []byte) (int, error) {
	w.written = true
	return w.ResponseWriter.Write(data)
}

func (w *trackingResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
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
