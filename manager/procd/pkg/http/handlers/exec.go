// Package handlers provides HTTP handlers for Procd.
package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"go.uber.org/zap"
)

// ExecHandler handles execution-related HTTP requests.
type ExecHandler struct {
	logger *zap.Logger
}

// NewExecHandler creates a new exec handler.
func NewExecHandler(logger *zap.Logger) *ExecHandler {
	return &ExecHandler{
		logger: logger,
	}
}

// ExecRequest is the request body for executing a command.
type ExecRequest struct {
	Type     string            `json:"type"`     // "repl" or "cmd"
	Language string            `json:"language"` // For REPL: python, node, bash, zsh, ruby, lua, php, r, perl
	Command  []string          `json:"command"`  // For CMD: command and args
	Code     string            `json:"code"`     // For REPL: code to execute
	CWD      string            `json:"cwd"`
	EnvVars  map[string]string `json:"env_vars"`
	Timeout  int               `json:"timeout"` // Timeout in seconds, 0 means no timeout
	PTYSize  *process.PTYSize  `json:"pty_size"`
}

// ExecResponse is the response for synchronous execution.
type ExecResponse struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   int    `json:"exit_code"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

// StreamEvent represents a Server-Sent Event.
type StreamEvent struct {
	Event string `json:"event"` // start, stdout, stderr, exit, error
	Data  any    `json:"data"`
}

// Exec handles synchronous (blocking) command execution.
// It waits for the command to complete and returns the full output.
func (h *ExecHandler) Exec(w http.ResponseWriter, r *http.Request) {
	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	// Validate request
	if req.Type == "cmd" && len(req.Command) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "command is required for cmd type")
		return
	}
	if req.Type == "repl" && req.Code == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "code is required for repl type")
		return
	}

	startTime := time.Now()

	// Create context with timeout
	ctx := r.Context()
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	var result ExecResponse

	if req.Type == "repl" || req.Type == "" {
		result = h.execREPL(ctx, &req)
	} else {
		result = h.execCMD(ctx, &req)
	}

	result.DurationMs = time.Since(startTime).Milliseconds()

	writeJSON(w, http.StatusOK, result)
}

// ExecStream handles streaming (SSE) command execution.
// It streams stdout/stderr in real-time and returns exit code at the end.
func (h *ExecHandler) ExecStream(w http.ResponseWriter, r *http.Request) {
	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	// Validate request
	if req.Type == "cmd" && len(req.Command) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "command is required for cmd type")
		return
	}
	if req.Type == "repl" && req.Code == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "code is required for repl type")
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_not_supported", "streaming not supported")
		return
	}

	// Create context with timeout
	ctx := r.Context()
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	startTime := time.Now()

	// Helper to send SSE event
	sendEvent := func(event string, data any) {
		dataBytes, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, string(dataBytes))
		flusher.Flush()
	}

	if req.Type == "repl" || req.Type == "" {
		h.execREPLStream(ctx, &req, sendEvent)
	} else {
		h.execCMDStream(ctx, &req, sendEvent)
	}

	durationMs := time.Since(startTime).Milliseconds()

	// The exit event is sent by execCMDStream/execREPLStream
	// Just add duration info
	_ = durationMs
}

// execCMD executes a command synchronously and returns the result.
func (h *ExecHandler) execCMD(ctx context.Context, req *ExecRequest) ExecResponse {
	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)

	if req.CWD != "" {
		cmd.Dir = req.CWD
	}

	// Set environment
	env := os.Environ()
	for k, v := range req.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = env

	// Create process group for cleanup
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	exitCode := 0
	errMsg := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() == context.DeadlineExceeded {
			exitCode = 124 // Standard timeout exit code
			errMsg = "execution timed out"
		} else if ctx.Err() == context.Canceled {
			exitCode = 130 // Standard interrupt exit code
			errMsg = "execution canceled"
		} else {
			exitCode = 1
			errMsg = err.Error()
		}
	}

	return ExecResponse{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
		Error:    errMsg,
	}
}

// execREPL executes code in a REPL synchronously.
func (h *ExecHandler) execREPL(ctx context.Context, req *ExecRequest) ExecResponse {
	// Determine the interpreter
	interpreter, args := h.getInterpreter(req.Language)
	if interpreter == "" {
		return ExecResponse{
			ExitCode: 1,
			Error:    fmt.Sprintf("unsupported language: %s", req.Language),
		}
	}

	// For REPL, we execute the code via stdin
	cmd := exec.CommandContext(ctx, interpreter, args...)

	if req.CWD != "" {
		cmd.Dir = req.CWD
	}

	env := os.Environ()
	for k, v := range req.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	// Force unbuffered output for Python
	env = append(env, "PYTHONUNBUFFERED=1")
	cmd.Env = env

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Pipe code to stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return ExecResponse{ExitCode: 1, Error: err.Error()}
	}

	if err := cmd.Start(); err != nil {
		return ExecResponse{ExitCode: 1, Error: err.Error()}
	}

	// Write code and close stdin
	io.WriteString(stdin, req.Code)
	stdin.Close()

	err = cmd.Wait()

	exitCode := 0
	errMsg := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() == context.DeadlineExceeded {
			exitCode = 124
			errMsg = "execution timed out"
		} else if ctx.Err() == context.Canceled {
			exitCode = 130
			errMsg = "execution canceled"
		} else {
			exitCode = 1
			errMsg = err.Error()
		}
	}

	return ExecResponse{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
		Error:    errMsg,
	}
}

// execCMDStream executes a command with streaming output.
func (h *ExecHandler) execCMDStream(ctx context.Context, req *ExecRequest, sendEvent func(string, any)) {
	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)

	if req.CWD != "" {
		cmd.Dir = req.CWD
	}

	env := os.Environ()
	for k, v := range req.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = env

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Check if PTY is requested
	if req.PTYSize != nil {
		h.execWithPTYStream(ctx, cmd, req.PTYSize, sendEvent)
		return
	}

	// Use pipes for stdout/stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	if err := cmd.Start(); err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	sendEvent("start", map[string]int{"pid": cmd.Process.Pid})

	// Stream stdout
	done := make(chan struct{})
	go func() {
		h.streamPipe(stdoutPipe, "stdout", sendEvent)
		done <- struct{}{}
	}()
	go func() {
		h.streamPipe(stderrPipe, "stderr", sendEvent)
		done <- struct{}{}
	}()

	// Wait for both pipes to close
	<-done
	<-done

	err = cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() == context.DeadlineExceeded {
			exitCode = 124
		} else if ctx.Err() == context.Canceled {
			exitCode = 130
		} else {
			exitCode = 1
		}
	}

	sendEvent("exit", map[string]int{"exit_code": exitCode})
}

// execREPLStream executes REPL code with streaming output.
func (h *ExecHandler) execREPLStream(ctx context.Context, req *ExecRequest, sendEvent func(string, any)) {
	interpreter, args := h.getInterpreter(req.Language)
	if interpreter == "" {
		sendEvent("error", map[string]string{"message": fmt.Sprintf("unsupported language: %s", req.Language)})
		return
	}

	cmd := exec.CommandContext(ctx, interpreter, args...)

	if req.CWD != "" {
		cmd.Dir = req.CWD
	}

	env := os.Environ()
	for k, v := range req.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	env = append(env, "PYTHONUNBUFFERED=1")
	cmd.Env = env

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Check if PTY is requested
	if req.PTYSize != nil {
		h.execREPLWithPTYStream(ctx, cmd, req.Code, req.PTYSize, sendEvent)
		return
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	if err := cmd.Start(); err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}

	sendEvent("start", map[string]int{"pid": cmd.Process.Pid})

	// Write code to stdin
	go func() {
		io.WriteString(stdin, req.Code)
		stdin.Close()
	}()

	// Stream output
	done := make(chan struct{})
	go func() {
		h.streamPipe(stdoutPipe, "stdout", sendEvent)
		done <- struct{}{}
	}()
	go func() {
		h.streamPipe(stderrPipe, "stderr", sendEvent)
		done <- struct{}{}
	}()

	<-done
	<-done

	err = cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() == context.DeadlineExceeded {
			exitCode = 124
		} else if ctx.Err() == context.Canceled {
			exitCode = 130
		} else {
			exitCode = 1
		}
	}

	sendEvent("exit", map[string]int{"exit_code": exitCode})
}

// execWithPTYStream executes a command with PTY and streams output.
func (h *ExecHandler) execWithPTYStream(ctx context.Context, cmd *exec.Cmd, ptySize *process.PTYSize, sendEvent func(string, any)) {
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: ptySize.Rows,
		Cols: ptySize.Cols,
	})
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}
	defer ptmx.Close()

	sendEvent("start", map[string]int{"pid": cmd.Process.Pid})

	// Stream PTY output
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				sendEvent("pty", map[string]string{"data": string(buf[:n])})
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	// Wait for context cancellation or process exit
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		cmd.Process.Kill()
		<-waitCh
	case err = <-waitCh:
	}

	<-done

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() != nil {
			exitCode = 130
		} else {
			exitCode = 1
		}
	}

	sendEvent("exit", map[string]int{"exit_code": exitCode})
}

// execREPLWithPTYStream executes REPL code with PTY and streams output.
func (h *ExecHandler) execREPLWithPTYStream(ctx context.Context, cmd *exec.Cmd, code string, ptySize *process.PTYSize, sendEvent func(string, any)) {
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: ptySize.Rows,
		Cols: ptySize.Cols,
	})
	if err != nil {
		sendEvent("error", map[string]string{"message": err.Error()})
		return
	}
	defer ptmx.Close()

	sendEvent("start", map[string]int{"pid": cmd.Process.Pid})

	// Write code to PTY
	go func() {
		time.Sleep(100 * time.Millisecond) // Wait for REPL to start
		io.WriteString(ptmx, code+"\n")
		// Send exit command for the REPL
		time.Sleep(100 * time.Millisecond)
		io.WriteString(ptmx, "exit()\n") // Python
	}()

	// Stream PTY output
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				sendEvent("pty", map[string]string{"data": string(buf[:n])})
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		cmd.Process.Kill()
		<-waitCh
	case err = <-waitCh:
	}

	<-done

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() != nil {
			exitCode = 130
		} else {
			exitCode = 1
		}
	}

	sendEvent("exit", map[string]int{"exit_code": exitCode})
}

// streamPipe reads from a pipe and sends events.
func (h *ExecHandler) streamPipe(pipe io.ReadCloser, eventType string, sendEvent func(string, any)) {
	scanner := bufio.NewScanner(pipe)
	// Increase buffer size for large outputs
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		sendEvent(eventType, map[string]string{"data": scanner.Text() + "\n"})
	}
}

// getInterpreter returns the interpreter path and args for a language.
func (h *ExecHandler) getInterpreter(language string) (string, []string) {
	switch language {
	case "python", "python3", "":
		// Try python3 first, then python
		if path, err := exec.LookPath("python3"); err == nil {
			return path, []string{"-u"} // -u for unbuffered
		}
		if path, err := exec.LookPath("python"); err == nil {
			return path, []string{"-u"}
		}
		return "", nil
	case "node", "nodejs", "javascript":
		if path, err := exec.LookPath("node"); err == nil {
			return path, []string{}
		}
		return "", nil
	case "bash":
		return "/bin/bash", []string{}
	case "zsh":
		return "/bin/zsh", []string{}
	case "sh":
		return "/bin/sh", []string{}
	case "ruby", "rb":
		if path, err := exec.LookPath("ruby"); err == nil {
			return path, []string{}
		}
		return "", nil
	case "lua":
		// Try lua variants
		for _, variant := range []string{"lua", "lua5.4", "lua5.3", "lua5.2", "lua5.1", "luajit"} {
			if path, err := exec.LookPath(variant); err == nil {
				return path, []string{}
			}
		}
		return "", nil
	case "php":
		if path, err := exec.LookPath("php"); err == nil {
			return path, []string{}
		}
		return "", nil
	case "r", "R":
		if path, err := exec.LookPath("Rscript"); err == nil {
			return path, []string{"--vanilla"}
		}
		if path, err := exec.LookPath("R"); err == nil {
			return path, []string{"--quiet", "--no-save"}
		}
		return "", nil
	case "perl", "pl":
		if path, err := exec.LookPath("perl"); err == nil {
			return path, []string{}
		}
		return "", nil
	default:
		return "", nil
	}
}
