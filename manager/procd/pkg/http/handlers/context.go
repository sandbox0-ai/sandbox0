// Package handlers provides HTTP handlers for Procd.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process/repl"
	"go.uber.org/zap"
)

// ContextHandler handles context-related HTTP requests.
type ContextHandler struct {
	manager  *ctxpkg.Manager
	logger   *zap.Logger
	upgrader websocket.Upgrader
}

// NewContextHandler creates a new context handler.
func NewContextHandler(manager *ctxpkg.Manager, logger *zap.Logger) *ContextHandler {
	return &ContextHandler{
		manager: manager,
		logger:  logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins in sandbox environment
			},
		},
	}
}

// CreateContextRequest is the request body for creating a context.
type CreateContextRequest struct {
	Type process.ProcessType `json:"type"` // "repl" or "cmd"

	Repl *CreateREPLContextRequest `json:"repl,omitempty"`
	Cmd  *CreateCMDContextRequest  `json:"cmd,omitempty"`

	WaitUntilDone bool `json:"wait_until_done"`

	CWD            string            `json:"cwd"`
	EnvVars        map[string]string `json:"env_vars"`
	PTYSize        *process.PTYSize  `json:"pty_size"`
	IdleTimeoutSec int32             `json:"idle_timeout_sec,omitempty"`
	TTLSec         int32             `json:"ttl_sec,omitempty"`
}

// CreateREPLContextRequest is the request body for creating a REPL context.
type CreateREPLContextRequest struct {
	Alias      string           `json:"alias"`                 // python, node, bash, zsh, etc.
	Input      string           `json:"input"`                 // code to execute
	ReplConfig *repl.REPLConfig `json:"repl_config,omitempty"` // custom config
}

// CreateCMDContextRequest is the request body for creating a CMD context.
type CreateCMDContextRequest struct {
	Command []string `json:"command"` // command path and args, e.g., ["/bin/ls", "-la"]
}

// ContextResponse is the response body for a context.
type ContextResponse struct {
	ID        string              `json:"id"`
	Type      process.ProcessType `json:"type"`
	Alias     string              `json:"alias"`
	CWD       string              `json:"cwd"`
	EnvVars   map[string]string   `json:"env_vars"`
	Running   bool                `json:"running"`
	Paused    bool                `json:"paused"`
	CreatedAt string              `json:"created_at"`
	OutputRaw string              `json:"output_raw,omitempty"`
}

func normalizeStringMap(value map[string]string) map[string]string {
	if value == nil {
		return map[string]string{}
	}
	return value
}

func newContextResponse(ctx *ctxpkg.Context, outputRaw string) ContextResponse {
	return ContextResponse{
		ID:        ctx.ID,
		Type:      ctx.Type,
		Alias:     ctx.Alias,
		CWD:       ctx.CWD,
		EnvVars:   normalizeStringMap(ctx.EnvVars),
		Running:   ctx.IsRunning(),
		Paused:    ctx.IsPaused(),
		CreatedAt: ctx.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		OutputRaw: outputRaw,
	}
}

// ContextStatsResponse is the response body for context resource stats.
type ContextStatsResponse struct {
	ContextID string                `json:"context_id"`
	Type      string                `json:"type"`
	Alias     string                `json:"alias"`
	Running   bool                  `json:"running"`
	Paused    bool                  `json:"paused"`
	Usage     process.ResourceUsage `json:"usage"`
}

// ContextInputRequest is the request body for sending input to a context.
type ContextInputRequest struct {
	Data string `json:"data"`
}

// ContextExecResponse is the response body for synchronous execution.
type ContextExecResponse struct {
	OutputRaw string `json:"output_raw"`
}

// ResizeContextRequest is the request body for resizing a PTY.
type ResizeContextRequest struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

// SignalContextRequest is the request body for sending a signal.
type SignalContextRequest struct {
	Signal string `json:"signal"`
}

type wsControlMessage struct {
	Type      string `json:"type"`
	Data      string `json:"data"`
	Rows      uint16 `json:"rows"`
	Cols      uint16 `json:"cols"`
	Signal    string `json:"signal"`
	RequestID string `json:"request_id"`
}

type execError struct {
	status  int
	code    string
	message string
}

func (e *execError) Error() string {
	return e.message
}

const execTimeout = 30 * time.Second

// List lists all contexts.
func (h *ContextHandler) List(w http.ResponseWriter, r *http.Request) {
	contexts := h.manager.ListContexts()

	var response []ContextResponse
	for _, ctx := range contexts {
		response = append(response, newContextResponse(ctx, ""))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"contexts": response,
	})
}

// Create creates a new context.
func (h *ContextHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateContextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.IdleTimeoutSec < 0 || req.TTLSec < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "idle_timeout_sec and ttl_sec must be >= 0")
		return
	}
	var (
		alias      string
		command    []string
		replConfig *repl.REPLConfig
		input      string
	)
	if req.Repl != nil && req.Type != process.ProcessTypeREPL {
		writeError(w, http.StatusBadRequest, "invalid_request", "repl is only valid for repl contexts")
		return
	}
	if req.Cmd != nil && req.Type != process.ProcessTypeCMD {
		writeError(w, http.StatusBadRequest, "invalid_request", "cmd is only valid for cmd contexts")
		return
	}
	if req.Repl != nil {
		alias = req.Repl.Alias
		replConfig = req.Repl.ReplConfig
		input = req.Repl.Input
	}
	if req.Cmd != nil {
		command = req.Cmd.Command
	}
	if replConfig != nil {
		if replConfig.Name == "" {
			replConfig.Name = alias
		}
		if replConfig.Name == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "repl.repl_config.name is required")
			return
		}
		if alias == "" {
			alias = replConfig.Name
		} else if alias != replConfig.Name {
			writeError(w, http.StatusBadRequest, "invalid_request", "repl.alias must match repl.repl_config.name")
			return
		}
		if err := replConfig.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}

	policy := ctxpkg.CleanupPolicy{}
	if req.IdleTimeoutSec > 0 {
		policy.IdleTimeout = time.Duration(req.IdleTimeoutSec) * time.Second
	}
	if req.TTLSec > 0 {
		policy.MaxLifetime = time.Duration(req.TTLSec) * time.Second
	}

	ctx, err := h.manager.CreateContextWithPolicyAndREPLConfig(process.ProcessConfig{
		Type:    req.Type,
		Alias:   alias,
		Command: command,
		CWD:     req.CWD,
		EnvVars: req.EnvVars,
		PTYSize: req.PTYSize,
	}, replConfig, policy)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create_failed", err.Error())
		return
	}

	if req.WaitUntilDone {
		output, execErr, aborted := h.execInputSync(ctx, input, r.Context())
		if aborted {
			return
		}
		if execErr != nil {
			writeError(w, execErr.status, execErr.code, execErr.message)
			return
		}
		writeJSON(w, http.StatusCreated, newContextResponse(ctx, output))
		return
	}

	writeJSON(w, http.StatusCreated, newContextResponse(ctx, ""))
}

// Get gets a context by ID.
func (h *ContextHandler) Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	ctx, err := h.manager.GetContext(id)
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, newContextResponse(ctx, ""))
}

// Delete deletes a context.
func (h *ContextHandler) Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	err := h.manager.DeleteContext(id)
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

// Restart restarts a context.
func (h *ContextHandler) Restart(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	ctx, err := h.manager.RestartContext(id)
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "restart_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, newContextResponse(ctx, ""))
}

// WriteInput writes input to a context's process.
func (h *ContextHandler) WriteInput(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req ContextInputRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	err := h.manager.WriteInput(id, []byte(req.Data))
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "write_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"written": true})
}

// Exec executes input synchronously and returns output when complete.
func (h *ContextHandler) Exec(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req ContextInputRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Data == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "data is required")
		return
	}

	ctx, err := h.manager.GetContext(id)
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "get_failed", err.Error())
		return
	}
	if ctx.MainProcess == nil {
		writeError(w, http.StatusConflict, "process_not_running", process.ErrProcessNotRunning.Error())
		return
	}

	output, execErr, aborted := h.execInputSync(ctx, req.Data, r.Context())
	if aborted {
		return
	}
	if execErr != nil {
		writeError(w, execErr.status, execErr.code, execErr.message)
		return
	}

	writeJSON(w, http.StatusOK, ContextExecResponse{OutputRaw: output})
}

// Stats returns resource usage statistics for a context.
func (h *ContextHandler) Stats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	usage, err := h.manager.GetResourceUsage(id)
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "stats_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, ContextStatsResponse{
		ContextID: usage.ContextID,
		Type:      string(usage.Type),
		Alias:     usage.Alias,
		Running:   usage.Running,
		Paused:    usage.Paused,
		Usage:     usage.Usage,
	})
}

func drainOutput(ch <-chan process.ProcessOutput) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func normalizeExecOutput(processType process.ProcessType, raw string) string {
	if processType != process.ProcessTypeREPL {
		return raw
	}
	normalized := strings.TrimSuffix(raw, repl.DefaultReadyToken)
	if !strings.HasPrefix(normalized, repl.DefaultReadyToken) {
		normalized = repl.DefaultReadyToken + normalized
	}
	return normalized
}

func (h *ContextHandler) execInputSync(ctx *ctxpkg.Context, input string, requestCtx context.Context) (string, *execError, bool) {
	if ctx == nil || ctx.MainProcess == nil {
		return "", &execError{
			status:  http.StatusConflict,
			code:    "process_not_running",
			message: process.ErrProcessNotRunning.Error(),
		}, false
	}
	if ctx.MainProcess.IsFinished() {
		return "", &execError{
			status:  http.StatusGone,
			code:    "process_finished",
			message: process.ErrProcessFinished.Error(),
		}, false
	}

	outputCh := ctx.MainProcess.ReadOutput()
	drainOutput(outputCh)

	normalizedInput := input
	if ctx.Type == process.ProcessTypeREPL {
		if !strings.HasSuffix(normalizedInput, "\n") && !strings.HasSuffix(normalizedInput, "\r") {
			normalizedInput += "\n"
		}
	}

	err := h.manager.WriteInput(ctx.ID, []byte(normalizedInput))
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			return "", &execError{
				status:  http.StatusNotFound,
				code:    "context_not_found",
				message: err.Error(),
			}, false
		}
		if errors.Is(err, process.ErrProcessNotRunning) {
			return "", &execError{
				status:  http.StatusConflict,
				code:    "process_not_running",
				message: err.Error(),
			}, false
		}
		if errors.Is(err, process.ErrInputBufferFull) {
			return "", &execError{
				status:  http.StatusConflict,
				code:    "input_buffer_full",
				message: err.Error(),
			}, false
		}
		return "", &execError{
			status:  http.StatusInternalServerError,
			code:    "write_failed",
			message: err.Error(),
		}, false
	}

	timeout := time.NewTimer(execTimeout)
	defer timeout.Stop()

	var output bytes.Buffer
	var promptTimer *time.Timer
	var promptWait <-chan time.Time
	for {
		select {
		case <-requestCtx.Done():
			return "", nil, true
		case <-timeout.C:
			return "", &execError{
				status:  http.StatusRequestTimeout,
				code:    "exec_timeout",
				message: "execution timed out",
			}, false
		case <-promptWait:
			return normalizeExecOutput(ctx.Type, output.String()), nil, false
		case msg, ok := <-outputCh:
			if !ok {
				return normalizeExecOutput(ctx.Type, output.String()), nil, false
			}
			if msg.Source == process.OutputSourcePrompt {
				if promptTimer == nil {
					promptTimer = time.NewTimer(50 * time.Millisecond)
				} else {
					if !promptTimer.Stop() {
						select {
						case <-promptTimer.C:
						default:
						}
					}
					promptTimer.Reset(50 * time.Millisecond)
				}
				promptWait = promptTimer.C
				continue
			}
			if len(msg.Data) > 0 {
				_, _ = output.Write(msg.Data)
			}
			if promptTimer != nil {
				if !promptTimer.Stop() {
					select {
					case <-promptTimer.C:
					default:
					}
				}
				promptTimer.Reset(50 * time.Millisecond)
				promptWait = promptTimer.C
			}
		}
	}
}

// ResizePTY resizes a context's PTY.
func (h *ContextHandler) ResizePTY(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req ResizeContextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Rows == 0 || req.Cols == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "rows and cols must be > 0")
		return
	}

	err := h.manager.ResizePTY(id, process.PTYSize{Rows: req.Rows, Cols: req.Cols})
	if err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		if errors.Is(err, process.ErrPTYNotAvailable) {
			writeError(w, http.StatusConflict, "pty_unavailable", err.Error())
			return
		}
		if errors.Is(err, process.ErrProcessNotRunning) {
			writeError(w, http.StatusConflict, "process_not_running", err.Error())
			return
		}
		if errors.Is(err, process.ErrInvalidPTYSize) {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "resize_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"resized": true})
}

// SendSignal sends a signal to a context's process.
func (h *ContextHandler) SendSignal(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req SignalContextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	sig, err := parseSignal(req.Signal)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if err := h.manager.SendSignal(id, sig); err != nil {
		if err == ctxpkg.ErrContextNotFound {
			writeError(w, http.StatusNotFound, "context_not_found", err.Error())
			return
		}
		if errors.Is(err, process.ErrProcessNotRunning) {
			writeError(w, http.StatusConflict, "process_not_running", err.Error())
			return
		}
		if errors.Is(err, process.ErrSignalFailed) {
			writeError(w, http.StatusInternalServerError, "signal_failed", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "signal_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"signaled": true})
}

func parseSignal(value string) (syscall.Signal, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, errors.New("signal is required")
	}

	upper := strings.ToUpper(trimmed)
	upper = strings.TrimPrefix(upper, "SIG")

	switch upper {
	case "INT":
		return syscall.SIGINT, nil
	case "TERM":
		return syscall.SIGTERM, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "HUP":
		return syscall.SIGHUP, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "USR1":
		return syscall.SIGUSR1, nil
	case "USR2":
		return syscall.SIGUSR2, nil
	case "WINCH":
		return syscall.SIGWINCH, nil
	case "STOP":
		return syscall.SIGSTOP, nil
	case "CONT":
		return syscall.SIGCONT, nil
	}

	if num, err := strconv.Atoi(upper); err == nil && num > 0 {
		return syscall.Signal(num), nil
	}

	return 0, errors.New("unsupported signal")
}

// WebSocket handles WebSocket connections for context I/O.
func (h *ContextHandler) WebSocket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("WebSocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	closeDone := make(chan struct{})
	var closeOnce sync.Once
	closeConn := func(reason string) {
		closeOnce.Do(func() {
			close(closeDone)
			if reason != "" {
				_ = conn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason),
					time.Now().Add(time.Second),
				)
			}
			_ = conn.Close()
		})
	}

	ctx, err := h.manager.GetContext(id)
	if err != nil {
		closeConn("context not found")
		return
	}

	if ctx.MainProcess == nil {
		closeConn("context not running")
		return
	}

	if ctx.MainProcess.IsFinished() {
		if outputProvider, ok := ctx.MainProcess.(process.OutputProvider); ok {
			stdout, stderr := outputProvider.GetOutput()
			if stdout != "" {
				_ = conn.WriteJSON(map[string]any{
					"source": string(process.OutputSourceStdout),
					"data":   stdout,
				})
			}
			if stderr != "" {
				_ = conn.WriteJSON(map[string]any{
					"source": string(process.OutputSourceStderr),
					"data":   stderr,
				})
			}
		}
		closeConn("context finished")
		return
	}

	ctx.AddExitHandler(func(event process.ExitEvent) {
		go closeConn("context exited")
	})

	var pendingMu sync.Mutex
	pendingRequestID := ""
	setPendingRequestID := func(value string) {
		pendingMu.Lock()
		pendingRequestID = value
		pendingMu.Unlock()
	}
	takePendingRequestID := func() string {
		pendingMu.Lock()
		value := pendingRequestID
		pendingRequestID = ""
		pendingMu.Unlock()
		return value
	}

	// Get output channel
	outputCh, err := h.manager.ReadOutput(id)
	if err != nil {
		return
	}

	// Write output to WebSocket
	go func() {
		for {
			select {
			case <-closeDone:
				return
			case output, ok := <-outputCh:
				if !ok {
					closeConn("context output closed")
					return
				}
				if output.Source == process.OutputSourcePrompt {
					requestID := takePendingRequestID()
					if requestID == "" {
						continue
					}
					ctx.Touch()
					continue
				}

				msg := map[string]any{
					"source": string(output.Source),
					"data":   string(output.Data),
				}

				if err := conn.WriteJSON(msg); err != nil {
					closeConn("websocket write failed")
					return
				}
				ctx.Touch()
			}
		}
	}()

	// Read input from WebSocket
	for {
		select {
		case <-closeDone:
			return
		default:
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			closeConn("")
			break
		}
		if len(data) == 0 {
			continue
		}

		var msg wsControlMessage
		if err := json.Unmarshal(data, &msg); err == nil && msg.Type != "" {
			switch msg.Type {
			case "input":
				setPendingRequestID(msg.RequestID)
				if msg.Data != "" {
					input := msg.Data
					if ctx.Type == process.ProcessTypeREPL {
						if !strings.HasSuffix(input, "\n") && !strings.HasSuffix(input, "\r") {
							input += "\n"
						}
					}
					_ = h.manager.WriteInput(id, []byte(input))
				}
			case "resize":
				if msg.Rows == 0 || msg.Cols == 0 {
					h.logger.Warn("Invalid resize request", zap.String("context_id", id))
					continue
				}
				if err := h.manager.ResizePTY(id, process.PTYSize{Rows: msg.Rows, Cols: msg.Cols}); err != nil {
					h.logger.Warn("Resize failed", zap.String("context_id", id), zap.Error(err))
				}
			case "signal":
				sig, err := parseSignal(msg.Signal)
				if err != nil {
					h.logger.Warn("Invalid signal request", zap.String("context_id", id), zap.Error(err))
					continue
				}
				if err := h.manager.SendSignal(id, sig); err != nil {
					h.logger.Warn("Signal failed", zap.String("context_id", id), zap.Error(err))
				}
			default:
				if ctx.MainProcess != nil {
					setPendingRequestID(msg.RequestID)
					_ = ctx.MainProcess.WriteInput(data)
				}
			}
			continue
		}

		if ctx.MainProcess != nil {
			setPendingRequestID(msg.RequestID)
			_ = ctx.MainProcess.WriteInput(data)
		}
	}
}
