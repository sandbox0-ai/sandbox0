// Package handlers provides HTTP handlers for Procd.
package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	ctxpkg "github.com/sandbox0-ai/infra/manager/procd/pkg/context"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
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
	Type     string            `json:"type"`     // "repl" or "cmd"
	Language string            `json:"language"` // For REPL: python, node, bash, zsh, etc.
	Command  []string          `json:"command"`  // For CMD: command path and args, e.g., ["/bin/ls", "-la"]
	CWD      string            `json:"cwd"`
	EnvVars  map[string]string `json:"env_vars"`
	PTYSize  *process.PTYSize  `json:"pty_size"`
}

// ContextResponse is the response body for a context.
type ContextResponse struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Language  string            `json:"language"`
	CWD       string            `json:"cwd"`
	EnvVars   map[string]string `json:"env_vars"`
	Running   bool              `json:"running"`
	Paused    bool              `json:"paused"`
	CreatedAt string            `json:"created_at"`
}

// ContextStatsResponse is the response body for context resource stats.
type ContextStatsResponse struct {
	ContextID string                `json:"context_id"`
	Type      string                `json:"type"`
	Language  string                `json:"language"`
	Running   bool                  `json:"running"`
	Paused    bool                  `json:"paused"`
	Usage     process.ResourceUsage `json:"usage"`
}

// List lists all contexts.
func (h *ContextHandler) List(w http.ResponseWriter, r *http.Request) {
	contexts := h.manager.ListContexts()

	var response []ContextResponse
	for _, ctx := range contexts {
		response = append(response, ContextResponse{
			ID:        ctx.ID,
			Type:      string(ctx.Type),
			Language:  ctx.Language,
			CWD:       ctx.CWD,
			EnvVars:   ctx.EnvVars,
			Running:   ctx.IsRunning(),
			Paused:    ctx.IsPaused(),
			CreatedAt: ctx.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		})
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

	// Default to REPL type (includes shells like bash/zsh)
	processType := process.ProcessTypeREPL
	if req.Type == "cmd" {
		processType = process.ProcessTypeCMD
	}

	config := process.ProcessConfig{
		Type:     processType,
		Language: req.Language,
		Command:  req.Command,
		CWD:      req.CWD,
		EnvVars:  req.EnvVars,
		PTYSize:  req.PTYSize,
	}

	ctx, err := h.manager.CreateContext(config)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, ContextResponse{
		ID:        ctx.ID,
		Type:      string(ctx.Type),
		Language:  ctx.Language,
		CWD:       ctx.CWD,
		EnvVars:   ctx.EnvVars,
		Running:   ctx.IsRunning(),
		Paused:    ctx.IsPaused(),
		CreatedAt: ctx.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	})
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

	writeJSON(w, http.StatusOK, ContextResponse{
		ID:        ctx.ID,
		Type:      string(ctx.Type),
		Language:  ctx.Language,
		CWD:       ctx.CWD,
		EnvVars:   ctx.EnvVars,
		Running:   ctx.IsRunning(),
		Paused:    ctx.IsPaused(),
		CreatedAt: ctx.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	})
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

	writeJSON(w, http.StatusOK, ContextResponse{
		ID:        ctx.ID,
		Type:      string(ctx.Type),
		Language:  ctx.Language,
		CWD:       ctx.CWD,
		EnvVars:   ctx.EnvVars,
		Running:   ctx.IsRunning(),
		Paused:    ctx.IsPaused(),
		CreatedAt: ctx.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	})
}

// WriteInput writes input to a context's process.
func (h *ContextHandler) WriteInput(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req struct {
		Data string `json:"data"`
	}
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
		Language:  usage.Language,
		Running:   usage.Running,
		Paused:    usage.Paused,
		Usage:     usage.Usage,
	})
}

// WebSocket handles WebSocket connections for context I/O.
func (h *ContextHandler) WebSocket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	ctx, err := h.manager.GetContext(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "context_not_found", err.Error())
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("WebSocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	// Get output channel
	outputCh, err := h.manager.ReadOutput(id)
	if err != nil {
		return
	}

	// Write output to WebSocket
	go func() {
		for output := range outputCh {
			msg := map[string]any{
				"type":   "output",
				"source": string(output.Source),
				"data":   string(output.Data),
			}

			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		}
	}()

	// Read input from WebSocket
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}

		if ctx.MainProcess != nil {
			ctx.MainProcess.WriteInput(data)
		}
	}
}
