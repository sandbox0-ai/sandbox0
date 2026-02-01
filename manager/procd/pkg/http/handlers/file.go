package handlers

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/file"
	"go.uber.org/zap"
)

// FileHandler handles file-related HTTP requests.
type FileHandler struct {
	manager  *file.Manager
	logger   *zap.Logger
	upgrader websocket.Upgrader
}

// NewFileHandler creates a new file handler.
func NewFileHandler(manager *file.Manager, logger *zap.Logger) *FileHandler {
	return &FileHandler{
		manager: manager,
		logger:  logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// Handle handles file operations based on HTTP method and query parameters.
func (h *FileHandler) Handle(w http.ResponseWriter, r *http.Request) {
	// Extract path from query
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "path is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, path)
	case http.MethodPost:
		h.handlePost(w, r, path)
	case http.MethodDelete:
		h.handleDelete(w, r, path)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (h *FileHandler) handleGet(w http.ResponseWriter, r *http.Request, path string) {
	query := r.URL.Query()

	// Read file
	data, err := h.manager.ReadFile(path)
	if err != nil {
		h.handleFileError(w, err)
		return
	}

	if query.Has("stat") || query.Has("list") || query.Has("binary") {
		writeError(w, http.StatusBadRequest, "invalid_request", "stat/list/binary queries are not supported")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}

func (h *FileHandler) Stat(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "path is required")
		return
	}

	info, err := h.manager.Stat(path)
	if err != nil {
		h.handleFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (h *FileHandler) List(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "path is required")
		return
	}

	entries, err := h.manager.ListDir(path)
	if err != nil {
		h.handleFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entries": entries,
	})
}

func (h *FileHandler) Binary(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "path is required")
		return
	}

	data, err := h.manager.ReadFile(path)
	if err != nil {
		h.handleFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"content":  base64.StdEncoding.EncodeToString(data),
		"encoding": "base64",
	})
}

func (h *FileHandler) handlePost(w http.ResponseWriter, r *http.Request, path string) {
	query := r.URL.Query()

	if query.Get("mkdir") == "true" {
		// Create directory
		recursive := query.Get("recursive") == "true"
		if err := h.manager.MakeDir(path, 0755, recursive); err != nil {
			h.handleFileError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]bool{"created": true})
		return
	}

	// Write file
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}

	if err := h.manager.WriteFile(path, data, 0644); err != nil {
		h.handleFileError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"written": true})
}

func (h *FileHandler) handleDelete(w http.ResponseWriter, r *http.Request, path string) {
	if err := h.manager.Remove(path); err != nil {
		h.handleFileError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

// Move handles file/directory move operations.
func (h *FileHandler) Move(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Source      string `json:"source"`
		Destination string `json:"destination"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.Source == "" || req.Destination == "" {
		writeError(w, http.StatusBadRequest, "invalid_paths", "source and destination are required")
		return
	}

	if err := h.manager.Move(req.Source, req.Destination); err != nil {
		h.handleFileError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"moved": true})
}

// Watch handles WebSocket file watching.
func (h *FileHandler) Watch(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("WebSocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	// Active watchers for this connection
	watchers := make(map[string]*file.Watcher)

	defer func() {
		// Cleanup all watchers on disconnect
		for _, watcher := range watchers {
			h.manager.UnwatchDir(watcher.ID)
		}
	}()

	// Read messages loop
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var req struct {
			Action    string `json:"action"`
			Path      string `json:"path"`
			Recursive bool   `json:"recursive"`
			WatchID   string `json:"watch_id"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			continue
		}

		switch req.Action {
		case "subscribe":
			watcher, err := h.manager.WatchDir(req.Path, req.Recursive)
			if err != nil {
				conn.WriteJSON(map[string]any{
					"type":  "error",
					"error": err.Error(),
				})
				continue
			}

			watchers[watcher.ID] = watcher

			// Send subscription confirmation
			conn.WriteJSON(map[string]any{
				"type":     "subscribed",
				"watch_id": watcher.ID,
				"path":     req.Path,
			})

			// Forward events for this watcher
			go func(w *file.Watcher) {
				for event := range w.EventChan {
					conn.WriteJSON(map[string]any{
						"type":     "event",
						"watch_id": event.WatchID,
						"event":    string(event.Type),
						"path":     event.Path,
					})
				}
			}(watcher)

		case "unsubscribe":
			if watcher, ok := watchers[req.WatchID]; ok {
				h.manager.UnwatchDir(watcher.ID)
				delete(watchers, req.WatchID)

				conn.WriteJSON(map[string]any{
					"type":     "unsubscribed",
					"watch_id": req.WatchID,
				})
			}
		}
	}
}

func (h *FileHandler) handleFileError(w http.ResponseWriter, err error) {
	switch err {
	case file.ErrFileNotFound:
		writeError(w, http.StatusNotFound, "file_not_found", err.Error())
	case file.ErrDirNotFound:
		writeError(w, http.StatusNotFound, "directory_not_found", err.Error())
	case file.ErrFileTooLarge:
		writeError(w, http.StatusRequestEntityTooLarge, "file_too_large", err.Error())
	case file.ErrPermissionDenied:
		writeError(w, http.StatusForbidden, "permission_denied", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "operation_failed", err.Error())
	}
}
