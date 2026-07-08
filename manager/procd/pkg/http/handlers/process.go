package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"go.uber.org/zap"
)

// ProcessHandler handles process-session HTTP requests.
type ProcessHandler struct {
	manager *process.SessionManager
	logger  *zap.Logger
}

// NewProcessHandler creates a process-session handler.
func NewProcessHandler(manager *process.SessionManager, logger *zap.Logger) *ProcessHandler {
	return &ProcessHandler{
		manager: manager,
		logger:  logger,
	}
}

// ProcessSessionResponse wraps a process session.
type ProcessSessionResponse struct {
	Process process.ProcessSessionSnapshot `json:"process"`
}

// ProcessSessionListResponse wraps process sessions.
type ProcessSessionListResponse struct {
	Processes []process.ProcessSessionSnapshot `json:"processes"`
}

// ProcessEventResponse wraps an accepted process event.
type ProcessEventResponse struct {
	Event process.ProcessEvent `json:"event"`
}

// ProcessPTYSizeRequest is the request body for resizing a process PTY channel.
type ProcessPTYSizeRequest struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

// List lists all process sessions.
func (h *ProcessHandler) List(w http.ResponseWriter, r *http.Request) {
	sessions := h.manager.ListSessions()
	response := ProcessSessionListResponse{
		Processes: make([]process.ProcessSessionSnapshot, 0, len(sessions)),
	}
	for _, session := range sessions {
		response.Processes = append(response.Processes, session.Snapshot())
	}
	writeJSON(w, http.StatusOK, response)
}

// Create creates a process session.
func (h *ProcessHandler) Create(w http.ResponseWriter, r *http.Request) {
	var spec process.ProcessSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	session, err := h.manager.CreateSession(spec)
	if err != nil {
		h.writeProcessError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, ProcessSessionResponse{Process: session.Snapshot()})
}

// Get gets a process session.
func (h *ProcessHandler) Get(w http.ResponseWriter, r *http.Request) {
	session, ok := h.getSession(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, ProcessSessionResponse{Process: session.Snapshot()})
}

// Delete stops and removes a process session.
func (h *ProcessHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "process id is required")
		return
	}
	if err := h.manager.DeleteSession(id); err != nil {
		h.writeProcessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

// SendEvent sends an input event to a process channel.
func (h *ProcessHandler) SendEvent(w http.ResponseWriter, r *http.Request) {
	session, ok := h.getSession(w, r)
	if !ok {
		return
	}
	var event process.ProcessInputEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	accepted, err := session.HandleInput(event)
	if err != nil {
		h.writeProcessError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, ProcessEventResponse{Event: accepted})
}

// StreamEvents streams replayed and live process events as SSE.
func (h *ProcessHandler) StreamEvents(w http.ResponseWriter, r *http.Request) {
	session, ok := h.getSession(w, r)
	if !ok {
		return
	}
	cursor, err := parseCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unavailable", "response writer does not support flushing")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	events, cancel := session.Subscribe(cursor)
	defer cancel()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, open := <-events:
			if !open {
				return
			}
			if event.ProcessID == "" {
				event.ProcessID = session.ID()
			}
			if err := writeSSEEvent(w, event); err != nil {
				h.logger.Debug("Failed to write process SSE event", zap.String("process_id", session.ID()), zap.Error(err))
				return
			}
			flusher.Flush()
		}
	}
}

// SendSignal sends a signal to a process session.
func (h *ProcessHandler) SendSignal(w http.ResponseWriter, r *http.Request) {
	session, ok := h.getSession(w, r)
	if !ok {
		return
	}
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
	if err := session.SendSignal(sig); err != nil {
		h.writeProcessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"signaled": true})
}

// ResizePTY resizes a process PTY channel.
func (h *ProcessHandler) ResizePTY(w http.ResponseWriter, r *http.Request) {
	session, ok := h.getSession(w, r)
	if !ok {
		return
	}
	channel := mux.Vars(r)["channel"]
	var req ProcessPTYSizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Rows == 0 || req.Cols == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "rows and cols must be > 0")
		return
	}
	if err := session.ResizePTY(channel, process.PTYSize{Rows: req.Rows, Cols: req.Cols}); err != nil {
		h.writeProcessError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"resized": true})
}

func (h *ProcessHandler) getSession(w http.ResponseWriter, r *http.Request) (*process.Session, bool) {
	id := mux.Vars(r)["id"]
	if id == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "process id is required")
		return nil, false
	}
	session, err := h.manager.GetSession(id)
	if err != nil {
		h.writeProcessError(w, err)
		return nil, false
	}
	return session, true
}

func parseCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseInt(value, 10, 64)
	if err != nil || cursor < 0 {
		return 0, errors.New("cursor must be a non-negative integer")
	}
	return cursor, nil
}

func writeSSEEvent(w http.ResponseWriter, event process.ProcessEvent) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\n", event.Seq); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", string(body)); err != nil {
		return err
	}
	return nil
}

func (h *ProcessHandler) writeProcessError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, process.ErrProcessSessionNotFound):
		writeError(w, http.StatusNotFound, "process_not_found", err.Error())
	case errors.Is(err, process.ErrInvalidProcessSpec),
		errors.Is(err, process.ErrInvalidProcessEvent),
		errors.Is(err, process.ErrUnsupportedChannelKind),
		errors.Is(err, process.ErrUnsupportedChannelEvent),
		errors.Is(err, process.ErrDuplicateEventID),
		errors.Is(err, process.ErrInvalidCommand),
		errors.Is(err, process.ErrInvalidPTYSize):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, process.ErrInputBufferFull):
		writeError(w, http.StatusConflict, "input_buffer_full", err.Error())
	case errors.Is(err, process.ErrProcessFinished):
		writeError(w, http.StatusGone, "process_finished", err.Error())
	case errors.Is(err, process.ErrProcessNotRunning),
		errors.Is(err, process.ErrPTYNotAvailable):
		writeError(w, http.StatusConflict, "process_not_running", err.Error())
	case errors.Is(err, process.ErrSignalFailed):
		writeError(w, http.StatusInternalServerError, "signal_failed", err.Error())
	default:
		if errors.Is(err, syscall.EINVAL) {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "process_failed", err.Error())
	}
}
