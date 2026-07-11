package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

const sessionStreamHeartbeat = 15 * time.Second

type SessionHandler struct {
	supervisor *session.Supervisor
	logger     *zap.Logger
	upgrader   websocket.Upgrader
}

func NewSessionHandler(supervisor *session.Supervisor, logger *zap.Logger) *SessionHandler {
	return &SessionHandler{
		supervisor: supervisor,
		logger:     logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(*http.Request) bool { return true },
		},
	}
}

func (h *SessionHandler) List(w http.ResponseWriter, _ *http.Request) {
	sessions := h.supervisor.List()
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (h *SessionHandler) Create(w http.ResponseWriter, r *http.Request) {
	var request session.SessionSpec
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	created, duplicate, err := h.supervisor.Create(request, r.Header.Get("Idempotency-Key"))
	if err != nil {
		h.writeSessionError(w, "create_session", err)
		return
	}
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, created)
}

func (h *SessionHandler) Get(w http.ResponseWriter, r *http.Request) {
	value, err := h.supervisor.Get(mux.Vars(r)["id"])
	if err != nil {
		h.writeSessionError(w, "get_session", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *SessionHandler) Update(w http.ResponseWriter, r *http.Request) {
	var request session.SessionSpec
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := h.supervisor.Update(mux.Vars(r)["id"], request)
	if err != nil {
		h.writeSessionError(w, "update_session", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *SessionHandler) Delete(w http.ResponseWriter, r *http.Request) {
	if err := h.supervisor.Delete(mux.Vars(r)["id"]); err != nil {
		h.writeSessionError(w, "delete_session", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (h *SessionHandler) SetDesiredState(w http.ResponseWriter, r *http.Request) {
	var request session.DesiredStateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := h.supervisor.SetDesiredState(mux.Vars(r)["id"], request.State)
	if err != nil {
		h.writeSessionError(w, "set_session_desired_state", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h *SessionHandler) CreateAttempt(w http.ResponseWriter, r *http.Request) {
	var request session.CreateAttemptRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	value, err := h.supervisor.CreateAttempt(mux.Vars(r)["id"], request.ReplaceCurrent)
	if err != nil {
		h.writeSessionError(w, "create_session_attempt", err)
		return
	}
	writeJSON(w, http.StatusCreated, value)
}

func (h *SessionHandler) WriteInput(w http.ResponseWriter, r *http.Request) {
	var request session.InputRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	value, err := h.supervisor.WriteInput(mux.Vars(r)["id"], request)
	if err != nil {
		h.writeSessionError(w, "write_session_input", err)
		return
	}
	writeJSON(w, http.StatusAccepted, value)
}

func (h *SessionHandler) SendSignal(w http.ResponseWriter, r *http.Request) {
	var request session.SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	signal, err := parseSessionSignal(request.Signal)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.supervisor.SendSignal(mux.Vars(r)["id"], request.ExpectedAttemptID, signal); err != nil {
		h.writeSessionError(w, "signal_session", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (h *SessionHandler) ResizeTerminal(w http.ResponseWriter, r *http.Request) {
	var request session.TerminalResizeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.supervisor.ResizeTerminal(mux.Vars(r)["id"], request.ExpectedAttemptID, request.Rows, request.Cols); err != nil {
		h.writeSessionError(w, "resize_session_terminal", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"resized": true})
}

func (h *SessionHandler) Events(w http.ResponseWriter, r *http.Request) {
	after, limit, err := sessionEventQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	page, err := h.supervisor.Events(mux.Vars(r)["id"], after, limit)
	if err != nil {
		h.writeSessionError(w, "list_session_events", err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *SessionHandler) EventStream(w http.ResponseWriter, r *http.Request) {
	after, _, err := sessionEventQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if header := strings.TrimSpace(r.Header.Get("Last-Event-ID")); header != "" {
		after, err = strconv.ParseInt(header, 10, 64)
		if err != nil || after < 0 {
			writeError(w, http.StatusBadRequest, "invalid_request", "Last-Event-ID must be a non-negative integer")
			return
		}
	}
	backlog, events, cancel, _, err := h.supervisor.Subscribe(mux.Vars(r)["id"], after)
	if err != nil {
		h.writeSessionError(w, "stream_session_events", err)
		return
	}
	defer cancel()
	if err := proxy.DisableResponseDeadlines(w); err != nil {
		h.logger.Debug("Failed to disable session event stream deadlines", zap.Error(err))
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "stream_unavailable", "response streaming is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	for _, event := range backlog {
		if err := writeSessionSSE(w, event); err != nil {
			return
		}
	}
	flusher.Flush()
	heartbeat := time.NewTicker(sessionStreamHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := writeSessionSSE(w, event); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

type sessionWSRequest struct {
	Type              string `json:"type"`
	RequestID         string `json:"request_id,omitempty"`
	InputID           string `json:"input_id,omitempty"`
	ExpectedAttemptID string `json:"expected_attempt_id,omitempty"`
	DataBase64        string `json:"data_base64,omitempty"`
	EOF               bool   `json:"eof,omitempty"`
	Signal            string `json:"signal,omitempty"`
	Rows              uint16 `json:"rows,omitempty"`
	Cols              uint16 `json:"cols,omitempty"`
}

type sessionWSResponse struct {
	Type      string         `json:"type"`
	RequestID string         `json:"request_id,omitempty"`
	Event     *session.Event `json:"event,omitempty"`
	Error     string         `json:"error,omitempty"`
}

func (h *SessionHandler) WebSocket(w http.ResponseWriter, r *http.Request) {
	after, _, err := sessionEventQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	backlog, events, cancel, _, err := h.supervisor.Subscribe(mux.Vars(r)["id"], after)
	if err != nil {
		h.writeSessionError(w, "attach_session", err)
		return
	}
	defer cancel()
	if err := proxy.DisableResponseDeadlines(w); err != nil {
		h.logger.Debug("Failed to disable session websocket response deadlines", zap.Error(err))
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	if err := proxy.DisableConnectionDeadlines(conn.UnderlyingConn()); err != nil {
		h.logger.Debug("Failed to disable session websocket deadlines", zap.Error(err))
	}
	var writeMu sync.Mutex
	write := func(value sessionWSResponse) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(value)
	}
	for _, event := range backlog {
		copy := event
		if err := write(sessionWSResponse{Type: "event", Event: &copy}); err != nil {
			return
		}
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case event, ok := <-events:
				if !ok {
					_ = conn.Close()
					return
				}
				copy := event
				if err := write(sessionWSResponse{Type: "event", Event: &copy}); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	for {
		var request sessionWSRequest
		if err := conn.ReadJSON(&request); err != nil {
			return
		}
		requestErr := h.handleWSRequest(mux.Vars(r)["id"], request)
		response := sessionWSResponse{Type: "ack", RequestID: request.RequestID}
		if requestErr != nil {
			response.Type = "error"
			response.Error = requestErr.Error()
		}
		if err := write(response); err != nil {
			return
		}
	}
}

func (h *SessionHandler) handleWSRequest(id string, request sessionWSRequest) error {
	switch request.Type {
	case "input":
		_, err := h.supervisor.WriteInput(id, session.InputRequest{
			InputID: request.InputID, ExpectedAttemptID: request.ExpectedAttemptID,
			DataBase64: request.DataBase64, EOF: request.EOF,
		})
		return err
	case "signal":
		signal, err := parseSessionSignal(request.Signal)
		if err != nil {
			return err
		}
		return h.supervisor.SendSignal(id, request.ExpectedAttemptID, signal)
	case "resize":
		return h.supervisor.ResizeTerminal(id, request.ExpectedAttemptID, request.Rows, request.Cols)
	default:
		return errors.New("unsupported websocket message type")
	}
}

func (h *SessionHandler) writeSessionError(w http.ResponseWriter, action string, err error) {
	switch {
	case errors.Is(err, session.ErrSessionNotFound):
		writeError(w, http.StatusNotFound, "session_not_found", err.Error())
	case errors.Is(err, session.ErrCursorExpired):
		writeError(w, http.StatusGone, "event_cursor_expired", err.Error())
	case errors.Is(err, session.ErrAttemptMismatch),
		errors.Is(err, session.ErrSessionNotRunning),
		errors.Is(err, session.ErrSessionExists),
		errors.Is(err, session.ErrInputAlreadyExists):
		writeError(w, http.StatusConflict, "session_conflict", err.Error())
	case errors.Is(err, process.ErrInputBufferFull):
		writeError(w, http.StatusTooManyRequests, "input_buffer_full", err.Error())
	default:
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "must be") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, action+"_failed", err.Error())
	}
}

func sessionEventQuery(r *http.Request) (int64, int, error) {
	after := int64(0)
	limit := 1000
	var err error
	if value := strings.TrimSpace(r.URL.Query().Get("after")); value != "" {
		after, err = strconv.ParseInt(value, 10, 64)
		if err != nil || after < 0 {
			return 0, 0, errors.New("after must be a non-negative integer")
		}
	}
	if value := strings.TrimSpace(r.URL.Query().Get("limit")); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil || limit <= 0 || limit > 10_000 {
			return 0, 0, errors.New("limit must be between 1 and 10000")
		}
	}
	return after, limit, nil
}

func writeSessionSSE(w http.ResponseWriter, event session.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Seq, event.Type, data)
	return err
}

func parseSessionSignal(value string) (syscall.Signal, error) {
	value = strings.TrimSpace(strings.ToUpper(value))
	value = strings.TrimPrefix(value, "SIG")
	signals := map[string]syscall.Signal{
		"HUP": syscall.SIGHUP, "INT": syscall.SIGINT, "QUIT": syscall.SIGQUIT,
		"KILL": syscall.SIGKILL, "TERM": syscall.SIGTERM, "USR1": syscall.SIGUSR1,
		"USR2": syscall.SIGUSR2, "STOP": syscall.SIGSTOP, "CONT": syscall.SIGCONT,
		"WINCH": syscall.SIGWINCH,
	}
	if signal, ok := signals[value]; ok {
		return signal, nil
	}
	if number, err := strconv.Atoi(value); err == nil && number > 0 && number <= 64 {
		return syscall.Signal(number), nil
	}
	return 0, errors.New("unsupported signal")
}
