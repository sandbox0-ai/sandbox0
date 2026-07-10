package handlers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"go.uber.org/zap"
)

func TestSessionWebSocketDisconnectOnlyDetachesAndReconnectReplays(t *testing.T) {
	supervisor := newHandlerTestSupervisor(t)
	value, _, err := supervisor.Create(session.SessionSpec{
		Command: []string{"/bin/sh", "-c", "while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"},
		IO:      session.IOSpec{Mode: session.IOModePipes},
		Lifecycle: session.LifecycleSpec{
			DesiredState:           session.DesiredStateRunning,
			StopGracePeriodSeconds: 1,
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitHandlerSessionPhase(t, supervisor, value.ID, session.PhaseRunning)
	value, _ = supervisor.Get(value.ID)
	attemptID := value.Attempt.ID

	handler := NewSessionHandler(supervisor, zap.NewNop())
	router := mux.NewRouter()
	router.HandleFunc("/sessions/{id}/ws", handler.WebSocket)
	router.HandleFunc("/sessions/{id}/inputs", handler.WriteInput).Methods(http.MethodPost)
	server := httptest.NewServer(router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/sessions/" + value.ID + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	time.Sleep(20 * time.Millisecond)
	afterDetach, err := supervisor.Get(value.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterDetach.Phase != session.PhaseRunning || afterDetach.Attempt.ID != attemptID {
		t.Fatalf("disconnect changed session: %#v", afterDetach)
	}
	cursor := afterDetach.Cursor.Latest

	input := session.InputRequest{
		InputID: "input-after-detach", ExpectedAttemptID: attemptID,
		DataBase64: base64.StdEncoding.EncodeToString([]byte("hello\n")),
	}
	body, _ := json.Marshal(input)
	resp, err := http.Post(server.URL+"/sessions/"+value.ID+"/inputs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("input status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	reconnectURL := wsURL + "?after=" + strconv.FormatInt(cursor, 10)
	conn, _, err = websocket.DefaultDialer.Dial(reconnectURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var output string
	for output == "" {
		var message sessionWSResponse
		if err := conn.ReadJSON(&message); err != nil {
			t.Fatal(err)
		}
		if message.Event == nil || message.Event.Type != "output" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(message.Event.DataBase64)
		if err != nil {
			t.Fatal(err)
		}
		output += string(data)
	}
	if output != "echo:hello\n" {
		t.Fatalf("replayed output = %q, want %q", output, "echo:hello\n")
	}
	current, _ := supervisor.Get(value.ID)
	if current.Attempt.ID != attemptID {
		t.Fatalf("reconnect changed attempt: %s -> %s", attemptID, current.Attempt.ID)
	}
}

func TestSessionEventStreamUsesCursorIDs(t *testing.T) {
	supervisor := newHandlerTestSupervisor(t)
	value, _, err := supervisor.Create(session.SessionSpec{Command: []string{"/bin/printf", "hello"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitHandlerSessionPhase(t, supervisor, value.ID, session.PhaseExited)
	handler := NewSessionHandler(supervisor, zap.NewNop())
	router := mux.NewRouter()
	router.HandleFunc("/sessions/{id}/events/stream", handler.EventStream)
	server := httptest.NewServer(router)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/sessions/"+value.ID+"/events/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buffer := make([]byte, 4096)
	n, err := resp.Body.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	payload := string(buffer[:n])
	if !strings.Contains(payload, "id: 1\n") || !strings.Contains(payload, "data: {") {
		t.Fatalf("SSE payload = %q", payload)
	}
}

func TestSessionHandlerRejectsExpiredCursor(t *testing.T) {
	store, err := session.NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Initialize("sandbox-1", 1, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	value, _, err := supervisor.Create(session.SessionSpec{
		Command:        []string{"/bin/printf", "hello"},
		EventRetention: session.EventRetentionSpec{MaxBytes: 250, MaxAgeSeconds: 3600},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	waitHandlerSessionPhase(t, supervisor, value.ID, session.PhaseExited)
	handler := NewSessionHandler(supervisor, zap.NewNop())
	req := httptest.NewRequest(http.MethodGet, "/sessions/"+url.PathEscape(value.ID)+"/events?after=1", nil)
	req = mux.SetURLVars(req, map[string]string{"id": value.ID})
	recorder := httptest.NewRecorder()
	handler.Events(recorder, req)
	if recorder.Code != http.StatusGone {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusGone, recorder.Body.String())
	}
}

func newHandlerTestSupervisor(t *testing.T) *session.Supervisor {
	t.Helper()
	store, err := session.NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Initialize("sandbox-1", 1, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, value := range supervisor.List() {
			_ = supervisor.Delete(value.ID)
		}
		_ = supervisor.Close()
	})
	return supervisor
}

func waitHandlerSessionPhase(t *testing.T, supervisor *session.Supervisor, id string, expected session.Phase) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		value, err := supervisor.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if value.Phase == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	value, _ := supervisor.Get(id)
	t.Fatal(fmt.Sprintf("phase = %s, want %s; session=%#v", value.Phase, expected, value))
}
