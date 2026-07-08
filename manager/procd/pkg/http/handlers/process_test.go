package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"go.uber.org/zap"
)

func TestProcessHandlerListReturnsEmptyArray(t *testing.T) {
	handler := NewProcessHandler(process.NewSessionManager(), zap.NewNop())
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/processes", nil)

	handler.List(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"processes":[]`) {
		t.Fatalf("response = %s, want empty processes array", recorder.Body.String())
	}
}

func TestProcessHandlerStreamEventsReplaysClosedSession(t *testing.T) {
	manager := process.NewSessionManager()
	session, err := manager.CreateSession(process.ProcessSpec{
		Command: []string{"/bin/sh", "-c", "echo hello"},
		Channels: []process.ChannelSpec{{
			Name:    "rpc",
			Kind:    process.ChannelKindStdio,
			Framing: process.ChannelFramingLine,
			Stdout:  true,
		}},
	})
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	waitForProcessState(t, session, process.ProcessSessionStateStopped)

	handler := NewProcessHandler(manager, zap.NewNop())
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/processes/"+session.ID()+"/events?cursor=0", nil)
	req = mux.SetURLVars(req, map[string]string{"id": session.ID()})

	handler.StreamEvents(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: stdout.line") || !strings.Contains(body, `"data":"hello"`) {
		t.Fatalf("SSE body = %s, want replayed stdout.line", body)
	}
}

func waitForProcessState(t *testing.T, session *process.Session, state process.ProcessSessionState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if session.Snapshot().State == state {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("state = %s, want %s", session.Snapshot().State, state)
}
