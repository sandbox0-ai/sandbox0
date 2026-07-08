package process

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProcessSessionStdioWritesInputAndSeparatesOutput(t *testing.T) {
	session := startTestSession(t, ProcessSpec{
		Command: []string{"/bin/sh", "-c", "read line; echo stdout:$line; echo stderr:$line >&2"},
		Channels: []ChannelSpec{{
			Name:    "rpc",
			Kind:    ChannelKindStdio,
			Framing: ChannelFramingLine,
			Stdin:   true,
			Stdout:  true,
			Stderr:  true,
		}},
	})
	defer session.Stop()

	events, cancel := session.Subscribe(0)
	defer cancel()

	accepted, err := session.HandleInput(ProcessInputEvent{
		EventID: "evt-input",
		Channel: "rpc",
		Type:    EventTypeStdinWrite,
		Payload: map[string]any{"data": "hello\n"},
	})
	if err != nil {
		t.Fatalf("HandleInput() error = %v", err)
	}
	if accepted.Type != EventTypeStdinWrite {
		t.Fatalf("accepted type = %s, want %s", accepted.Type, EventTypeStdinWrite)
	}

	stdout, stderr := waitForStdioLines(t, events)
	if stdout.Channel != "rpc" || stdout.Payload["data"] != "stdout:hello" {
		t.Fatalf("stdout event = %#v", stdout)
	}
	if stderr.Channel != "rpc" || stderr.Payload["data"] != "stderr:hello" {
		t.Fatalf("stderr event = %#v", stderr)
	}
}

func TestProcessSessionSubscriptionCloseDoesNotStopProcess(t *testing.T) {
	session := startTestSession(t, ProcessSpec{
		Command: []string{"/bin/sh", "-c", "while read line; do echo $line; done"},
		Channels: []ChannelSpec{{
			Name:    "rpc",
			Kind:    ChannelKindStdio,
			Framing: ChannelFramingLine,
			Stdin:   true,
			Stdout:  true,
		}},
	})
	defer session.Stop()

	_, cancel := session.Subscribe(0)
	cancel()

	if _, err := session.HandleInput(ProcessInputEvent{
		EventID: "evt-after-cancel",
		Channel: "rpc",
		Type:    EventTypeStdinWrite,
		Payload: map[string]any{"data": "still-running\n"},
	}); err != nil {
		t.Fatalf("HandleInput() after cancel error = %v", err)
	}

	events, replayCancel := session.Subscribe(0)
	defer replayCancel()
	output := waitForEventType(t, events, EventTypeStdoutLine)
	if output.Payload["data"] != "still-running" {
		t.Fatalf("output data = %#v, want still-running", output.Payload["data"])
	}
	if got := session.Snapshot().State; got != ProcessSessionStateRunning {
		t.Fatalf("state = %s, want running", got)
	}
}

func TestProcessSessionDuplicateEventIDIsIdempotent(t *testing.T) {
	session := startTestSession(t, ProcessSpec{
		Command: []string{"/bin/sh", "-c", "while read line; do echo $line; done"},
		Channels: []ChannelSpec{{
			Name:   "rpc",
			Kind:   ChannelKindStdio,
			Stdin:  true,
			Stdout: true,
		}},
	})
	defer session.Stop()

	event := ProcessInputEvent{
		EventID: "evt-repeat",
		Channel: "rpc",
		Type:    EventTypeStdinWrite,
		Payload: map[string]any{"data": "hello\n"},
	}
	first, err := session.HandleInput(event)
	if err != nil {
		t.Fatalf("first HandleInput() error = %v", err)
	}
	second, err := session.HandleInput(event)
	if err != nil {
		t.Fatalf("second HandleInput() error = %v", err)
	}
	if first.Seq != second.Seq {
		t.Fatalf("duplicate seq = %d, want %d", second.Seq, first.Seq)
	}

	event.Payload = map[string]any{"data": "different\n"}
	if _, err := session.HandleInput(event); err != ErrDuplicateEventID {
		t.Fatalf("changed duplicate error = %v, want ErrDuplicateEventID", err)
	}
}

func TestEventLogReportsCursorLost(t *testing.T) {
	log := NewEventLog(2)
	log.Publish(ProcessEvent{ProcessID: "proc", Type: EventTypeStdoutLine})
	log.Publish(ProcessEvent{ProcessID: "proc", Type: EventTypeStdoutLine})
	log.Publish(ProcessEvent{ProcessID: "proc", Type: EventTypeStdoutLine})

	events, cancel := log.Subscribe(1)
	defer cancel()
	event := waitForEventType(t, events, EventTypeCursorLost)
	if event.Payload["oldest_seq"] == nil {
		t.Fatalf("cursor_lost payload missing oldest_seq: %#v", event.Payload)
	}
}

func TestProcessSessionHTTPChannelPublishesResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/work" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	session := startTestSession(t, ProcessSpec{
		Command: []string{"/bin/sleep", "5"},
		Channels: []ChannelSpec{{
			Name: "api",
			Kind: ChannelKindHTTP,
			HTTP: &HTTPChannelSpec{BaseURL: upstream.URL},
		}},
	})
	defer session.Stop()

	events, cancel := session.Subscribe(0)
	defer cancel()

	if _, err := session.HandleInput(ProcessInputEvent{
		EventID: "evt-http",
		Channel: "api",
		Type:    EventTypeHTTPRequest,
		Payload: map[string]any{
			"method": "POST",
			"path":   "/work",
			"body":   "payload",
		},
	}); err != nil {
		t.Fatalf("HandleInput(http.request) error = %v", err)
	}
	response := waitForEventType(t, events, EventTypeHTTPResponse)
	if response.Channel != "api" || response.Payload["body"] != "ok" {
		t.Fatalf("http response event = %#v", response)
	}
}

func startTestSession(t *testing.T, spec ProcessSpec) *Session {
	t.Helper()
	session, err := NewSession("proc-test", spec)
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	if err := session.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	return session
}

func waitForEventType(t *testing.T, events <-chan ProcessEvent, eventType ProcessEventType) ProcessEvent {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("event stream closed before %s", eventType)
			}
			if event.Type == eventType {
				return event
			}
		case <-timeout:
			seen := make([]ProcessEvent, 0)
			for {
				select {
				case event := <-events:
					seen = append(seen, event)
				default:
					body, _ := json.Marshal(seen)
					t.Fatalf("timed out waiting for %s; buffered=%s", eventType, string(body))
				}
			}
		}
	}
}

func waitForStdioLines(t *testing.T, events <-chan ProcessEvent) (ProcessEvent, ProcessEvent) {
	t.Helper()
	timeout := time.After(3 * time.Second)
	var stdout ProcessEvent
	var stderr ProcessEvent
	for stdout.Type == "" || stderr.Type == "" {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("event stream closed before stdout/stderr lines")
			}
			switch event.Type {
			case EventTypeStdoutLine:
				stdout = event
			case EventTypeStderrLine:
				stderr = event
			}
		case <-timeout:
			t.Fatalf("timed out waiting for stdout/stderr lines; stdout=%#v stderr=%#v", stdout, stderr)
		}
	}
	return stdout, stderr
}
