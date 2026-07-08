package process

import "time"

// ChannelKind describes how procd communicates with a process capability.
type ChannelKind string

const (
	ChannelKindStdio     ChannelKind = "stdio"
	ChannelKindPTY       ChannelKind = "pty"
	ChannelKindHTTP      ChannelKind = "http"
	ChannelKindWebSocket ChannelKind = "websocket"
)

// ChannelFraming describes how byte streams are turned into process events.
type ChannelFraming string

const (
	ChannelFramingRaw     ChannelFraming = "raw"
	ChannelFramingLine    ChannelFraming = "line"
	ChannelFramingJSONL   ChannelFraming = "jsonl"
	ChannelFramingJSONRPC ChannelFraming = "jsonrpc"
)

// ProcessEventType is the normalized event vocabulary for process sessions.
type ProcessEventType string

const (
	EventTypeProcessStarted ProcessEventType = "process.started"
	EventTypeProcessExited  ProcessEventType = "process.exited"
	EventTypeProcessCrashed ProcessEventType = "process.crashed"
	EventTypeProcessStopped ProcessEventType = "process.stopped"
	EventTypeStdinWrite     ProcessEventType = "stdin.write"
	EventTypeStdoutChunk    ProcessEventType = "stdout.chunk"
	EventTypeStdoutLine     ProcessEventType = "stdout.line"
	EventTypeStderrChunk    ProcessEventType = "stderr.chunk"
	EventTypeStderrLine     ProcessEventType = "stderr.line"
	EventTypePTYInput       ProcessEventType = "pty.input"
	EventTypePTYOutput      ProcessEventType = "pty.output"
	EventTypeHTTPRequest    ProcessEventType = "http.request"
	EventTypeHTTPResponse   ProcessEventType = "http.response"
	EventTypeWebSocketOpen  ProcessEventType = "websocket.open"
	EventTypeWebSocketMsg   ProcessEventType = "websocket.message"
	EventTypeWebSocketClose ProcessEventType = "websocket.close"
	EventTypeError          ProcessEventType = "error"
	EventTypeCursorLost     ProcessEventType = "cursor_lost"
)

// ProcessSessionState is the lifecycle state for a process session.
type ProcessSessionState string

const (
	ProcessSessionStateCreated  ProcessSessionState = "created"
	ProcessSessionStateStarting ProcessSessionState = "starting"
	ProcessSessionStateRunning  ProcessSessionState = "running"
	ProcessSessionStatePaused   ProcessSessionState = "paused"
	ProcessSessionStateStopping ProcessSessionState = "stopping"
	ProcessSessionStateStopped  ProcessSessionState = "stopped"
	ProcessSessionStateKilled   ProcessSessionState = "killed"
	ProcessSessionStateCrashed  ProcessSessionState = "crashed"
)

// ProcessSpec declares a process session and its brokered channels.
type ProcessSpec struct {
	Alias           string             `json:"alias,omitempty"`
	Command         []string           `json:"command"`
	CWD             string             `json:"cwd,omitempty"`
	EnvVars         map[string]string  `json:"env_vars,omitempty"`
	Channels        []ChannelSpec      `json:"channels"`
	Cleanup         ProcessCleanupSpec `json:"cleanup,omitempty"`
	Restart         ProcessRestartSpec `json:"restart,omitempty"`
	EventBufferSize int                `json:"event_buffer_size,omitempty"`
	InputBufferSize int                `json:"input_buffer_size,omitempty"`
}

// ChannelSpec declares one brokered communication channel.
type ChannelSpec struct {
	Name      string                `json:"name"`
	Kind      ChannelKind           `json:"kind"`
	Framing   ChannelFraming        `json:"framing,omitempty"`
	Stdin     bool                  `json:"stdin,omitempty"`
	Stdout    bool                  `json:"stdout,omitempty"`
	Stderr    bool                  `json:"stderr,omitempty"`
	PTYSize   *PTYSize              `json:"pty_size,omitempty"`
	HTTP      *HTTPChannelSpec      `json:"http,omitempty"`
	WebSocket *WebSocketChannelSpec `json:"websocket,omitempty"`
}

// HTTPChannelSpec configures event-driven HTTP requests through procd.
type HTTPChannelSpec struct {
	BaseURL string            `json:"base_url"`
	Headers map[string]string `json:"headers,omitempty"`
	Timeout int32             `json:"timeout_sec,omitempty"`
}

// WebSocketChannelSpec configures an upstream websocket owned by procd.
type WebSocketChannelSpec struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ProcessCleanupSpec controls process-session cleanup.
type ProcessCleanupSpec struct {
	IdleTimeoutSec int32 `json:"idle_timeout_sec,omitempty"`
	TTLSec         int32 `json:"ttl_sec,omitempty"`
}

// ProcessRestartSpec declares restart policy. Only "never" is currently
// accepted; the type is present so the API shape matches the broker contract.
type ProcessRestartSpec struct {
	Policy      string `json:"policy,omitempty"`
	MaxRestarts int32  `json:"max_restarts,omitempty"`
}

// EventLogSnapshot summarizes replay state.
type EventLogSnapshot struct {
	NextSeq   int64 `json:"next_seq"`
	OldestSeq int64 `json:"oldest_seq"`
	Capacity  int   `json:"capacity"`
}

// ProcessSessionSnapshot is the wire representation of a process session.
type ProcessSessionSnapshot struct {
	ID        string              `json:"id"`
	Alias     string              `json:"alias,omitempty"`
	Command   []string            `json:"command"`
	CWD       string              `json:"cwd,omitempty"`
	EnvVars   map[string]string   `json:"env_vars,omitempty"`
	State     ProcessSessionState `json:"state"`
	PID       int                 `json:"pid,omitempty"`
	CreatedAt time.Time           `json:"created_at"`
	StartedAt *time.Time          `json:"started_at,omitempty"`
	ExitedAt  *time.Time          `json:"exited_at,omitempty"`
	ExitCode  *int                `json:"exit_code,omitempty"`
	Channels  []ChannelSpec       `json:"channels"`
	EventLog  EventLogSnapshot    `json:"event_log"`
	Cleanup   ProcessCleanupSpec  `json:"cleanup,omitempty"`
	Restart   ProcessRestartSpec  `json:"restart,omitempty"`
}

// ProcessEvent is a normalized process-session event.
type ProcessEvent struct {
	Seq       int64            `json:"seq"`
	EventID   string           `json:"event_id,omitempty"`
	ProcessID string           `json:"process_id"`
	Channel   string           `json:"channel,omitempty"`
	Type      ProcessEventType `json:"type"`
	Timestamp time.Time        `json:"timestamp"`
	Payload   map[string]any   `json:"payload,omitempty"`
}

// ProcessInputEvent is a client-supplied event sent to a process channel.
type ProcessInputEvent struct {
	EventID string           `json:"event_id"`
	Channel string           `json:"channel"`
	Type    ProcessEventType `json:"type"`
	Payload map[string]any   `json:"payload,omitempty"`
}
