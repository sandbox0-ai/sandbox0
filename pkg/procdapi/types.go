package procdapi

import (
	"encoding/json"
	"time"
)

const (
	StartupPath          = "/startupz"
	ContextsPath         = "/api/v1/contexts"
	FilesPath            = "/api/v1/files"
	FileStatPath         = "/api/v1/files/stat"
	FileListPath         = "/api/v1/files/list"
	FileMovePath         = "/api/v1/files/move"
	FileWatchPath        = "/api/v1/files/watch"
	SandboxStatsPath     = "/api/v1/sandbox/stats"
	SandboxPausePath     = "/api/v1/sandbox/pause"
	SandboxResumePath    = "/api/v1/sandbox/resume"
	LifecycleBarrierPath = "/api/v1/lifecycle/barrier"
)

// StartupResponse identifies the exact Pod process serving the procd HTTP
// socket. It is intentionally independent from runtime assignment readiness.
type StartupResponse struct {
	Status    string `json:"status"`
	Namespace string `json:"namespace"`
	PodName   string `json:"pod_name"`
	PodUID    string `json:"pod_uid"`
}

func ContextPath(contextID string) string {
	return ContextsPath + "/" + contextID
}

func ContextWebSocketPath(contextID string) string {
	return ContextPath(contextID) + "/ws"
}

type ProcessType string

const (
	ProcessTypeREPL ProcessType = "repl"
	ProcessTypeCMD  ProcessType = "cmd"
)

type PTYSize struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

type CreateContextRequest struct {
	Type ProcessType `json:"type"`

	Repl *CreateREPLContextRequest `json:"repl,omitempty"`
	Cmd  *CreateCMDContextRequest  `json:"cmd,omitempty"`

	WaitUntilDone bool `json:"wait_until_done,omitempty"`

	CWD            string            `json:"cwd,omitempty"`
	EnvVars        map[string]string `json:"env_vars,omitempty"`
	PTYSize        *PTYSize          `json:"pty_size,omitempty"`
	IdleTimeoutSec int32             `json:"idle_timeout_sec,omitempty"`
	TTLSec         int32             `json:"ttl_sec,omitempty"`
}

type CreateREPLContextRequest struct {
	Alias      string          `json:"alias"`
	Input      string          `json:"input,omitempty"`
	ReplConfig json.RawMessage `json:"repl_config,omitempty"`
}

type CreateCMDContextRequest struct {
	Command []string `json:"command"`
}

type ContextResponse struct {
	ID        string            `json:"id"`
	Type      ProcessType       `json:"type"`
	Alias     string            `json:"alias"`
	Command   []string          `json:"command,omitempty"`
	CWD       string            `json:"cwd"`
	EnvVars   map[string]string `json:"env_vars"`
	Running   bool              `json:"running"`
	Paused    bool              `json:"paused"`
	CreatedAt string            `json:"created_at"`
	OutputRaw string            `json:"output_raw,omitempty"`
	Stdout    *string           `json:"stdout,omitempty"`
	Stderr    *string           `json:"stderr,omitempty"`
	ExitCode  *int              `json:"exit_code,omitempty"`
	State     string            `json:"state,omitempty"`
}

type WSControlMessage struct {
	Type      string `json:"type"`
	Data      string `json:"data,omitempty"`
	Rows      uint16 `json:"rows,omitempty"`
	Cols      uint16 `json:"cols,omitempty"`
	Signal    string `json:"signal,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type WSOutputMessage struct {
	Type   string `json:"type"`
	Source string `json:"source,omitempty"`
	Data   string `json:"data,omitempty"`
}

type WSDoneMessage struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	State     string `json:"state,omitempty"`
}

// WSMessage is the decoder view used by bridges that consume both output and
// completion messages from a context WebSocket.
type WSMessage struct {
	Type      string `json:"type"`
	Source    string `json:"source,omitempty"`
	Data      string `json:"data,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	State     string `json:"state,omitempty"`
}

type FileType string

const (
	FileTypeFile    FileType = "file"
	FileTypeDir     FileType = "dir"
	FileTypeSymlink FileType = "symlink"
)

type FileInfo struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Type       FileType  `json:"type"`
	Size       int64     `json:"size"`
	Mode       string    `json:"mode"`
	ModTime    time.Time `json:"mod_time"`
	IsLink     bool      `json:"is_link"`
	LinkTarget string    `json:"link_target,omitempty"`
}

type FileListResponse struct {
	Entries []FileInfo `json:"entries"`
}
