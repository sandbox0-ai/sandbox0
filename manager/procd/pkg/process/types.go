// Package process provides process management for Procd.
package process

import (
	"syscall"
	"time"
)

// ProcessType defines the type of process.
type ProcessType string

const (
	// ProcessTypeREPL represents a REPL process (Python, Node, Bash, Zsh, Ruby, Lua, PHP, R, Perl, etc.)
	ProcessTypeREPL ProcessType = "repl"
	// ProcessTypeCMD represents a one-time command execution (e.g., /bin/ls, /bin/cat)
	ProcessTypeCMD ProcessType = "cmd"
)

// OutputSource defines the source of process output.
type OutputSource string

const (
	OutputSourceStdout OutputSource = "stdout"
	OutputSourceStderr OutputSource = "stderr"
	OutputSourcePTY    OutputSource = "pty"
	OutputSourcePrompt OutputSource = "prompt"
)

// ProcessState represents the current state of a process.
type ProcessState string

const (
	ProcessStateCreated  ProcessState = "created"
	ProcessStateStarting ProcessState = "starting"
	ProcessStateRunning  ProcessState = "running"
	ProcessStatePaused   ProcessState = "paused"
	ProcessStateStopped  ProcessState = "stopped"
	ProcessStateKilled   ProcessState = "killed"
	ProcessStateCrashed  ProcessState = "crashed"
)

// PTYSize represents terminal dimensions.
type PTYSize struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

// ProcessConfig holds configuration for creating a process.
type ProcessConfig struct {
	Type     ProcessType       `json:"type"`
	Language string            `json:"language"` // For REPL: python, node, bash, zsh, ruby, lua, php, r, perl, etc.
	Code     string            `json:"code"`     // For REPL: code to execute
	Command  []string          `json:"command"`  // For CMD: command path and arguments, e.g., ["/bin/ls", "-la"]
	CWD      string            `json:"cwd"`
	EnvVars  map[string]string `json:"env_vars"`
	PTYSize  *PTYSize          `json:"pty_size"`
	Term     string            `json:"term"`
}

// ProcessOutput represents output from a process.
type ProcessOutput struct {
	Source OutputSource `json:"source"`
	Data   []byte       `json:"data"`
}

// ExecutionResult represents the result of code/command execution.
type ExecutionResult struct {
	Output   []byte `json:"output"`
	Error    string `json:"error,omitempty"`
	ExitCode int    `json:"exit_code"`
}

// ResourceUsage represents resource consumption of a process.
// All fields are best-effort; unavailable stats will be zero or -1 for CPU percent.
type ResourceUsage struct {
	// Process-level stats (from /proc/[pid]/*)
	// CPUPercent is the CPU usage percentage since last sample.
	// Returns -1 on first call (no previous sample available).
	CPUPercent float64 `json:"cpu_percent"`
	// MemoryRSS is the Resident Set Size - actual physical memory used by process tree.
	MemoryRSS int64 `json:"memory_rss"`
	// MemoryVMS is the Virtual Memory Size - total virtual address space.
	MemoryVMS int64 `json:"memory_vms"`
	// OpenFiles is the number of open file descriptors.
	OpenFiles int `json:"open_files"`
	// ThreadCount is the number of threads in the process tree.
	ThreadCount int `json:"thread_count"`

	// Container-level stats (from cgroup, if available)
	// ContainerMemoryUsage is the total memory used by the container.
	ContainerMemoryUsage int64 `json:"container_memory_usage,omitempty"`
	// ContainerMemoryLimit is the memory limit for the container, 0 if unlimited.
	ContainerMemoryLimit int64 `json:"container_memory_limit,omitempty"`
	// ContainerMemoryWorkingSet is the non-reclaimable memory (used for OOM decisions).
	ContainerMemoryWorkingSet int64 `json:"container_memory_working_set,omitempty"`

	// I/O stats (from /proc/[pid]/io, may be unavailable)
	IOReadBytes  int64 `json:"io_read_bytes,omitempty"`
	IOWriteBytes int64 `json:"io_write_bytes,omitempty"`

	// Deprecated: Use MemoryRSS instead. Kept for backward compatibility.
	MemoryBytes int64 `json:"memory_bytes"`
}

// ExitEvent captures details about a process exit.
type ExitEvent struct {
	ProcessID     string
	ProcessType   ProcessType
	PID           int
	ExitCode      int
	Duration      time.Duration
	State         ProcessState
	StdoutPreview string
	StderrPreview string
	Config        ProcessConfig
}

// StartEvent captures details about a process start.
type StartEvent struct {
	ProcessID   string
	ProcessType ProcessType
	PID         int
	StartTime   time.Time
	State       ProcessState
	Config      ProcessConfig
}

// ExitHandler is a function that handles process exit events.
type ExitHandler func(ExitEvent)

// StartHandler is a function that handles process start events.
type StartHandler func(StartEvent)

// Process interface defines the contract for all process types.
type Process interface {
	// Identity
	ID() string
	Type() ProcessType
	PID() int

	// Lifecycle
	Start() error
	Stop() error
	Restart() error
	IsRunning() bool
	State() ProcessState
	SetStartHandler(StartHandler)
	SetExitHandler(ExitHandler)

	// Pause/Resume - sends SIGSTOP/SIGCONT to process group
	Pause() error
	Resume() error
	IsPaused() bool

	// I/O
	WriteInput(data []byte) error
	ReadOutput() <-chan ProcessOutput
	ResizePTY(size PTYSize) error
	SendSignal(sig syscall.Signal) error

	// Status
	ExitCode() (int, error)
	ResourceUsage() ResourceUsage
}

type CodeInterpreter interface {
	Process
	ExecuteCode(string) (*ExecutionResult, error)
}
