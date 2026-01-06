// Package process provides process management for Procd.
package process

import (
	"os"
	"sync"
	"time"
)

// ProcessType defines the type of process.
type ProcessType string

const (
	// ProcessTypeREPL represents a REPL process (Python, Node, Bash, Zsh, etc.)
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
)

// ProcessState represents the current state of a process.
type ProcessState string

const (
	ProcessStateCreated  ProcessState = "created"
	ProcessStateStarting ProcessState = "starting"
	ProcessStateRunning  ProcessState = "running"
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
	Type        ProcessType       `json:"type"`
	Language    string            `json:"language"` // For REPL: python, node, bash, zsh, etc.
	Command     []string          `json:"command"`  // For CMD: command path and arguments, e.g., ["/bin/ls", "-la"]
	CWD         string            `json:"cwd"`
	EnvVars     map[string]string `json:"env_vars"`
	AutoRestart bool              `json:"auto_restart"`
	PTYSize     *PTYSize          `json:"pty_size"`
	Term        string            `json:"term"`
}

// ProcessOutput represents output from a process.
type ProcessOutput struct {
	Timestamp time.Time    `json:"timestamp"`
	Source    OutputSource `json:"source"`
	Data      []byte       `json:"data"`
}

// ExecutionResult represents the result of code/command execution.
type ExecutionResult struct {
	Output   []byte `json:"output"`
	Error    string `json:"error,omitempty"`
	ExitCode int    `json:"exit_code"`
}

// ResourceUsage represents resource consumption of a process.
type ResourceUsage struct {
	CPUPercent  float64 `json:"cpu_percent"`
	MemoryBytes int64   `json:"memory_bytes"`
	OpenFiles   int     `json:"open_files"`
	ThreadCount int     `json:"thread_count"`
}

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

	// I/O
	WriteInput(data []byte) error
	ReadOutput() <-chan ProcessOutput

	// Status
	ExitCode() (int, error)
	ResourceUsage() ResourceUsage
}

// MultiplexedChannel provides a fan-out mechanism for process output.
// Multiple subscribers can receive the same events.
type MultiplexedChannel[T any] struct {
	mu          sync.RWMutex
	Source      chan T
	subscribers []chan T
	bufferSize  int
	closed      bool
}

// NewMultiplexedChannel creates a new multiplexed channel.
func NewMultiplexedChannel[T any](bufferSize int) *MultiplexedChannel[T] {
	mc := &MultiplexedChannel[T]{
		Source:      make(chan T, bufferSize),
		subscribers: make([]chan T, 0),
		bufferSize:  bufferSize,
	}

	go mc.dispatch()

	return mc
}

func (mc *MultiplexedChannel[T]) dispatch() {
	for event := range mc.Source {
		mc.mu.RLock()
		for _, sub := range mc.subscribers {
			select {
			case sub <- event:
			default:
				// Subscriber buffer full, drop event
			}
		}
		mc.mu.RUnlock()
	}

	// Source closed, close all subscribers
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, sub := range mc.subscribers {
		close(sub)
	}
	mc.closed = true
}

// Fork creates a new subscription to the channel.
// Returns the subscription channel and a cancel function.
func (mc *MultiplexedChannel[T]) Fork() (<-chan T, func()) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		ch := make(chan T)
		close(ch)
		return ch, func() {}
	}

	sub := make(chan T, mc.bufferSize)
	mc.subscribers = append(mc.subscribers, sub)

	cancel := func() {
		mc.Unsubscribe(sub)
	}

	return sub, cancel
}

// Unsubscribe removes a subscriber from the channel.
func (mc *MultiplexedChannel[T]) Unsubscribe(sub chan T) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for i, s := range mc.subscribers {
		if s == sub {
			mc.subscribers = append(mc.subscribers[:i], mc.subscribers[i+1:]...)
			close(sub)
			return
		}
	}
}

// Publish sends an event to all subscribers.
func (mc *MultiplexedChannel[T]) Publish(event T) {
	select {
	case mc.Source <- event:
	default:
		// Source buffer full
	}
}

// Close closes the multiplexed channel.
func (mc *MultiplexedChannel[T]) Close() {
	close(mc.Source)
}

// SubscriberCount returns the number of active subscribers.
func (mc *MultiplexedChannel[T]) SubscriberCount() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.subscribers)
}

// BaseProcess provides common functionality for all process types.
type BaseProcess struct {
	id              string
	processType     ProcessType
	config          ProcessConfig
	state           ProcessState
	pid             int
	exitCode        int
	pty             *os.File
	outputMultiplex *MultiplexedChannel[ProcessOutput]

	mu sync.RWMutex
}

// NewBaseProcess creates a new base process.
func NewBaseProcess(id string, processType ProcessType, config ProcessConfig) *BaseProcess {
	return &BaseProcess{
		id:              id,
		processType:     processType,
		config:          config,
		state:           ProcessStateCreated,
		outputMultiplex: NewMultiplexedChannel[ProcessOutput](64),
	}
}

// ID returns the process ID.
func (bp *BaseProcess) ID() string {
	return bp.id
}

// Type returns the process type.
func (bp *BaseProcess) Type() ProcessType {
	return bp.processType
}

// PID returns the system process ID.
func (bp *BaseProcess) PID() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.pid
}

// State returns the current process state.
func (bp *BaseProcess) State() ProcessState {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.state
}

// IsRunning returns true if the process is running.
func (bp *BaseProcess) IsRunning() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.state == ProcessStateRunning
}

// ExitCode returns the process exit code.
func (bp *BaseProcess) ExitCode() (int, error) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.exitCode, nil
}

// ResourceUsage returns resource usage statistics.
func (bp *BaseProcess) ResourceUsage() ResourceUsage {
	// TODO: Implement actual resource monitoring
	return ResourceUsage{}
}

// ReadOutput returns a channel for reading process output.
func (bp *BaseProcess) ReadOutput() <-chan ProcessOutput {
	ch, _ := bp.outputMultiplex.Fork()
	return ch
}

// WriteInput writes data to the process input.
func (bp *BaseProcess) WriteInput(data []byte) error {
	bp.mu.RLock()
	pty := bp.pty
	bp.mu.RUnlock()

	if pty == nil {
		return ErrProcessNotRunning
	}

	_, err := pty.Write(data)
	return err
}

// SetState updates the process state.
func (bp *BaseProcess) SetState(state ProcessState) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.state = state
}

// SetPTY sets the PTY file descriptor.
func (bp *BaseProcess) SetPTY(pty *os.File) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.pty = pty
}

// SetPID sets the system process ID.
func (bp *BaseProcess) SetPID(pid int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.pid = pid
}

// SetExitCode sets the exit code.
func (bp *BaseProcess) SetExitCode(code int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.exitCode = code
}

// PublishOutput publishes output to all subscribers.
func (bp *BaseProcess) PublishOutput(output ProcessOutput) {
	bp.outputMultiplex.Publish(output)
}

// CloseOutput closes the output channel.
func (bp *BaseProcess) CloseOutput() {
	bp.outputMultiplex.Close()
}

// GetPTY returns the PTY file descriptor.
func (bp *BaseProcess) GetPTY() *os.File {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.pty
}

// GetConfig returns the process configuration.
func (bp *BaseProcess) GetConfig() ProcessConfig {
	return bp.config
}
