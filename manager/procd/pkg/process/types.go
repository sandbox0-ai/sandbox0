// Package process provides process management for Procd.
package process

import (
	"fmt"
	"os"
	"sync"
	"syscall"
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
	Type        ProcessType       `json:"type"`
	Language    string            `json:"language"` // For REPL: python, node, bash, zsh, ruby, lua, php, r, perl, etc.
	Command     []string          `json:"command"`  // For CMD: command path and arguments, e.g., ["/bin/ls", "-la"]
	CWD         string            `json:"cwd"`
	EnvVars     map[string]string `json:"env_vars"`
	AutoRestart bool              `json:"auto_restart"`
	PTYSize     *PTYSize          `json:"pty_size"`
	Term        string            `json:"term"`
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

// ExitHandler is a function that handles process exit events.
type ExitHandler func(*ProcessConfig)

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
	SetExitHandler(ExitHandler)

	// Pause/Resume - sends SIGSTOP/SIGCONT to process group
	Pause() error
	Resume() error
	IsPaused() bool

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
// It's safe to call multiple times or on already-closed channels.
func (mc *MultiplexedChannel[T]) Unsubscribe(sub chan T) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for i, s := range mc.subscribers {
		if s == sub {
			mc.subscribers = append(mc.subscribers[:i], mc.subscribers[i+1:]...)
			// Safely close the channel - use recover to handle already-closed channels
			defer func() {
				recover() // Ignore panic from closing an already-closed channel
			}()
			close(sub)
			return
		}
	}
}

// Publish sends an event to all subscribers.
// It's safe to call after Close - events will be silently dropped.
func (mc *MultiplexedChannel[T]) Publish(event T) {
	mc.mu.RLock()
	closed := mc.closed
	mc.mu.RUnlock()

	if closed {
		return // Channel is closed, drop the event
	}

	select {
	case mc.Source <- event:
	default:
		// Source buffer full
	}
}

// Close closes the multiplexed channel.
// It's safe to call multiple times.
func (mc *MultiplexedChannel[T]) Close() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return // Already closed
	}

	mc.closed = true
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

	// cpuTracker tracks CPU time between samples for percentage calculation
	cpuTracker *cpuTracker

	exitHandler ExitHandler

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
		cpuTracker:      newCPUTracker(),
	}
}

// SetExitHandler sets the exit handler.
func (bp *BaseProcess) SetExitHandler(handler ExitHandler) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.exitHandler = handler
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

// IsPaused returns true if the process is paused.
func (bp *BaseProcess) IsPaused() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.state == ProcessStatePaused
}

// Pause sends SIGSTOP to the process group to pause the process and all its children.
func (bp *BaseProcess) Pause() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.state != ProcessStateRunning {
		return ErrProcessNotRunning
	}

	if bp.pid <= 0 {
		return ErrProcessNotRunning
	}

	// Send SIGSTOP to the entire process group (negative PID)
	// This ensures all child processes are also paused
	if err := syscall.Kill(-bp.pid, syscall.SIGSTOP); err != nil {
		// If process group signal fails, try sending to the process directly
		if err := syscall.Kill(bp.pid, syscall.SIGSTOP); err != nil {
			return fmt.Errorf("%w: %v", ErrPauseFailed, err)
		}
	}

	bp.state = ProcessStatePaused
	return nil
}

// Resume sends SIGCONT to the process group to resume the process and all its children.
func (bp *BaseProcess) Resume() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.state != ProcessStatePaused {
		return ErrProcessNotPaused
	}

	if bp.pid <= 0 {
		return ErrProcessNotRunning
	}

	// Send SIGCONT to the entire process group (negative PID)
	// This ensures all child processes are also resumed
	if err := syscall.Kill(-bp.pid, syscall.SIGCONT); err != nil {
		// If process group signal fails, try sending to the process directly
		if err := syscall.Kill(bp.pid, syscall.SIGCONT); err != nil {
			return fmt.Errorf("%w: %v", ErrResumeFailed, err)
		}
	}

	bp.state = ProcessStateRunning
	return nil
}

// ExitCode returns the process exit code.
func (bp *BaseProcess) ExitCode() (int, error) {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.exitCode, nil
}

// ResourceUsage returns resource usage statistics.
// This method reads from /proc and /sys/fs/cgroup pseudo-filesystems,
// which are available even in scratch containers as they are provided by the kernel.
// All stats are best-effort; failures are silently ignored and result in zero values.
func (bp *BaseProcess) ResourceUsage() ResourceUsage {
	bp.mu.RLock()
	pid := bp.pid
	bp.mu.RUnlock()

	usage := ResourceUsage{
		CPUPercent: -1, // Indicates no sample available
	}

	// If process is not running, return empty stats
	if pid <= 0 {
		return usage
	}

	// Aggregate stats for the process and all its children
	memStats, cpuStats, threads, openFiles, err := AggregateProcessTreeStats(pid)
	if err == nil {
		usage.MemoryRSS = memStats.RSS
		usage.MemoryVMS = memStats.VMS
		usage.MemoryBytes = memStats.RSS // Backward compatibility
		usage.ThreadCount = threads
		usage.OpenFiles = openFiles
	}

	// Calculate CPU percentage from delta since last sample
	if cpuStats != nil && bp.cpuTracker != nil {
		usage.CPUPercent = bp.cpuTracker.CalculateCPUPercent(cpuStats)
	}

	// Try to read I/O stats (may fail due to permissions)
	if ioStats, err := defaultProcReader.ReadIOStats(pid); err == nil {
		usage.IOReadBytes = ioStats.ReadBytes
		usage.IOWriteBytes = ioStats.WriteBytes
	}

	// Read container-level stats from cgroup (works in both runc and Kata)
	if containerMem, err := defaultCgroupReader.ReadContainerMemoryStats(); err == nil {
		usage.ContainerMemoryUsage = containerMem.Usage
		usage.ContainerMemoryLimit = containerMem.Limit
		usage.ContainerMemoryWorkingSet = containerMem.WorkingSet
	}

	return usage
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
