package process

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
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
	Type       ProcessType       `json:"type"`
	Language   string            `json:"language"` // For REPL: python, node, bash, zsh, ruby, lua, php, r, perl, etc.
	Code       string            `json:"code"`     // For REPL: code to execute
	Command    []string          `json:"command"`  // For CMD: command path and arguments, e.g., ["/bin/ls", "-la"]
	CWD        string            `json:"cwd"`
	EnvVars    map[string]string `json:"env_vars"`
	PTYSize    *PTYSize          `json:"pty_size"`
	Term       string            `json:"term"`
	BufferSize int               `json:"buffer_size"` // Number of messages to buffer for the output multiplexer, default is 64 if not set
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
	IsFinished() bool
	State() ProcessState
	AddStartHandler(StartHandler)
	AddExitHandler(ExitHandler)

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
	startTime       time.Time

	// cpuTracker tracks CPU time between samples for percentage calculation
	cpuTracker *cpuTracker

	exitHandlers  []ExitHandler
	startHandlers []StartHandler

	mu sync.RWMutex

	inputQueue      chan []byte
	inputReady      chan struct{}
	inputStop       chan struct{}
	inputReadyOnce  sync.Once
	inputWriterOnce sync.Once
	inputStopOnce   sync.Once
}

// NewBaseProcess creates a new base process.
func NewBaseProcess(id string, processType ProcessType, config ProcessConfig) *BaseProcess {
	if config.BufferSize == 0 {
		config.BufferSize = 64
	}
	bp := &BaseProcess{
		id:              id,
		processType:     processType,
		config:          config,
		state:           ProcessStateCreated,
		outputMultiplex: NewMultiplexedChannel[ProcessOutput](config.BufferSize),
		cpuTracker:      newCPUTracker(),
		inputQueue:      make(chan []byte, config.BufferSize),
		inputReady:      make(chan struct{}),
		inputStop:       make(chan struct{}),
	}
	if processType != ProcessTypeREPL {
		close(bp.inputReady)
	}
	return bp
}

// AddExitHandler appends an exit handler to the handler chain.
// Handlers are executed in the order they were added.
func (bp *BaseProcess) AddExitHandler(handler ExitHandler) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if handler != nil {
		bp.exitHandlers = append(bp.exitHandlers, handler)
	}
}

// AddStartHandler appends a start handler to the handler chain.
// Handlers are executed in the order they were added.
func (bp *BaseProcess) AddStartHandler(handler StartHandler) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if handler != nil {
		bp.startHandlers = append(bp.startHandlers, handler)
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

// IsPaused returns true if the process is paused.
func (bp *BaseProcess) IsPaused() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.state == ProcessStatePaused
}

func (bp *BaseProcess) IsFinished() bool {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.state == ProcessStateStopped || bp.state == ProcessStateKilled || bp.state == ProcessStateCrashed
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

	if len(data) == 0 {
		return nil
	}
	if pty == nil {
		return ErrProcessNotRunning
	}
	if bp.IsFinished() {
		return ErrProcessFinished
	}

	payload := make([]byte, len(data))
	copy(payload, data)

	select {
	case bp.inputQueue <- payload:
		return nil
	default:
		return ErrInputBufferFull
	}
}

// ResizePTY resizes the attached PTY, if present.
func (bp *BaseProcess) ResizePTY(size PTYSize) error {
	ptyFile := bp.GetPTY()
	if ptyFile == nil {
		return ErrPTYNotAvailable
	}

	if size.Rows == 0 || size.Cols == 0 {
		return fmt.Errorf("%w: rows and cols must be > 0", ErrInvalidPTYSize)
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

// SendSignal sends a signal to the process group, falling back to the PID.
func (bp *BaseProcess) SendSignal(sig syscall.Signal) error {
	bp.mu.RLock()
	pid := bp.pid
	state := bp.state
	bp.mu.RUnlock()

	if pid <= 0 || (state != ProcessStateRunning && state != ProcessStatePaused) {
		return ErrProcessNotRunning
	}

	if err := syscall.Kill(-pid, sig); err != nil {
		if err := syscall.Kill(pid, sig); err != nil {
			return fmt.Errorf("%w: %v", ErrSignalFailed, err)
		}
	}

	return nil
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
	bp.startInputWriter(pty)
}

func (bp *BaseProcess) signalInputReady() {
	bp.inputReadyOnce.Do(func() {
		close(bp.inputReady)
	})
}

// SignalInputReady marks the process as ready for input.
func (bp *BaseProcess) SignalInputReady() {
	bp.signalInputReady()
}

func (bp *BaseProcess) stopInputWriter() {
	bp.inputStopOnce.Do(func() {
		close(bp.inputStop)
	})
}

func (bp *BaseProcess) startInputWriter(pty *os.File) {
	bp.inputWriterOnce.Do(func() {
		go func() {
			select {
			case <-bp.inputReady:
			case <-bp.inputStop:
				return
			}
			for {
				select {
				case <-bp.inputStop:
					return
				case data := <-bp.inputQueue:
					if len(data) == 0 {
						continue
					}
					_, _ = pty.Write(data)
				}
			}
		}()
	})
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

// SetStartTime records when the process starts.
func (bp *BaseProcess) SetStartTime(start time.Time) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.startTime = start
}

// StartTime returns the recorded start time.
func (bp *BaseProcess) StartTime() time.Time {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.startTime
}

// NotifyExit invokes all exit handlers in the order they were added.
func (bp *BaseProcess) NotifyExit(event ExitEvent) {
	bp.mu.RLock()
	handlers := make([]ExitHandler, len(bp.exitHandlers))
	copy(handlers, bp.exitHandlers)
	bp.mu.RUnlock()

	for _, handler := range handlers {
		if handler != nil {
			handler(event)
		}
	}
}

// NotifyStart invokes all start handlers in the order they were added.
func (bp *BaseProcess) NotifyStart(event StartEvent) {
	bp.mu.RLock()
	handlers := make([]StartHandler, len(bp.startHandlers))
	copy(handlers, bp.startHandlers)
	bp.mu.RUnlock()

	for _, handler := range handlers {
		if handler != nil {
			handler(event)
		}
	}
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
