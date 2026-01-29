package process

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

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

	exitHandler  ExitHandler
	startHandler StartHandler

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

// SetStartHandler sets the start handler.
func (bp *BaseProcess) SetStartHandler(handler StartHandler) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.startHandler = handler
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

// NotifyExit invokes the exit handler, if configured.
func (bp *BaseProcess) NotifyExit(event ExitEvent) {
	bp.mu.RLock()
	handler := bp.exitHandler
	bp.mu.RUnlock()
	if handler != nil {
		handler(event)
	}
}

// NotifyStart invokes the start handler, if configured.
func (bp *BaseProcess) NotifyStart(event StartEvent) {
	bp.mu.RLock()
	handler := bp.startHandler
	bp.mu.RUnlock()
	if handler != nil {
		handler(event)
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
