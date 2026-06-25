package process

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// DirectRunner manages non-PTY process execution with stdout/stderr pipes.
type DirectRunner struct {
	base         *BaseProcess
	cmd          *exec.Cmd
	ctx          context.Context
	stdout       *limitedBuffer
	stderr       *limitedBuffer
	onStop       func()
	exitResolver func(error) (int, bool)
	stdoutBytes  int64
	stderrBytes  int64
	stdoutChunks int64
	stderrChunks int64
}

var _ OutputProvider = (*DirectRunner)(nil)

// NewDirectRunner creates a direct runner for a process.
func NewDirectRunner(base *BaseProcess, ctx context.Context, onStop func()) *DirectRunner {
	return &DirectRunner{
		base:   base,
		ctx:    ctx,
		onStop: onStop,
		stdout: newLimitedBuffer(maxDirectRunnerOutputBytes),
		stderr: newLimitedBuffer(maxDirectRunnerOutputBytes),
	}
}

// SetExitResolver sets a custom exit code resolver for this runner.
func (r *DirectRunner) SetExitResolver(resolver func(error) (int, bool)) {
	r.exitResolver = resolver
}

// Start launches the command with stdout/stderr pipes.
func (r *DirectRunner) Start(cmd *exec.Cmd) error {
	if r.base.IsRunning() {
		return ErrProcessAlreadyRunning
	}

	r.base.SetState(ProcessStateStarting)

	// Create a new process group for signal management
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	cmd.Stdout = &directRunnerOutputWriter{
		runner: r,
		source: OutputSourceStdout,
		buffer: r.stdout,
	}
	cmd.Stderr = &directRunnerOutputWriter{
		runner: r,
		source: OutputSourceStderr,
		buffer: r.stderr,
	}

	if err := cmd.Start(); err != nil {
		r.base.SetState(ProcessStateCrashed)
		return fmt.Errorf("%w: %v", ErrProcessStartFailed, err)
	}

	r.cmd = cmd
	r.base.SetPID(cmd.Process.Pid)
	r.base.SetStartTime(time.Now())
	r.base.SetState(ProcessStateRunning)
	r.base.NotifyStart(StartEvent{
		ProcessID:   r.base.ID(),
		ProcessType: r.base.Type(),
		PID:         r.base.PID(),
		StartTime:   r.base.StartTime(),
		State:       r.base.State(),
		Config:      r.base.GetConfig(),
	})

	go r.monitorProcess()

	return nil
}

// Stop terminates the direct process.
func (r *DirectRunner) Stop() error {
	if !r.base.IsRunning() {
		return nil
	}

	if r.onStop != nil {
		r.onStop()
	}

	if r.cmd != nil && r.cmd.Process != nil {
		if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			_ = r.cmd.Process.Kill()
		}
	}

	r.base.SetState(ProcessStateStopped)
	r.base.CloseOutput()

	return nil
}

// GetOutput returns the captured stdout and stderr.
func (r *DirectRunner) GetOutput() (stdout, stderr string) {
	return r.stdout.String(), r.stderr.String()
}

func (r *DirectRunner) monitorProcess() {
	if r.cmd == nil {
		return
	}

	err := r.cmd.Wait()

	exitCode := 0
	if err != nil {
		if r.exitResolver != nil {
			if code, ok := r.exitResolver(err); ok {
				exitCode = code
			}
		}
		if exitCode == 0 {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else if r.ctx.Err() == context.Canceled {
				exitCode = 137
			}
		}
	}

	r.base.SetExitCode(exitCode)
	duration := time.Since(r.base.StartTime())

	stdoutPreview := truncatePreview(r.stdout.Bytes(), 2048)
	stderrPreview := truncatePreview(r.stderr.Bytes(), 2048)

	switch exitCode {
	case 0:
		r.base.SetState(ProcessStateStopped)
	case -1, 137:
		r.base.SetState(ProcessStateKilled)
	default:
		r.base.SetState(ProcessStateCrashed)
	}

	r.base.NotifyExit(ExitEvent{
		ProcessID:     r.base.ID(),
		ProcessType:   r.base.Type(),
		PID:           r.base.PID(),
		ExitCode:      exitCode,
		Duration:      duration,
		State:         r.base.State(),
		StdoutPreview: stdoutPreview,
		StderrPreview: stderrPreview,
		Config:        r.base.GetConfig(),
	})

	r.base.CloseOutput()
}

type directRunnerOutputWriter struct {
	runner *DirectRunner
	source OutputSource
	buffer *limitedBuffer
}

func (w *directRunnerOutputWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	payload := make([]byte, len(p))
	copy(payload, p)
	w.buffer.Write(payload)
	w.runner.base.PublishOutput(ProcessOutput{
		Source: w.source,
		Data:   payload,
	})

	switch w.source {
	case OutputSourceStdout:
		atomic.AddInt64(&w.runner.stdoutBytes, int64(len(p)))
		atomic.AddInt64(&w.runner.stdoutChunks, 1)
	case OutputSourceStderr:
		atomic.AddInt64(&w.runner.stderrBytes, int64(len(p)))
		atomic.AddInt64(&w.runner.stderrChunks, 1)
	}

	return len(p), nil
}

const maxDirectRunnerOutputBytes = 1 << 20

type limitedBuffer struct {
	mu   sync.Mutex
	max  int
	data []byte
}

func newLimitedBuffer(max int) *limitedBuffer {
	if max <= 0 {
		max = 1 << 20
	}
	return &limitedBuffer{max: max}
}

func (b *limitedBuffer) Write(p []byte) {
	if len(p) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) >= b.max {
		b.data = append(b.data[:0], p[len(p)-b.max:]...)
		return
	}
	if len(b.data)+len(p) > b.max {
		excess := len(b.data) + len(p) - b.max
		if excess >= len(b.data) {
			b.data = b.data[:0]
		} else {
			b.data = b.data[excess:]
		}
	}
	b.data = append(b.data, p...)
}

func (b *limitedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) == 0 {
		return nil
	}
	out := make([]byte, len(b.data))
	copy(out, b.data)
	return out
}

func (b *limitedBuffer) String() string {
	return string(b.Bytes())
}

func (b *limitedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.data)
}

func truncatePreview(data []byte, limit int) string {
	if limit <= 0 || len(data) == 0 {
		return ""
	}
	if len(data) <= limit {
		return string(data)
	}
	return string(data[:limit])
}
