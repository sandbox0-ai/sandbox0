package process

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

// PTYRunner manages PTY lifecycle for exec-based processes.
type PTYRunner struct {
	base         *BaseProcess
	cmd          *exec.Cmd
	outputFilter func([]byte) ([]byte, bool)
	onStop       func()
	exitResolver func(error) (int, bool)

	mu              sync.RWMutex
	readerDone      chan struct{}
	closeOutputOnce sync.Once
}

// NewPTYRunner creates a PTY runner for a process.
func NewPTYRunner(base *BaseProcess, outputFilter func([]byte) ([]byte, bool), onStop func()) *PTYRunner {
	return &PTYRunner{
		base:         base,
		outputFilter: outputFilter,
		onStop:       onStop,
	}
}

// SetExitResolver sets a custom exit code resolver for this runner.
func (r *PTYRunner) SetExitResolver(resolver func(error) (int, bool)) {
	r.exitResolver = resolver
}

// Start launches the command with a PTY attached.
func (r *PTYRunner) Start(cmd *exec.Cmd, size *PTYSize) error {
	if r.base.IsRunning() {
		return ErrProcessAlreadyRunning
	}

	r.base.SetState(ProcessStateStarting)

	ptySize := size
	if ptySize == nil {
		ptySize = &PTYSize{Rows: 100, Cols: 500}
	}

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: ptySize.Rows,
		Cols: ptySize.Cols,
	})
	if err != nil {
		r.base.SetState(ProcessStateCrashed)
		return fmt.Errorf("%w: %v", ErrProcessStartFailed, err)
	}

	r.cmd = cmd
	r.mu.Lock()
	r.readerDone = make(chan struct{})
	r.closeOutputOnce = sync.Once{}
	r.mu.Unlock()
	r.base.SetPTY(ptmx)
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

	go r.readOutput(ptmx)
	go r.monitorProcess()

	return nil
}

// Stop terminates the PTY-backed process.
func (r *PTYRunner) Stop() error {
	if !r.base.IsRunning() {
		return nil
	}

	if r.onStop != nil {
		r.onStop()
	}

	r.base.stopInputWriter()

	if r.cmd != nil && r.cmd.Process != nil {
		if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			_ = r.cmd.Process.Kill()
		}
	}

	if ptyFile := r.base.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	r.waitReaderDone()
	r.base.SetState(ProcessStateStopped)
	r.closeOutput()

	return nil
}

func (r *PTYRunner) readOutput(ptmx *os.File) {
	defer r.markReaderDone()
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			promptDetected := false
			if r.outputFilter != nil {
				data, promptDetected = r.outputFilter(data)
			}
			if len(data) > 0 {
				r.base.PublishOutput(ProcessOutput{
					Source: OutputSourcePTY,
					Data:   data,
				})
			}
			if promptDetected {
				r.base.signalInputReady()
				r.base.PublishOutput(ProcessOutput{
					Source: OutputSourcePrompt,
				})
			}
		}
		if err != nil {
			break
		}
	}
}

func (r *PTYRunner) monitorProcess() {
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
			} else if r.base.State() == ProcessStateStarting {
				exitCode = 1
			}
		}
	}

	r.base.SetExitCode(exitCode)

	duration := time.Since(r.base.StartTime())

	switch exitCode {
	case 0:
		r.base.SetState(ProcessStateStopped)
	case -1, 137:
		r.base.SetState(ProcessStateKilled)
	default:
		r.base.SetState(ProcessStateCrashed)
	}

	r.base.NotifyExit(ExitEvent{
		ProcessID:   r.base.ID(),
		ProcessType: r.base.Type(),
		PID:         r.base.PID(),
		ExitCode:    exitCode,
		Duration:    duration,
		State:       r.base.State(),
		Config:      r.base.GetConfig(),
	})

	r.base.stopInputWriter()
	r.waitReaderDone()
	r.closeOutput()
}

func (r *PTYRunner) markReaderDone() {
	r.mu.RLock()
	done := r.readerDone
	r.mu.RUnlock()
	if done != nil {
		close(done)
	}
}

func (r *PTYRunner) waitReaderDone() {
	r.mu.RLock()
	done := r.readerDone
	r.mu.RUnlock()
	if done != nil {
		<-done
	}
}

func (r *PTYRunner) closeOutput() {
	r.closeOutputOnce.Do(func() {
		r.base.CloseOutput()
	})
}
