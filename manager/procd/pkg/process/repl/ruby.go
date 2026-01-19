package repl

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// RubyREPL implements a Ruby REPL using IRB.
type RubyREPL struct {
	*process.BaseProcess
	cmd *exec.Cmd
}

// NewRubyREPL creates a new Ruby REPL process.
func NewRubyREPL(id string, config process.ProcessConfig) (*RubyREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &RubyREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the Ruby REPL process.
func (r *RubyREPL) Start() error {
	if r.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	r.SetState(process.ProcessStateStarting)

	config := r.GetConfig()

	// Try Ruby interpreters in order of preference
	var cmd *exec.Cmd
	rubyCandidates := []struct {
		name string
		args []string
	}{
		{"irb", []string{"--simple-prompt", "--noreadline"}},
		{"ruby", []string{"-e", "require 'irb'; IRB.start"}},
	}

	for _, candidate := range rubyCandidates {
		if path, err := exec.LookPath(candidate.name); err == nil {
			cmd = exec.Command(path, candidate.args...)
			break
		}
	}

	if cmd == nil {
		r.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: no Ruby interpreter found (tried: irb, ruby)", process.ErrProcessStartFailed)
	}

	// Set working directory
	if config.CWD != "" {
		cmd.Dir = config.CWD
	}

	// Set environment variables
	env := os.Environ()
	for k, v := range config.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = env

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Get PTY size
	ptySize := config.PTYSize
	if ptySize == nil {
		ptySize = &process.PTYSize{Rows: 24, Cols: 80}
	}

	// Start with PTY
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: ptySize.Rows,
		Cols: ptySize.Cols,
	})
	if err != nil {
		r.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	r.cmd = cmd
	r.SetPTY(ptmx)
	r.SetPID(cmd.Process.Pid)
	r.SetState(process.ProcessStateRunning)

	// Start output reader
	go r.readOutput(ptmx)

	// Start process monitor
	go r.monitorProcess()

	return nil
}

// Stop stops the Ruby REPL process.
func (r *RubyREPL) Stop() error {
	if !r.IsRunning() {
		return nil
	}

	if r.cmd != nil && r.cmd.Process != nil {
		if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			r.cmd.Process.Kill()
		}
	}

	if ptyFile := r.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	r.SetState(process.ProcessStateStopped)
	r.CloseOutput()

	return nil
}

// Restart restarts the process.
func (r *RubyREPL) Restart() error {
	if err := r.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return r.Start()
}

// ExecuteCode executes Ruby code in the REPL.
func (r *RubyREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !r.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := r.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

	// Write code to PTY
	_, err := fmt.Fprintln(ptyFile, code)
	if err != nil {
		return nil, err
	}

	return &process.ExecutionResult{
		Output: []byte{},
	}, nil
}

// ResizeTerminal resizes the PTY.
func (r *RubyREPL) ResizeTerminal(size process.PTYSize) error {
	ptyFile := r.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (r *RubyREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		nr, err := ptmx.Read(buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])

			r.PublishOutput(process.ProcessOutput{
				Source: process.OutputSourcePTY,
				Data:   data,
			})
		}
		if err != nil {
			if err != io.EOF {
				// Log error if needed
			}
			break
		}
	}
}

func (r *RubyREPL) monitorProcess() {
	if r.cmd == nil {
		return
	}

	err := r.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	r.SetExitCode(exitCode)

	if exitCode == 0 {
		r.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		r.SetState(process.ProcessStateKilled)
	} else {
		r.SetState(process.ProcessStateCrashed)
	}

	r.CloseOutput()
}
