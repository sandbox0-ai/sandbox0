package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// RubyREPL implements a Ruby REPL using IRB.
type RubyREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
}

// NewRubyREPL creates a new Ruby REPL process.
func NewRubyREPL(id string, config process.ProcessConfig) (*RubyREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &RubyREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
	}, nil
}

// Start starts the Ruby REPL process.
func (r *RubyREPL) Start() error {
	if r.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

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

	return r.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Ruby REPL process.
func (r *RubyREPL) Stop() error {
	return r.runner.Stop()
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
	if !r.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return r.BaseProcess.ResizePTY(size)
}
