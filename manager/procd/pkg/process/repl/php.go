package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// PHPREPL implements a PHP REPL.
type PHPREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
}

// NewPHPREPL creates a new PHP REPL process.
func NewPHPREPL(id string, config process.ProcessConfig) (*PHPREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &PHPREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
	}, nil
}

// Start starts the PHP REPL process.
func (p *PHPREPL) Start() error {
	if p.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := p.GetConfig()

	// Check for PHP
	phpPath, err := exec.LookPath("php")
	if err != nil {
		p.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: php not found", process.ErrProcessStartFailed)
	}

	// Use PHP interactive mode
	cmd := exec.Command(phpPath, "-a")

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

	return p.runner.Start(cmd, config.PTYSize)
}

// Stop stops the PHP REPL process.
func (p *PHPREPL) Stop() error {
	return p.runner.Stop()
}

// Restart restarts the process.
func (p *PHPREPL) Restart() error {
	if err := p.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return p.Start()
}

// ExecuteCode executes PHP code in the REPL.
func (p *PHPREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !p.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := p.GetPTY()
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
func (p *PHPREPL) ResizeTerminal(size process.PTYSize) error {
	if !p.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return p.BaseProcess.ResizePTY(size)
}
