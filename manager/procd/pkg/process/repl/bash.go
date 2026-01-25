package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// BashREPL implements a Bash shell REPL.
type BashREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
	prompt string
}

// NewBashREPL creates a new Bash REPL process.
func NewBashREPL(id string, config process.ProcessConfig) (*BashREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &BashREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
		prompt:      "SANDBOX0>>> ",
	}, nil
}

// Start starts the Bash REPL process.
func (b *BashREPL) Start() error {
	if b.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := b.GetConfig()

	// Start interactive bash
	cmd := exec.Command("bash", "--norc", "--noprofile", "-i")

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
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

	// Set TERM
	term := config.Term
	if term == "" {
		term = "xterm-256color"
	}
	env = append(env, fmt.Sprintf("TERM=%s", term))

	// Set custom prompt
	env = append(env, fmt.Sprintf("PS1=%s", b.prompt))

	cmd.Env = env

	return b.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Bash REPL process.
func (b *BashREPL) Stop() error {
	return b.runner.Stop()
}

// Restart restarts the process.
func (b *BashREPL) Restart() error {
	if err := b.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return b.Start()
}

// ExecuteCode executes a command in the Bash REPL.
func (b *BashREPL) ExecuteCode(cmd string) (*process.ExecutionResult, error) {
	if !b.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := b.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

	// Write command to PTY
	_, err := fmt.Fprintln(ptyFile, cmd)
	if err != nil {
		return nil, err
	}

	return &process.ExecutionResult{
		Output: []byte{},
	}, nil
}

// ResizeTerminal resizes the PTY.
func (b *BashREPL) ResizeTerminal(size process.PTYSize) error {
	if !b.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return b.BaseProcess.ResizePTY(size)
}
