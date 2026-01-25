package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// ZshREPL implements a Zsh shell REPL.
type ZshREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
	prompt string
}

// NewZshREPL creates a new Zsh REPL process.
func NewZshREPL(id string, config process.ProcessConfig) (*ZshREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &ZshREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
		prompt:      "SANDBOX0>>> ",
	}, nil
}

// Start starts the Zsh REPL process.
func (z *ZshREPL) Start() error {
	if z.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := z.GetConfig()

	// Check if zsh is available
	zshPath, err := exec.LookPath("zsh")
	if err != nil {
		// Fall back to bash
		zshPath = "bash"
	}

	cmd := exec.Command(zshPath, "--no-rcs", "-i")

	if config.CWD != "" {
		cmd.Dir = config.CWD
	}

	env := os.Environ()
	for k, v := range config.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	term := config.Term
	if term == "" {
		term = "xterm-256color"
	}
	env = append(env, fmt.Sprintf("TERM=%s", term))
	env = append(env, fmt.Sprintf("PS1=%s", z.prompt))

	cmd.Env = env

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	return z.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Zsh REPL process.
func (z *ZshREPL) Stop() error {
	return z.runner.Stop()
}

// Restart restarts the process.
func (z *ZshREPL) Restart() error {
	if err := z.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return z.Start()
}

// ExecuteCode executes a command in the Zsh REPL.
func (z *ZshREPL) ExecuteCode(cmd string) (*process.ExecutionResult, error) {
	if !z.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := z.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

	_, err := fmt.Fprintln(ptyFile, cmd)
	if err != nil {
		return nil, err
	}

	return &process.ExecutionResult{
		Output: []byte{},
	}, nil
}

// ResizeTerminal resizes the PTY.
func (z *ZshREPL) ResizeTerminal(size process.PTYSize) error {
	if !z.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return z.BaseProcess.ResizePTY(size)
}
