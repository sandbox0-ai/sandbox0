package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// NodeREPL implements a Node.js REPL.
type NodeREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
}

// NewNodeREPL creates a new Node.js REPL process.
func NewNodeREPL(id string, config process.ProcessConfig) (*NodeREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &NodeREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
	}, nil
}

// Start starts the Node.js REPL process.
func (n *NodeREPL) Start() error {
	if n.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := n.GetConfig()

	// Use node REPL
	cmd := exec.Command("node", "--interactive")

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

	return n.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Node.js REPL process.
func (n *NodeREPL) Stop() error {
	return n.runner.Stop()
}

// Restart restarts the process.
func (n *NodeREPL) Restart() error {
	if err := n.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return n.Start()
}

// ExecuteCode executes JavaScript code in the REPL.
func (n *NodeREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !n.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := n.GetPTY()
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
func (n *NodeREPL) ResizeTerminal(size process.PTYSize) error {
	if !n.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return n.BaseProcess.ResizePTY(size)
}
