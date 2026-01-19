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

// NodeREPL implements a Node.js REPL.
type NodeREPL struct {
	*process.BaseProcess
	cmd *exec.Cmd
}

// NewNodeREPL creates a new Node.js REPL process.
func NewNodeREPL(id string, config process.ProcessConfig) (*NodeREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &NodeREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the Node.js REPL process.
func (n *NodeREPL) Start() error {
	if n.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	n.SetState(process.ProcessStateStarting)

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
		n.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	n.cmd = cmd
	n.SetPTY(ptmx)
	n.SetPID(cmd.Process.Pid)
	n.SetState(process.ProcessStateRunning)

	// Start output reader
	go n.readOutput(ptmx)

	// Start process monitor
	go n.monitorProcess()

	return nil
}

// Stop stops the Node.js REPL process.
func (n *NodeREPL) Stop() error {
	if !n.IsRunning() {
		return nil
	}

	if n.cmd != nil && n.cmd.Process != nil {
		if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			n.cmd.Process.Kill()
		}
	}

	if ptyFile := n.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	n.SetState(process.ProcessStateStopped)
	n.CloseOutput()

	return nil
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
	ptyFile := n.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (n *NodeREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		nr, err := ptmx.Read(buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])

			n.PublishOutput(process.ProcessOutput{
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

func (n *NodeREPL) monitorProcess() {
	if n.cmd == nil {
		return
	}

	err := n.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	n.SetExitCode(exitCode)

	if exitCode == 0 {
		n.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		n.SetState(process.ProcessStateKilled)
	} else {
		n.SetState(process.ProcessStateCrashed)
	}

	n.CloseOutput()
}
