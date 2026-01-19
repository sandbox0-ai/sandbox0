// Package cmd provides one-time command execution.
package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// CMD implements a one-time command execution process.
// Unlike REPL processes, CMD processes execute a single command and terminate.
type CMD struct {
	*process.BaseProcess
	cmd     *exec.Cmd
	ctx     context.Context
	cancel  context.CancelFunc
	stdout  bytes.Buffer
	stderr  bytes.Buffer
	command []string
}

// NewCMD creates a new CMD process.
// The command parameter should be the full command path and arguments, e.g., []string{"/bin/ls", "-la"}
func NewCMD(id string, config process.ProcessConfig, command []string) (*CMD, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("%w: command cannot be empty", process.ErrInvalidCommand)
	}

	bp := process.NewBaseProcess(id, process.ProcessTypeCMD, config)

	ctx, cancel := context.WithCancel(context.Background())

	return &CMD{
		BaseProcess: bp,
		ctx:         ctx,
		cancel:      cancel,
		command:     command,
	}, nil
}

// Start starts the command execution.
func (c *CMD) Start() error {
	if c.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	c.SetState(process.ProcessStateStarting)

	config := c.GetConfig()

	// Create command with context for cancellation support
	cmdPath := c.command[0]
	args := []string{}
	if len(c.command) > 1 {
		args = c.command[1:]
	}

	cmd := exec.CommandContext(c.ctx, cmdPath, args...)

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

	// Check if PTY is requested
	if config.PTYSize != nil {
		// Start with PTY for interactive commands
		ptySize := config.PTYSize
		ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
			Rows: ptySize.Rows,
			Cols: ptySize.Cols,
		})
		if err != nil {
			c.SetState(process.ProcessStateCrashed)
			return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
		}

		c.SetPTY(ptmx)
		c.cmd = cmd
		c.SetPID(cmd.Process.Pid)
		c.SetState(process.ProcessStateRunning)

		// Start output reader
		go c.readPTYOutput(ptmx)

		// Start process monitor
		go c.monitorProcess()
	} else {
		// Start with pipes for non-interactive commands
		cmd.Stdout = &c.stdout
		cmd.Stderr = &c.stderr

		if err := cmd.Start(); err != nil {
			c.SetState(process.ProcessStateCrashed)
			return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
		}

		c.cmd = cmd
		c.SetPID(cmd.Process.Pid)
		c.SetState(process.ProcessStateRunning)

		// Start process monitor
		go c.monitorProcess()
	}

	return nil
}

// Stop stops the command execution.
func (c *CMD) Stop() error {
	if !c.IsRunning() {
		return nil
	}

	// Cancel the context to signal the command to stop
	c.cancel()

	if c.cmd != nil && c.cmd.Process != nil {
		// Send SIGTERM first
		if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			// If SIGTERM fails, use SIGKILL
			c.cmd.Process.Kill()
		}
	}

	// Close PTY if it exists
	if ptyFile := c.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	c.SetState(process.ProcessStateStopped)
	c.CloseOutput()

	return nil
}

// Restart restarts the command.
func (c *CMD) Restart() error {
	if err := c.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)

	// Create new context
	c.ctx, c.cancel = context.WithCancel(context.Background())

	return c.Start()
}

// GetOutput returns the captured stdout and stderr.
// This is only available for non-PTY commands.
func (c *CMD) GetOutput() (stdout, stderr string) {
	return c.stdout.String(), c.stderr.String()
}

// GetCommand returns the command being executed.
func (c *CMD) GetCommand() string {
	return strings.Join(c.command, " ")
}

func (c *CMD) readPTYOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			c.PublishOutput(process.ProcessOutput{
				Source: process.OutputSourcePTY,
				Data:   data,
			})
		}
		if err != nil {
			break
		}
	}
}

func (c *CMD) monitorProcess() {
	if c.cmd == nil {
		return
	}

	err := c.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if c.ctx.Err() == context.Canceled {
			// Context was canceled, treat as killed
			exitCode = 137
		}
	}

	c.SetExitCode(exitCode)

	// Publish final output for non-PTY commands
	if c.GetPTY() == nil {
		if c.stdout.Len() > 0 {
			c.PublishOutput(process.ProcessOutput{
				Source: process.OutputSourceStdout,
				Data:   c.stdout.Bytes(),
			})
		}
		if c.stderr.Len() > 0 {
			c.PublishOutput(process.ProcessOutput{
				Source: process.OutputSourceStderr,
				Data:   c.stderr.Bytes(),
			})
		}
	}

	if exitCode == 0 {
		c.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		c.SetState(process.ProcessStateKilled)
	} else {
		c.SetState(process.ProcessStateCrashed)
	}

	c.CloseOutput()
}
