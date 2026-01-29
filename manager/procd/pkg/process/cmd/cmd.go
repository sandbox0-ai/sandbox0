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
	runner  *process.PTYRunner
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
	if config.PTYSize != nil {
		env = append(env, fmt.Sprintf("TERM=%s", resolveTerm(config)))
	}
	cmd.Env = env

	// Check if PTY is requested
	if config.PTYSize != nil {
		// PTY mode: Do NOT set Setpgid.
		// PTY automatically creates a new session and handles terminal control.
		if c.runner == nil {
			c.runner = process.NewPTYRunner(c.BaseProcess, nil, c.cancel)
		}
		c.runner.SetExitResolver(func(err error) (int, bool) {
			if c.ctx.Err() == context.Canceled {
				return 137, true
			}
			return 0, false
		})
		return c.runner.Start(cmd, config.PTYSize)
	} else {
		// Non-PTY mode: Create a new process group for signal management
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}

		// Start with pipes for non-interactive commands
		cmd.Stdout = &c.stdout
		cmd.Stderr = &c.stderr

		if err := cmd.Start(); err != nil {
			c.SetState(process.ProcessStateCrashed)
			return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
		}

		c.cmd = cmd
		c.SetPID(cmd.Process.Pid)
		c.SetStartTime(time.Now())
		c.SetState(process.ProcessStateRunning)
		c.NotifyStart(process.StartEvent{
			ProcessID:   c.ID(),
			ProcessType: c.Type(),
			PID:         c.PID(),
			StartTime:   c.StartTime(),
			State:       c.State(),
			Config:      config,
		})

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

	if c.GetPTY() != nil && c.runner != nil {
		return c.runner.Stop()
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

	duration := time.Since(c.StartTime())
	stdoutPreview := ""
	stderrPreview := ""

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
		stdoutPreview = truncatePreview(c.stdout.Bytes(), 2048)
		stderrPreview = truncatePreview(c.stderr.Bytes(), 2048)
	}

	if exitCode == 0 {
		c.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		c.SetState(process.ProcessStateKilled)
	} else {
		c.SetState(process.ProcessStateCrashed)
	}

	c.NotifyExit(process.ExitEvent{
		ProcessID:     c.ID(),
		ProcessType:   c.Type(),
		PID:           c.PID(),
		ExitCode:      exitCode,
		Duration:      duration,
		State:         c.State(),
		StdoutPreview: stdoutPreview,
		StderrPreview: stderrPreview,
		Config:        c.GetConfig(),
	})

	c.CloseOutput()
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

func resolveTerm(config process.ProcessConfig) string {
	if config.Term != "" {
		return config.Term
	}
	if val, ok := config.EnvVars["TERM"]; ok && strings.TrimSpace(val) != "" {
		return val
	}
	return "xterm-256color"
}
