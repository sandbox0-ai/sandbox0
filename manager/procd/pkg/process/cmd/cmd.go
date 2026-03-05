// Package cmd provides one-time command execution.
package cmd

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
)

// CMD implements a one-time command execution process.
// Unlike REPL processes, CMD processes execute a single command and terminate.
type CMD struct {
	*process.BaseProcess
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	command      []string
	ptyRunner    *process.PTYRunner
	directRunner *process.DirectRunner
}

var _ process.Process = (*CMD)(nil)
var _ process.OutputProvider = (*CMD)(nil)

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

	ctx := c.context()
	cmd := c.prepareExecCmd(ctx)
	config := c.GetConfig()

	// Check if PTY is requested
	if config.PTYSize != nil {
		// PTY mode
		ptyRunner := c.ensurePTYRunner()
		ptyRunner.SetExitResolver(func(err error) (int, bool) {
			if ctx.Err() == context.Canceled {
				return 137, true
			}
			return 0, false
		})
		return ptyRunner.Start(cmd, config.PTYSize)
	}

	// Direct mode
	directRunner := c.ensureDirectRunner(ctx)
	directRunner.SetExitResolver(func(err error) (int, bool) {
		if ctx.Err() == context.Canceled {
			return 137, true
		}
		return 0, false
	})
	return directRunner.Start(cmd)
}

// prepareExecCmd creates and configures the exec.Cmd instance.
func (c *CMD) prepareExecCmd(ctx context.Context) *exec.Cmd {
	config := c.GetConfig()

	cmdPath := c.command[0]
	args := []string{}
	if len(c.command) > 1 {
		args = c.command[1:]
	}

	cmd := exec.CommandContext(ctx, cmdPath, args...)

	// Set working directory
	if config.CWD != "" {
		cmd.Dir = config.CWD
	}

	// Set environment variables
	env := []string{}
	for k, v := range config.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	if config.PTYSize != nil {
		env = append(env, fmt.Sprintf("TERM=%s", resolveTerm(config)))
	}
	cmd.Env = env

	return cmd
}

// Stop stops the command execution.
func (c *CMD) Stop() error {
	if !c.IsRunning() {
		return nil
	}

	ptyRunner, directRunner := c.runners()
	if ptyRunner != nil {
		return ptyRunner.Stop()
	}

	if directRunner != nil {
		return directRunner.Stop()
	}

	return nil
}

// Restart restarts the command.
func (c *CMD) Restart() error {
	if err := c.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)

	// Create new context
	ctx, cancel := context.WithCancel(context.Background())
	c.setContext(ctx, cancel)
	c.resetRunners()

	return c.Start()
}

// GetOutput returns the captured stdout and stderr.
// This is only available for non-PTY commands.
func (c *CMD) GetOutput() (stdout, stderr string) {
	_, directRunner := c.runners()
	if directRunner != nil {
		return directRunner.GetOutput()
	}
	return "", ""
}

// GetCommand returns the command being executed.
func (c *CMD) GetCommand() string {
	return strings.Join(c.command, " ")
}

func (c *CMD) context() context.Context {
	c.mu.RLock()
	ctx := c.ctx
	c.mu.RUnlock()
	return ctx
}

func (c *CMD) cancelFunc() context.CancelFunc {
	c.mu.RLock()
	cancel := c.cancel
	c.mu.RUnlock()
	return cancel
}

func (c *CMD) setContext(ctx context.Context, cancel context.CancelFunc) {
	c.mu.Lock()
	c.ctx = ctx
	c.cancel = cancel
	c.mu.Unlock()
}

func (c *CMD) ensurePTYRunner() *process.PTYRunner {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ptyRunner == nil {
		c.ptyRunner = process.NewPTYRunner(c.BaseProcess, nil, c.cancel)
	}
	return c.ptyRunner
}

func (c *CMD) ensureDirectRunner(ctx context.Context) *process.DirectRunner {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.directRunner == nil {
		c.directRunner = process.NewDirectRunner(c.BaseProcess, ctx, c.cancel)
	}
	return c.directRunner
}

func (c *CMD) runners() (*process.PTYRunner, *process.DirectRunner) {
	c.mu.RLock()
	ptyRunner := c.ptyRunner
	directRunner := c.directRunner
	c.mu.RUnlock()
	return ptyRunner, directRunner
}

func (c *CMD) resetRunners() {
	c.mu.Lock()
	c.ptyRunner = nil
	c.directRunner = nil
	c.mu.Unlock()
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
