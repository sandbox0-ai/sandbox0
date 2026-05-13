// Package context provides context management for Procd.
package context

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process/repl"
)

// Context represents a logical container for a process with its environment.
type Context struct {
	ID             string              `json:"id"`
	Type           process.ProcessType `json:"type"`
	Alias          string              `json:"alias"`
	Command        []string            `json:"command,omitempty"`
	CWD            string              `json:"cwd"`
	EnvVars        map[string]string   `json:"env_vars"`
	MainProcess    process.Process     `json:"-"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
	LastActivityAt time.Time           `json:"-"`
	FinishedAt     *time.Time          `json:"-"`
	CleanupPolicy  CleanupPolicy       `json:"-"`

	mu sync.RWMutex
}

// CleanupPolicy defines when a context should be cleaned up.
type CleanupPolicy struct {
	IdleTimeout time.Duration
	MaxLifetime time.Duration
	FinishedTTL time.Duration
}

func (p CleanupPolicy) isZero() bool {
	return p.IdleTimeout == 0 && p.MaxLifetime == 0 && p.FinishedTTL == 0
}

// NewContext creates a new context with the given configuration.
func NewContext(config process.ProcessConfig, replConfig *repl.REPLConfig, exitHandler process.ExitHandler, startHandler process.StartHandler) (*Context, error) {
	id := "ctx-" + uuid.New().String()[:8]

	var proc process.Process
	var err error

	if config.Type == process.ProcessTypeREPL && replConfig != nil && config.Alias == "" {
		config.Alias = replConfig.Name
	}

	switch config.Type {
	case process.ProcessTypeREPL:
		proc, err = createREPLProcess(id, config, replConfig)
	case process.ProcessTypeCMD:
		if len(config.Command) == 0 {
			return nil, fmt.Errorf("CMD process type requires command to be specified in config.Command")
		}
		proc, err = createCMDProcess(id, config, config.Command)
	default:
		return nil, fmt.Errorf("%w: %s", process.ErrUnsupportedProcessType, config.Type)
	}

	if err != nil {
		return nil, err
	}

	now := time.Now()
	ctx := &Context{
		ID:             id,
		Type:           config.Type,
		Alias:          config.Alias,
		Command:        append([]string(nil), config.Command...),
		CWD:            config.CWD,
		EnvVars:        config.EnvVars,
		MainProcess:    proc,
		CreatedAt:      now,
		UpdatedAt:      now,
		LastActivityAt: now,
	}

	proc.AddExitHandler(func(event process.ExitEvent) {
		ctx.markFinished()
	})
	if exitHandler != nil {
		proc.AddExitHandler(exitHandler)
	}
	if startHandler != nil {
		proc.AddStartHandler(startHandler)
	}

	if err := proc.Start(); err != nil {
		return nil, err
	}

	return ctx, nil
}

// Stop stops the context and its main process.
func (ctx *Context) Stop() error {
	if ctx.MainProcess != nil {
		return ctx.MainProcess.Stop()
	}
	return nil
}

// Restart restarts the context's main process.
func (ctx *Context) Restart() error {
	if ctx.MainProcess != nil {
		ctx.touch()
		return ctx.MainProcess.Restart()
	}
	return nil
}

// IsRunning returns true if the main process is running.
func (ctx *Context) IsRunning() bool {
	if ctx.MainProcess != nil {
		return ctx.MainProcess.IsRunning()
	}
	return false
}

// IsPaused returns true if the main process is paused.
func (ctx *Context) IsPaused() bool {
	if ctx.MainProcess != nil {
		return ctx.MainProcess.IsPaused()
	}
	return false
}

// Pause pauses the context's main process and all its children.
func (ctx *Context) Pause() error {
	if ctx.MainProcess != nil {
		ctx.touch()
		return ctx.MainProcess.Pause()
	}
	return nil
}

// Resume resumes the context's main process and all its children.
func (ctx *Context) Resume() error {
	if ctx.MainProcess != nil {
		ctx.touch()
		return ctx.MainProcess.Resume()
	}
	return nil
}

// ResourceUsage returns the resource usage of this context's process tree.
func (ctx *Context) ResourceUsage() process.ResourceUsage {
	if ctx.MainProcess != nil {
		return ctx.MainProcess.ResourceUsage()
	}
	return process.ResourceUsage{}
}

// ResizePTY resizes the context's PTY, if available.
func (ctx *Context) ResizePTY(size process.PTYSize) error {
	if ctx.MainProcess != nil {
		ctx.touch()
		return ctx.MainProcess.ResizePTY(size)
	}
	return process.ErrProcessNotRunning
}

// SendSignal sends a signal to the context's process.
func (ctx *Context) SendSignal(sig syscall.Signal) error {
	if ctx.MainProcess != nil {
		ctx.touch()
		return ctx.MainProcess.SendSignal(sig)
	}
	return process.ErrProcessNotRunning
}

// AddExitHandler appends an exit handler to the handler chain.
// Handlers are executed in the order they were added, enabling middleware-like behavior.
func (ctx *Context) AddExitHandler(handler process.ExitHandler) {
	if ctx.MainProcess != nil {
		ctx.MainProcess.AddExitHandler(handler)
		ctx.touch()
	}
}

// AddStartHandler appends a start handler to the handler chain.
// Handlers are executed in the order they were added, enabling middleware-like behavior.
func (ctx *Context) AddStartHandler(handler process.StartHandler) {
	if ctx.MainProcess != nil {
		ctx.MainProcess.AddStartHandler(handler)
		ctx.touch()
	}
}

// Touch marks the context as recently active.
func (ctx *Context) Touch() {
	ctx.touch()
}

// SetCleanupPolicy sets the cleanup policy for the context.
func (ctx *Context) SetCleanupPolicy(policy CleanupPolicy) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.CleanupPolicy = policy
}

func (ctx *Context) markFinished() {
	now := time.Now()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.FinishedAt = &now
	ctx.UpdatedAt = now
}

func (ctx *Context) touch() {
	now := time.Now()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.LastActivityAt = now
	ctx.UpdatedAt = now
}

func (ctx *Context) shouldCleanup(now time.Time) bool {
	ctx.mu.RLock()
	policy := ctx.CleanupPolicy
	lastActivity := ctx.LastActivityAt
	finishedAt := ctx.FinishedAt
	ctx.mu.RUnlock()

	if policy.MaxLifetime > 0 && now.Sub(ctx.CreatedAt) > policy.MaxLifetime {
		return true
	}
	if policy.FinishedTTL > 0 && finishedAt != nil && now.Sub(*finishedAt) > policy.FinishedTTL {
		return true
	}
	if policy.IdleTimeout > 0 && now.Sub(lastActivity) > policy.IdleTimeout {
		return true
	}
	return false
}
