// Package context provides context management for Procd.
package context

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// Context represents a logical container for a process with its environment.
type Context struct {
	ID          string              `json:"id"`
	Type        process.ProcessType `json:"type"`
	Language    string              `json:"language"`
	CWD         string              `json:"cwd"`
	EnvVars     map[string]string   `json:"env_vars"`
	MainProcess process.Process     `json:"-"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

// NewContext creates a new context with the given configuration.
func NewContext(config process.ProcessConfig, exitHandler process.ExitHandler) (*Context, error) {
	id := "ctx-" + uuid.New().String()[:8]

	var proc process.Process
	var err error

	switch config.Type {
	case process.ProcessTypeREPL:
		proc, err = createREPLProcess(id, config)
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

	if exitHandler != nil {
		proc.SetExitHandler(exitHandler)
	}

	if err := proc.Start(); err != nil {
		return nil, err
	}

	return &Context{
		ID:          id,
		Type:        config.Type,
		Language:    config.Language,
		CWD:         config.CWD,
		EnvVars:     config.EnvVars,
		MainProcess: proc,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}, nil
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
		ctx.UpdatedAt = time.Now()
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
		ctx.UpdatedAt = time.Now()
		return ctx.MainProcess.Pause()
	}
	return nil
}

// Resume resumes the context's main process and all its children.
func (ctx *Context) Resume() error {
	if ctx.MainProcess != nil {
		ctx.UpdatedAt = time.Now()
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
