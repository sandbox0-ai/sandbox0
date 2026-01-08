// Package context provides context management for Procd.
package context

import (
	"fmt"
	"sync"
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
func NewContext(config process.ProcessConfig) (*Context, error) {
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

// Manager manages contexts in the sandbox.
type Manager struct {
	mu       sync.RWMutex
	contexts map[string]*Context
	maxCtxs  int
}

// NewManager creates a new context manager.
func NewManager(maxContexts int) *Manager {
	return &Manager{
		contexts: make(map[string]*Context),
		maxCtxs:  maxContexts,
	}
}

// CreateContext creates a new context.
func (m *Manager) CreateContext(config process.ProcessConfig) (*Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.contexts) >= m.maxCtxs {
		return nil, ErrMaxContextsReached
	}

	ctx, err := NewContext(config)
	if err != nil {
		return nil, err
	}

	m.contexts[ctx.ID] = ctx
	return ctx, nil
}

// GetContext returns a context by ID.
func (m *Manager) GetContext(id string) (*Context, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ctx, exists := m.contexts[id]
	if !exists {
		return nil, ErrContextNotFound
	}

	return ctx, nil
}

// ListContexts returns all contexts.
func (m *Manager) ListContexts() []*Context {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Context, 0, len(m.contexts))
	for _, ctx := range m.contexts {
		result = append(result, ctx)
	}

	return result
}

// DeleteContext deletes a context.
func (m *Manager) DeleteContext(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, exists := m.contexts[id]
	if !exists {
		return ErrContextNotFound
	}

	if err := ctx.Stop(); err != nil {
		// Log but continue with deletion
	}

	delete(m.contexts, id)
	return nil
}

// RestartContext restarts a context.
func (m *Manager) RestartContext(id string) (*Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, exists := m.contexts[id]
	if !exists {
		return nil, ErrContextNotFound
	}

	if err := ctx.Restart(); err != nil {
		return nil, err
	}

	return ctx, nil
}

// WriteInput writes input to a context's main process.
func (m *Manager) WriteInput(contextID string, data []byte) error {
	ctx, err := m.GetContext(contextID)
	if err != nil {
		return err
	}

	if ctx.MainProcess == nil {
		return process.ErrProcessNotRunning
	}

	return ctx.MainProcess.WriteInput(data)
}

// ReadOutput returns the output channel for a context.
func (m *Manager) ReadOutput(contextID string) (<-chan process.ProcessOutput, error) {
	ctx, err := m.GetContext(contextID)
	if err != nil {
		return nil, err
	}

	if ctx.MainProcess == nil {
		return nil, process.ErrProcessNotRunning
	}

	return ctx.MainProcess.ReadOutput(), nil
}

// Cleanup cleans up all contexts.
func (m *Manager) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ctx := range m.contexts {
		ctx.Stop()
	}

	m.contexts = make(map[string]*Context)
}
