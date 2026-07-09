package context

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process/repl"
)

// ContextResourceUsage represents resource usage for a single context.
type ContextResourceUsage struct {
	ContextID string                `json:"context_id"`
	Type      process.ProcessType   `json:"type"`
	Alias     string                `json:"alias"`
	Running   bool                  `json:"running"`
	Paused    bool                  `json:"paused"`
	Usage     process.ResourceUsage `json:"usage"`
}

// SandboxResourceUsage represents aggregated resource usage for the entire sandbox.
type SandboxResourceUsage struct {
	// Container-level stats (from cgroup)
	ContainerMemoryUsage      int64 `json:"container_memory_usage"`
	ContainerMemoryLimit      int64 `json:"container_memory_limit"`
	ContainerMemoryWorkingSet int64 `json:"container_memory_working_set"`

	// Aggregated process stats across all contexts
	TotalMemoryRSS    int64 `json:"total_memory_rss"`
	TotalMemoryVMS    int64 `json:"total_memory_vms"`
	TotalOpenFiles    int   `json:"total_open_files"`
	TotalThreadCount  int   `json:"total_thread_count"`
	TotalIOReadBytes  int64 `json:"total_io_read_bytes"`
	TotalIOWriteBytes int64 `json:"total_io_write_bytes"`

	// Context count
	ContextCount        int `json:"context_count"`
	RunningContextCount int `json:"running_context_count"`
	PausedContextCount  int `json:"paused_context_count"`

	// Per-context breakdown
	Contexts []ContextResourceUsage `json:"contexts"`
}

// Manager manages contexts in the sandbox.
type Manager struct {
	mu                   sync.RWMutex
	contexts             map[string]*Context
	sandboxEnvVars       map[string]string
	onExit               process.ExitHandler
	onStart              process.StartHandler
	defaultCleanupPolicy CleanupPolicy
	cleanupOnce          sync.Once
	stateStore           contextStateStore
	preserveStateOnExit  atomic.Bool
}

type contextPersistence struct {
	mu                  sync.Mutex
	store               contextStateStore
	record              persistedContext
	preserveStateOnExit *atomic.Bool
	disabled            bool
	hasState            bool
	lastState           process.ProcessState
}

func (p *contextPersistence) save(state process.ProcessState, preserveOnExit bool) error {
	if p == nil || p.store == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.disabled || (preserveOnExit && p.preserveStateOnExit != nil && p.preserveStateOnExit.Load()) {
		return nil
	}
	if p.hasState && p.lastState == state {
		return nil
	}
	record := p.record
	record.DesiredState = state
	if err := p.store.Save(record); err != nil {
		return err
	}
	p.hasState = true
	p.lastState = state
	return nil
}

func (p *contextPersistence) disable() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.disabled = true
	p.mu.Unlock()
}

func (p *contextPersistence) disableAndDelete() error {
	if p == nil || p.store == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disabled = true
	return p.store.Delete(p.record.ID)
}

// NewManager creates a new context manager.
func NewManager() *Manager {
	return &Manager{
		contexts: make(map[string]*Context),
	}
}

// SetStateStore enables durable context-definition persistence.
func (m *Manager) SetStateStore(store contextStateStore) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateStore = store
}

// SetExitHandler sets a global exit handler for new contexts.
func (m *Manager) SetExitHandler(handler process.ExitHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onExit = handler
}

// SetStartHandler sets a global start handler for new contexts and restarts.
func (m *Manager) SetStartHandler(handler process.StartHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onStart = handler
}

// SetDefaultCleanupPolicy sets the default cleanup policy for new contexts.
func (m *Manager) SetDefaultCleanupPolicy(policy CleanupPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultCleanupPolicy = policy
}

// SetSandboxEnvVars sets sandbox-level default environment variables for new contexts.
func (m *Manager) SetSandboxEnvVars(envVars map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sandboxEnvVars = process.CloneEnvVars(envVars)
}

// SandboxEnvVars returns a copy of the sandbox-level default environment variables.
func (m *Manager) SandboxEnvVars() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return process.CloneEnvVars(m.sandboxEnvVars)
}

// StartCleanup starts a background cleanup loop.
func (m *Manager) StartCleanup(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	m.cleanupOnce.Do(func() {
		ticker := time.NewTicker(interval)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					m.cleanupExpired()
				}
			}
		}()
	})
}

// CreateContext creates a new context.
func (m *Manager) CreateContext(config process.ProcessConfig) (*Context, error) {
	return m.CreateContextWithPolicyAndREPLConfig(config, nil, CleanupPolicy{})
}

// CreateContextWithPolicyAndREPLConfig creates a new context with a cleanup policy and optional REPL config.
func (m *Manager) CreateContextWithPolicyAndREPLConfig(config process.ProcessConfig, replConfig *repl.REPLConfig, policy CleanupPolicy) (*Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createContextLocked(newContextID(), time.Time{}, config, replConfig, policy, true, true)
}

func (m *Manager) createContextLocked(id string, createdAt time.Time, config process.ProcessConfig, replConfig *repl.REPLConfig, policy CleanupPolicy, mergeSandboxEnv, persistBeforeStart bool) (*Context, error) {
	if _, exists := m.contexts[id]; exists {
		return nil, fmt.Errorf("context %s already exists", id)
	}
	if mergeSandboxEnv {
		config.EnvVars = process.MergeEnvVars(m.sandboxEnvVars, config.EnvVars)
	} else {
		config = cloneProcessConfig(config)
	}
	if mergeSandboxEnv && policy.isZero() {
		policy = m.defaultCleanupPolicy
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	baseRecord := persistedContext{
		ID:            id,
		Config:        cloneProcessConfig(config),
		CleanupPolicy: policy,
		DesiredState:  process.ProcessStateRunning,
		CreatedAt:     createdAt,
	}
	if replConfig != nil {
		baseRecord.REPLConfig = replConfig.Clone()
	}
	persistence := &contextPersistence{
		store:               m.stateStore,
		record:              baseRecord,
		preserveStateOnExit: &m.preserveStateOnExit,
	}
	if persistBeforeStart {
		if err := persistence.save(process.ProcessStateRunning, false); err != nil {
			return nil, fmt.Errorf("persist context before start: %w", err)
		}
	}

	onExit := m.onExit
	onStart := m.onStart
	exitHandler := func(event process.ExitEvent) {
		_ = persistence.save(event.State, true)
		if onExit != nil {
			onExit(event)
		}
	}
	startHandler := func(event process.StartEvent) {
		_ = persistence.save(process.ProcessStateRunning, false)
		if onStart != nil {
			onStart(event)
		}
	}

	ctx, err := newContextWithID(id, createdAt, config, replConfig, exitHandler, startHandler)
	if err != nil {
		if persistBeforeStart {
			_ = persistence.disableAndDelete()
		}
		return nil, err
	}
	ctx.SetCleanupPolicy(policy)
	ctx.persistence = persistence
	m.contexts[id] = ctx
	return ctx, nil
}

// RestoreContexts recreates contexts that were running or paused before ctld
// replaced the procd container. Process memory, PIDs, and open descriptors are
// intentionally not restored.
func (m *Manager) RestoreContexts() ([]*Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stateStore == nil {
		return nil, nil
	}
	records, err := m.stateStore.Load()
	if err != nil {
		return nil, err
	}
	restored := make([]*Context, 0, len(records))
	var errs []error
	for _, record := range records {
		if record.DesiredState != process.ProcessStateRunning && record.DesiredState != process.ProcessStatePaused {
			continue
		}
		ctx, createErr := m.createContextLocked(record.ID, record.CreatedAt, record.Config, record.REPLConfig, record.CleanupPolicy, false, false)
		if createErr != nil {
			errs = append(errs, fmt.Errorf("restore context %s: %w", record.ID, createErr))
			continue
		}
		if record.DesiredState == process.ProcessStatePaused {
			if pauseErr := ctx.Pause(); pauseErr != nil {
				ctx.persistence.disable()
				_ = ctx.Stop()
				delete(m.contexts, ctx.ID)
				_ = m.stateStore.Save(record)
				errs = append(errs, fmt.Errorf("restore paused context %s: %w", record.ID, pauseErr))
				continue
			}
			if persistErr := m.stateStore.Save(ctx.persistedState(process.ProcessStatePaused)); persistErr != nil {
				errs = append(errs, fmt.Errorf("persist restored context %s: %w", record.ID, persistErr))
				continue
			}
		}
		restored = append(restored, ctx)
	}
	return restored, errors.Join(errs...)
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

	var persistErr error
	if ctx.persistence != nil {
		persistErr = ctx.persistence.disableAndDelete()
	}
	_ = ctx.Stop()

	delete(m.contexts, id)
	if ctx.persistence == nil && m.stateStore != nil {
		persistErr = m.stateStore.Delete(id)
	}
	return persistErr
}

// RestartContext restarts a context.
func (m *Manager) RestartContext(id string) (*Context, error) {
	m.mu.Lock()

	ctx, exists := m.contexts[id]
	if !exists {
		m.mu.Unlock()
		return nil, ErrContextNotFound
	}

	if err := ctx.Restart(); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if m.stateStore != nil {
		if err := ctx.persistence.save(process.ProcessStateRunning, false); err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("persist restarted context: %w", err)
		}
	}

	m.mu.Unlock()
	return ctx, nil
}

// PauseAll pauses all running contexts and their child processes.
// Returns an error if any context fails to pause.
func (m *Manager) PauseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, ctx := range m.contexts {
		if ctx.IsRunning() {
			if err := ctx.Pause(); err != nil {
				errs = append(errs, fmt.Errorf("context %s: %w", ctx.ID, err))
			} else if m.stateStore != nil {
				if err := ctx.persistence.save(process.ProcessStatePaused, false); err != nil {
					errs = append(errs, fmt.Errorf("persist context %s: %w", ctx.ID, err))
				}
			}
		}
	}

	return errors.Join(errs...)
}

// ResumeAll resumes all paused contexts and their child processes.
// Returns an error if any context fails to resume.
func (m *Manager) ResumeAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, ctx := range m.contexts {
		if ctx.IsPaused() {
			if err := ctx.Resume(); err != nil {
				errs = append(errs, fmt.Errorf("context %s: %w", ctx.ID, err))
			} else if m.stateStore != nil {
				if err := ctx.persistence.save(process.ProcessStateRunning, false); err != nil {
					errs = append(errs, fmt.Errorf("persist context %s: %w", ctx.ID, err))
				}
			}
		}
	}
	return errors.Join(errs...)
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
	if err := ctx.MainProcess.WriteInput(data); err != nil {
		return err
	}
	ctx.Touch()
	return nil
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

// ResizePTY resizes the PTY for a context.
func (m *Manager) ResizePTY(contextID string, size process.PTYSize) error {
	ctx, err := m.GetContext(contextID)
	if err != nil {
		return err
	}

	return ctx.ResizePTY(size)
}

// SendSignal sends a signal to a context's process.
func (m *Manager) SendSignal(contextID string, sig syscall.Signal) error {
	ctx, err := m.GetContext(contextID)
	if err != nil {
		return err
	}

	return ctx.SendSignal(sig)
}

// Cleanup cleans up all contexts.
func (m *Manager) Cleanup() {
	m.cleanup(false)
}

// CleanupPreservingState stops local processes without changing their durable
// desired state. A subsequent procd only replays it when ctld supplied a marker.
func (m *Manager) CleanupPreservingState() {
	m.cleanup(true)
}

func (m *Manager) cleanup(preserveState bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preserveStateOnExit.Store(preserveState)

	for _, ctx := range m.contexts {
		if !preserveState && ctx.persistence != nil {
			ctx.persistence.disable()
		}
		ctx.Stop()
	}

	m.contexts = make(map[string]*Context)
	if !preserveState && m.stateStore != nil {
		_ = m.stateStore.Clear()
	}
}

func (m *Manager) cleanupExpired() {
	now := time.Now()
	expiredIDs := make([]string, 0)

	m.mu.RLock()
	for id, ctx := range m.contexts {
		if ctx.shouldCleanup(now) {
			expiredIDs = append(expiredIDs, id)
		}
	}
	m.mu.RUnlock()

	for _, id := range expiredIDs {
		_ = m.DeleteContext(id)
	}
}

// GetResourceUsage returns resource usage for a specific context.
func (m *Manager) GetResourceUsage(contextID string) (*ContextResourceUsage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ctx, exists := m.contexts[contextID]
	if !exists {
		return nil, ErrContextNotFound
	}

	return &ContextResourceUsage{
		ContextID: ctx.ID,
		Type:      ctx.Type,
		Alias:     ctx.Alias,
		Running:   ctx.IsRunning(),
		Paused:    ctx.IsPaused(),
		Usage:     ctx.ResourceUsage(),
	}, nil
}

// GetAllResourceUsage returns aggregated resource usage for the entire sandbox.
func (m *Manager) GetAllResourceUsage() *SandboxResourceUsage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := &SandboxResourceUsage{
		Contexts: make([]ContextResourceUsage, 0, len(m.contexts)),
	}

	if containerStats, err := process.GetContainerResourceUsage(); err == nil {
		result.ContainerMemoryUsage = containerStats.Usage
		result.ContainerMemoryLimit = containerStats.Limit
		result.ContainerMemoryWorkingSet = containerStats.WorkingSet
	}

	for _, ctx := range m.contexts {
		usage := ctx.ResourceUsage()

		ctxUsage := ContextResourceUsage{
			ContextID: ctx.ID,
			Type:      ctx.Type,
			Alias:     ctx.Alias,
			Running:   ctx.IsRunning(),
			Paused:    ctx.IsPaused(),
			Usage:     usage,
		}
		result.Contexts = append(result.Contexts, ctxUsage)

		// Aggregate stats
		result.TotalMemoryRSS += usage.MemoryRSS
		result.TotalMemoryVMS += usage.MemoryVMS
		result.TotalOpenFiles += usage.OpenFiles
		result.TotalThreadCount += usage.ThreadCount
		result.TotalIOReadBytes += usage.IOReadBytes
		result.TotalIOWriteBytes += usage.IOWriteBytes

		// Count states
		result.ContextCount++
		if ctx.IsRunning() {
			result.RunningContextCount++
		}
		if ctx.IsPaused() {
			result.PausedContextCount++
		}
	}

	return result
}
