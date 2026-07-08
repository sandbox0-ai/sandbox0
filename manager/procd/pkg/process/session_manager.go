package process

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SessionManager owns process sessions in a sandbox.
type SessionManager struct {
	mu                   sync.RWMutex
	sessions             map[string]*Session
	sandboxEnvVars       map[string]string
	defaultCleanupPolicy ProcessCleanupSpec
	cleanupOnce          sync.Once
}

// NewSessionManager creates a process-session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*Session),
	}
}

// SetDefaultCleanupPolicy sets cleanup defaults for newly created sessions.
func (m *SessionManager) SetDefaultCleanupPolicy(policy ProcessCleanupSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultCleanupPolicy = policy
}

// SetSandboxEnvVars sets sandbox-level environment variables for new sessions.
func (m *SessionManager) SetSandboxEnvVars(envVars map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sandboxEnvVars = CloneEnvVars(envVars)
}

// SandboxEnvVars returns a copy of sandbox-level environment variables.
func (m *SessionManager) SandboxEnvVars() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return CloneEnvVars(m.sandboxEnvVars)
}

// CreateSession creates and starts a process session.
func (m *SessionManager) CreateSession(spec ProcessSpec) (*Session, error) {
	m.mu.Lock()
	defaultCleanup := m.defaultCleanupPolicy
	spec.EnvVars = MergeEnvVars(m.sandboxEnvVars, spec.EnvVars)
	if spec.Cleanup == (ProcessCleanupSpec{}) {
		spec.Cleanup = defaultCleanup
	}
	m.mu.Unlock()

	session, err := NewSession("proc_"+uuid.NewString(), spec)
	if err != nil {
		return nil, err
	}
	if err := session.Start(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.sessions[session.ID()] = session
	m.mu.Unlock()
	return session, nil
}

// GetSession returns a process session by ID.
func (m *SessionManager) GetSession(id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, ok := m.sessions[id]
	if !ok {
		return nil, ErrProcessSessionNotFound
	}
	return session, nil
}

// ListSessions returns all process sessions.
func (m *SessionManager) ListSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		result = append(result, session)
	}
	return result
}

// DeleteSession stops and removes a process session.
func (m *SessionManager) DeleteSession(id string) error {
	m.mu.Lock()
	session, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrProcessSessionNotFound
	}
	delete(m.sessions, id)
	m.mu.Unlock()
	return session.Stop()
}

// Cleanup stops and removes all process sessions.
func (m *SessionManager) Cleanup() {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.sessions = make(map[string]*Session)
	m.mu.Unlock()
	for _, session := range sessions {
		_ = session.Stop()
	}
}

// StartCleanup starts cleanup for expired process sessions.
func (m *SessionManager) StartCleanup(ctx context.Context, interval time.Duration) {
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

func (m *SessionManager) cleanupExpired() {
	now := time.Now()
	expiredIDs := make([]string, 0)

	m.mu.RLock()
	for id, session := range m.sessions {
		if session.shouldCleanup(now) {
			expiredIDs = append(expiredIDs, id)
		}
	}
	m.mu.RUnlock()

	for _, id := range expiredIDs {
		_ = m.DeleteSession(id)
	}
}
