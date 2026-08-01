package portal

import (
	"context"
	"fmt"
	"strings"
)

// volumeActivation serializes engine creation and hot-cache claims for one
// volume without blocking unrelated portal binds.
type volumeActivation struct {
	semaphore chan struct{}
	users     int
}

func (m *Manager) acquireVolumeActivation(ctx context.Context, volumeID string) (func(), error) {
	if m == nil {
		return nil, fmt.Errorf("portal manager is unavailable")
	}
	volumeID = strings.TrimSpace(volumeID)
	if volumeID == "" {
		return nil, fmt.Errorf("volume id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.activationMu.Lock()
	if m.activations == nil {
		m.activations = make(map[string]*volumeActivation)
	}
	activation := m.activations[volumeID]
	if activation == nil {
		activation = &volumeActivation{semaphore: make(chan struct{}, 1)}
		m.activations[volumeID] = activation
	}
	activation.users++
	m.activationMu.Unlock()

	select {
	case activation.semaphore <- struct{}{}:
	case <-ctx.Done():
		m.releaseVolumeActivationUser(volumeID, activation)
		return nil, ctx.Err()
	}

	var released bool
	return func() {
		m.activationMu.Lock()
		if released {
			m.activationMu.Unlock()
			return
		}
		released = true
		<-activation.semaphore
		activation.users--
		if activation.users == 0 && m.activations[volumeID] == activation {
			delete(m.activations, volumeID)
		}
		m.activationMu.Unlock()
	}, nil
}

// tryAcquireVolumeActivation acquires an idle per-volume activation without
// waiting. Cache admission uses it while holding another volume's activation,
// so a busy victim must be skipped to avoid cross-volume lock inversion.
func (m *Manager) tryAcquireVolumeActivation(volumeID string) (func(), bool) {
	if m == nil {
		return nil, false
	}
	volumeID = strings.TrimSpace(volumeID)
	if volumeID == "" {
		return nil, false
	}

	m.activationMu.Lock()
	if m.activations == nil {
		m.activations = make(map[string]*volumeActivation)
	}
	activation := m.activations[volumeID]
	if activation == nil {
		activation = &volumeActivation{semaphore: make(chan struct{}, 1)}
		m.activations[volumeID] = activation
	}
	activation.users++
	m.activationMu.Unlock()

	select {
	case activation.semaphore <- struct{}{}:
		var released bool
		return func() {
			m.activationMu.Lock()
			if released {
				m.activationMu.Unlock()
				return
			}
			released = true
			<-activation.semaphore
			activation.users--
			if activation.users == 0 && m.activations[volumeID] == activation {
				delete(m.activations, volumeID)
			}
			m.activationMu.Unlock()
		}, true
	default:
		m.releaseVolumeActivationUser(volumeID, activation)
		return nil, false
	}
}

func (m *Manager) releaseVolumeActivationUser(volumeID string, activation *volumeActivation) {
	m.activationMu.Lock()
	defer m.activationMu.Unlock()
	activation.users--
	if activation.users == 0 && m.activations[volumeID] == activation {
		delete(m.activations, volumeID)
	}
}
