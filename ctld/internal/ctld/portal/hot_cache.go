package portal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"go.uber.org/zap"
)

func (m *Manager) hotCacheEnabled() bool {
	return m != nil && m.hotCacheTTL > 0 && m.hotCacheMaxEntries > 0 && m.hotCacheMaxBytes > 0
}

func (m *Manager) activateHotBoundVolume(
	_ context.Context,
	req ctldapi.BindVolumePortalRequest,
	volumeRecord *db.SandboxVolume,
	accessMode volume.AccessMode,
	mountedAt time.Time,
) (*boundVolume, func(), bool, error) {
	entry, ok := m.takeHotVolume(req.SandboxVolumeID)
	if !ok {
		return nil, nil, false, nil
	}
	bound := entry.bound
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		m.closeHotVolume(entry)
		return nil, nil, true, fmt.Errorf("hot S0FS volume %s has no engine", req.SandboxVolumeID)
	}
	bound.teamID = volumeRecord.TeamID
	bound.access = accessMode
	bound.mountedAt = mountedAt
	bound.refCount = 1
	bound.closing = false
	bound.hotReuse = true
	bound.statePath = m.volumeStatePath(req.SandboxVolumeID)
	bound.volCtx.TeamID = volumeRecord.TeamID
	bound.volCtx.Access = accessMode
	bound.volCtx.MountedAt = mountedAt
	bound.volCtx.RestoreHandleState(volume.HandleState{})
	bound.volCtx.S0FS.PruneUnlinked(nil)
	bound.session = newLocalSession(req.SandboxVolumeID, m.volumes, m.logrus)
	if session, ok := bound.session.(*localSession); ok {
		session.statePath = bound.statePath
		session.incrementalReady = m.incrementalS0FSHandleRecoveryReady
	}

	cleanup := func() {
		if bound.ownerRegistered {
			m.unregisterOwner(bound)
		}
		closeBoundSession(bound)
		m.closeHotVolume(&hotVolume{bound: bound, estimatedBytes: entry.estimatedBytes})
	}
	compactStarted := time.Now()
	if err := compactS0FSHandleState(bound.statePath, req.SandboxVolumeID, bound.volCtx.SnapshotHandleState(), m.localRecoverySyncRequired(), nil); err != nil {
		m.observer.ObservePhase("bind", "handle_state_compact", "local", 0, req.SandboxVolumeID, compactStarted, err)
		cleanup()
		return nil, nil, true, err
	}
	m.observer.ObservePhase("bind", "handle_state_compact", "local", 0, req.SandboxVolumeID, compactStarted, nil)
	return bound, cleanup, true, nil
}

func (m *Manager) prepareHotBoundAfterOwner(ctx context.Context, bound *boundVolume) error {
	if bound == nil || !bound.hotReuse || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return nil
	}
	current, err := bound.volCtx.S0FS.CommittedHeadCurrent(ctx)
	if err != nil {
		return fmt.Errorf("validate hot S0FS committed head: %w", err)
	}
	if current {
		return nil
	}
	if _, err := bound.volCtx.S0FS.RefreshMaterialized(ctx); err != nil {
		return fmt.Errorf("refresh stale hot S0FS engine: %w", err)
	}
	current, err = bound.volCtx.S0FS.CommittedHeadCurrent(ctx)
	if err != nil {
		return fmt.Errorf("revalidate hot S0FS committed head: %w", err)
	}
	if !current {
		return fmt.Errorf("hot S0FS committed head remained stale after refresh")
	}
	return nil
}

func (m *Manager) takeHotVolume(volumeID string) (*hotVolume, bool) {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return nil, false
	}
	now := time.Now().UTC()
	var expired *hotVolume
	m.mu.Lock()
	entry := m.hotVolumes[volumeID]
	if entry != nil && (m.hotCacheTTL <= 0 || now.Sub(entry.cachedAt) >= m.hotCacheTTL) {
		expired = entry
		m.removeHotVolumeLocked(volumeID, entry)
		entry = nil
	}
	if entry != nil {
		m.removeHotVolumeLocked(volumeID, entry)
	}
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()
	if expired != nil {
		m.observer.ObserveHotCacheEviction("expired")
		m.closeHotVolume(expired)
	}
	if entry == nil {
		m.observer.ObserveHotCacheRequest("miss")
		return nil, false
	}
	m.observer.ObserveHotCacheRequest("hit")
	return entry, true
}

// retainHotVolume is called while the volume activation is held so replacing
// an entry cannot overlap another engine open on the same WAL path.
func (m *Manager) retainHotVolume(bound *boundVolume) {
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return
	}
	entry := &hotVolume{
		bound:          bound,
		cachedAt:       time.Now().UTC(),
		estimatedBytes: bound.volCtx.S0FS.EstimatedMemoryBytes(),
	}
	if !m.hotCacheEnabled() || entry.estimatedBytes > m.hotCacheMaxBytes {
		reason := "disabled"
		if entry.estimatedBytes > m.hotCacheMaxBytes && m.hotCacheMaxBytes > 0 {
			reason = "oversize"
		}
		m.observer.ObserveHotCacheEviction(reason)
		m.closeHotVolume(entry)
		return
	}

	var replaced *hotVolume
	m.mu.Lock()
	if previous := m.hotVolumes[bound.volumeID]; previous != nil {
		m.removeHotVolumeLocked(bound.volumeID, previous)
		replaced = previous
	}
	m.hotVolumes[bound.volumeID] = entry
	m.hotCacheBytes += entry.estimatedBytes
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()

	if replaced != nil {
		m.observer.ObserveHotCacheEviction("replaced")
		m.closeHotVolume(replaced)
	}
	for {
		m.mu.Lock()
		if len(m.hotVolumes) <= m.hotCacheMaxEntries && m.hotCacheBytes <= m.hotCacheMaxBytes {
			m.observeHotCacheSizeLocked()
			m.mu.Unlock()
			return
		}
		volumeID, oldest := m.oldestHotVolumeExceptLocked(bound.volumeID)
		m.mu.Unlock()
		if oldest == nil {
			return
		}
		m.evictHotVolumeEntry(volumeID, oldest, "capacity")
	}
}

func (m *Manager) cleanupExpiredHotVolumes() {
	if m == nil || m.hotCacheTTL <= 0 {
		return
	}
	cutoff := time.Now().UTC().Add(-m.hotCacheTTL)
	for {
		var (
			volumeID string
			expired  *hotVolume
		)
		m.mu.Lock()
		for candidateID, entry := range m.hotVolumes {
			if entry != nil && !entry.cachedAt.After(cutoff) {
				volumeID = candidateID
				expired = entry
				break
			}
		}
		m.mu.Unlock()
		if expired == nil {
			return
		}
		m.evictHotVolumeEntry(volumeID, expired, "expired")
	}
}

func (m *Manager) evictHotVolumeEntry(volumeID string, expected *hotVolume, reason string) {
	release, err := m.acquireVolumeActivation(context.Background(), volumeID)
	if err != nil {
		return
	}
	defer release()
	m.mu.Lock()
	entry := m.hotVolumes[volumeID]
	if entry == expected {
		m.removeHotVolumeLocked(volumeID, entry)
	} else {
		entry = nil
	}
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()
	if entry != nil {
		m.observer.ObserveHotCacheEviction(reason)
		m.closeHotVolume(entry)
	}
}

func (m *Manager) evictHotVolume(volumeID, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	entry := m.hotVolumes[volumeID]
	if entry != nil {
		m.removeHotVolumeLocked(volumeID, entry)
	}
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()
	if entry != nil {
		m.observer.ObserveHotCacheEviction(reason)
		m.closeHotVolume(entry)
	}
}

func (m *Manager) drainHotVolumes(reason string) []*hotVolume {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	entries := make([]*hotVolume, 0, len(m.hotVolumes))
	for volumeID, entry := range m.hotVolumes {
		entries = append(entries, entry)
		delete(m.hotVolumes, volumeID)
	}
	m.hotCacheBytes = 0
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()
	for range entries {
		m.observer.ObserveHotCacheEviction(reason)
	}
	return entries
}

func (m *Manager) removeHotVolumeLocked(volumeID string, entry *hotVolume) {
	if current := m.hotVolumes[volumeID]; current != entry {
		return
	}
	delete(m.hotVolumes, volumeID)
	m.hotCacheBytes -= entry.estimatedBytes
	if m.hotCacheBytes < 0 {
		m.hotCacheBytes = 0
	}
}

func (m *Manager) oldestHotVolumeExceptLocked(exceptVolumeID string) (string, *hotVolume) {
	var (
		oldestID string
		oldest   *hotVolume
	)
	for volumeID, entry := range m.hotVolumes {
		if volumeID == exceptVolumeID {
			continue
		}
		if entry == nil || (oldest != nil && !entry.cachedAt.Before(oldest.cachedAt)) {
			continue
		}
		oldestID = volumeID
		oldest = entry
	}
	return oldestID, oldest
}

func (m *Manager) observeHotCacheSizeLocked() {
	if m != nil && m.observer != nil {
		m.observer.SetHotCacheSize(len(m.hotVolumes), m.hotCacheBytes)
	}
}

func (m *Manager) closeHotVolume(entry *hotVolume) {
	if entry == nil || entry.bound == nil || entry.bound.volCtx == nil {
		return
	}
	bound := entry.bound
	closeBoundSession(bound)
	if bound.volCtx.S0FS != nil {
		if err := bound.volCtx.S0FS.Close(); err != nil && m != nil && m.logger != nil {
			m.logger.Warn("ctld hot S0FS engine close failed", zap.String("volume_id", bound.volumeID), zap.Error(err))
		}
	}
	if cacheDir := strings.TrimSpace(bound.volCtx.CacheDir); cacheDir != "" {
		if err := os.RemoveAll(cacheDir); err != nil && m != nil && m.logger != nil {
			m.logger.Warn("ctld hot S0FS cache cleanup failed", zap.String("volume_id", bound.volumeID), zap.Error(err))
		}
	}
}
