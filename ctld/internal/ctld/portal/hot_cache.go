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

type hotCacheSegment string

const (
	hotCacheSegmentProbation hotCacheSegment = "probation"
	hotCacheSegmentProtected hotCacheSegment = "protected"

	// A retained engine keeps a WAL descriptor, object-store client, and other
	// fixed state that is not represented by the decoded filesystem state.
	hotCacheMinimumEntryBytes int64 = 1 << 20

	// Proven-reused entries may occupy three quarters of the byte budget. The
	// remaining window lets first-time pause candidates prove reuse without a
	// scan evicting the protected working set.
	hotCacheProtectedTargetNumerator   int64 = 3
	hotCacheProtectedTargetDenominator int64 = 4

	// Runtime bands prevent a few milliseconds of active-time difference from
	// hiding the cold-open cost signal. Any stable workload outranks rapid
	// churn; within a band, expensive large-state opens get priority.
	hotCacheStableRuntime   = 30 * time.Second
	hotCacheLongRuntime     = 5 * time.Minute
	hotCacheVeryLongRuntime = time.Hour
)

func (m *Manager) hotCacheEnabled() bool {
	return m != nil && m.hotCacheMaxBytes > 0
}

func boundMetadataBytes(bound *boundVolume) int64 {
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return 0
	}
	estimated := bound.volCtx.S0FS.EstimatedMemoryBytes()
	if estimated < hotCacheMinimumEntryBytes {
		estimated = hotCacheMinimumEntryBytes
	}
	return estimated
}

// admitActiveMetadata charges active and detached engines to the same node
// budget. The caller holds the candidate volume activation, so detached
// victims can be evicted without racing a second open of the candidate.
func (m *Manager) admitActiveMetadata(bound *boundVolume) error {
	return m.admitActiveMetadataBytes(bound, boundMetadataBytes(bound))
}

func (m *Manager) admitActiveMetadataBytes(bound *boundVolume, charge int64) error {
	if m == nil || bound == nil || bound.metadataCharged {
		return nil
	}
	if charge == 0 {
		return nil
	}
	if m.hotCacheMaxBytes > 0 && charge > m.hotCacheMaxBytes {
		m.observer.ObserveHotCacheAdmission("rejected", "active_oversize")
		m.observer.ObserveMetadataBudgetPressure("active_oversize")
		return fmt.Errorf("S0FS active metadata requires %d bytes, exceeding node budget %d", charge, m.hotCacheMaxBytes)
	}
	for {
		m.mu.Lock()
		if m.hotCacheMaxBytes <= 0 || m.activeMetadataBytes+m.hotCacheBytes+charge <= m.hotCacheMaxBytes {
			m.activeMetadataBytes += charge
			bound.metadataBytes = charge
			bound.metadataCharged = true
			m.observeMetadataResidencyLocked()
			m.mu.Unlock()
			return nil
		}
		victimID, victim := m.leastValuableProbationLocked()
		if victim == nil {
			victimID, victim = m.oldestProtectedExceptLocked(bound.volumeID)
		}
		activeBytes, detachedBytes, budgetBytes := m.activeMetadataBytes, m.hotCacheBytes, m.hotCacheMaxBytes
		m.mu.Unlock()
		if victim == nil {
			m.observer.ObserveHotCacheAdmission("rejected", "active_capacity")
			m.observer.ObserveMetadataBudgetPressure("active_capacity")
			return fmt.Errorf("S0FS node metadata budget exhausted: active=%d detached=%d candidate=%d budget=%d", activeBytes, detachedBytes, charge, budgetBytes)
		}
		m.observer.ObserveMetadataBudgetPressure("active_evict_detached")
		if !m.tryEvictHotVolumeEntry(victimID, victim, "active_capacity") {
			m.observer.ObserveHotCacheAdmission("rejected", "active_victim_busy")
			m.observer.ObserveMetadataBudgetPressure("active_victim_busy")
			return fmt.Errorf("S0FS node metadata budget is blocked by a busy detached engine")
		}
	}
}

func (m *Manager) releaseActiveMetadata(bound *boundVolume) {
	if m == nil || bound == nil || !bound.metadataCharged {
		return
	}
	m.mu.Lock()
	m.releaseActiveMetadataLocked(bound)
	m.mu.Unlock()
}

func (m *Manager) releaseActiveMetadataLocked(bound *boundVolume) {
	if bound == nil || !bound.metadataCharged {
		return
	}
	m.activeMetadataBytes -= bound.metadataBytes
	if m.activeMetadataBytes < 0 {
		m.activeMetadataBytes = 0
	}
	bound.metadataBytes = 0
	bound.metadataCharged = false
	m.observeMetadataResidencyLocked()
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
	if err := m.admitActiveMetadata(bound); err != nil {
		m.closeHotVolume(entry)
		return nil, nil, true, err
	}

	cleanup := func() {
		m.releaseActiveMetadata(bound)
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
	m.mu.Lock()
	entry := m.hotVolumes[volumeID]
	if entry != nil {
		m.removeHotVolumeLocked(volumeID, entry)
	}
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()
	if entry == nil {
		m.observer.ObserveHotCacheRequest("miss", "none")
		return nil, false
	}
	m.observer.ObserveHotCacheRequest("hit", string(entry.segment))
	m.observer.ObserveHotCacheResidence(string(entry.segment), "hit", time.Since(entry.cachedAt))
	return entry, true
}

// retainHotVolume is called while the volume activation is held so replacing
// an entry cannot overlap another engine open on the same WAL path.
func (m *Manager) retainHotVolume(bound *boundVolume) {
	if bound == nil || bound.volCtx == nil || bound.volCtx.S0FS == nil {
		return
	}
	m.releaseActiveMetadata(bound)
	now := time.Now().UTC()
	mountedDuration := time.Duration(0)
	if !bound.mountedAt.IsZero() && now.After(bound.mountedAt) {
		mountedDuration = now.Sub(bound.mountedAt)
	}
	segment := hotCacheSegmentProbation
	if bound.hotReuse {
		segment = hotCacheSegmentProtected
	}
	estimatedBytes := boundMetadataBytes(bound)
	entry := &hotVolume{
		bound:            bound,
		cachedAt:         now,
		estimatedBytes:   estimatedBytes,
		segment:          segment,
		mountedDuration:  mountedDuration,
		coldOpenDuration: bound.coldOpenDuration,
	}
	m.observer.ObserveHotCacheCandidate(string(segment), estimatedBytes, mountedDuration, bound.coldOpenDuration)
	if !m.hotCacheEnabled() {
		m.observer.ObserveHotCacheAdmission("rejected", "disabled")
		m.closeHotVolume(entry)
		return
	}
	if entry.estimatedBytes > m.hotCacheMaxBytes {
		m.observer.ObserveHotCacheAdmission("rejected", "oversize")
		m.closeHotVolume(entry)
		return
	}

	var replaced *hotVolume
	m.mu.Lock()
	if previous := m.hotVolumes[bound.volumeID]; previous != nil {
		m.removeHotVolumeLocked(bound.volumeID, previous)
		replaced = previous
	}
	m.addHotVolumeLocked(bound.volumeID, entry)
	m.observeHotCacheSizeLocked()
	m.mu.Unlock()

	if replaced != nil {
		m.observer.ObserveHotCacheEviction("replaced", string(replaced.segment))
		m.observer.ObserveHotCacheResidence(string(replaced.segment), "replaced", time.Since(replaced.cachedAt))
		m.closeHotVolume(replaced)
	}

	if segment == hotCacheSegmentProtected {
		m.rebalanceProtectedSegment(entry)
	}
	for {
		m.mu.Lock()
		if m.activeMetadataBytes+m.hotCacheBytes <= m.hotCacheMaxBytes {
			admittedSegment := entry.segment
			m.observeHotCacheSizeLocked()
			m.mu.Unlock()
			m.observer.ObserveHotCacheAdmission("admitted", string(admittedSegment))
			return
		}
		m.observer.ObserveMetadataBudgetPressure("detached_capacity")
		victimID, victim := m.capacityVictimLocked(entry)
		if victim == entry {
			m.removeHotVolumeLocked(victimID, victim)
			m.observeHotCacheSizeLocked()
			m.mu.Unlock()
			m.observer.ObserveHotCacheAdmission("rejected", "capacity")
			m.closeHotVolume(entry)
			return
		}
		m.mu.Unlock()
		if victim == nil {
			m.observer.ObserveHotCacheAdmission("rejected", "capacity")
			m.evictHotVolume(bound.volumeID, "capacity")
			return
		}
		if !m.tryEvictHotVolumeEntry(victimID, victim, "capacity") {
			// retainHotVolume runs while the candidate activation is held. A
			// blocking wait for another volume could deadlock with a concurrent
			// admission choosing this candidate as its victim, so reject the
			// candidate when the selected victim is busy.
			m.observer.ObserveHotCacheAdmission("rejected", "victim_busy")
			m.evictHotVolume(bound.volumeID, "victim_busy")
			return
		}
	}
}

func (m *Manager) rebalanceProtectedSegment(candidate *hotVolume) {
	if m == nil || candidate == nil {
		return
	}
	available := m.hotCacheMaxBytes - m.activeMetadataBytes
	if available < 0 {
		available = 0
	}
	target := available * hotCacheProtectedTargetNumerator / hotCacheProtectedTargetDenominator
	for {
		m.mu.Lock()
		if m.hotCacheProtectedBytes <= target {
			m.mu.Unlock()
			return
		}
		volumeID, entry := m.oldestProtectedExceptLocked(candidate.bound.volumeID)
		if entry == nil {
			m.mu.Unlock()
			return
		}
		if m.hotVolumes[volumeID] == entry && entry.segment == hotCacheSegmentProtected {
			m.setHotVolumeSegmentLocked(entry, hotCacheSegmentProbation)
			m.observeHotCacheSizeLocked()
		}
		m.mu.Unlock()
	}
}

func (m *Manager) capacityVictimLocked(candidate *hotVolume) (string, *hotVolume) {
	if candidate == nil {
		return "", nil
	}
	if volumeID, entry := m.leastValuableProbationLocked(); entry != nil {
		return volumeID, entry
	}
	return m.oldestProtectedExceptLocked(candidate.bound.volumeID)
}

func (m *Manager) tryEvictHotVolumeEntry(volumeID string, expected *hotVolume, reason string) bool {
	release, ok := m.tryAcquireVolumeActivation(volumeID)
	if !ok {
		return false
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
		m.observer.ObserveHotCacheEviction(reason, string(entry.segment))
		m.observer.ObserveHotCacheResidence(string(entry.segment), reason, time.Since(entry.cachedAt))
		m.closeHotVolume(entry)
	}
	return true
}

func (m *Manager) evictHotVolumeWithActivation(ctx context.Context, volumeID, reason string) error {
	if m == nil || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	release, err := m.acquireVolumeActivation(ctx, volumeID)
	if err != nil {
		return err
	}
	defer release()
	m.evictHotVolume(volumeID, reason)
	return nil
}

// evictHotVolume requires the caller to hold the per-volume activation when a
// bind or cleanup for the same volume may still be running.
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
		m.observer.ObserveHotCacheEviction(reason, string(entry.segment))
		m.observer.ObserveHotCacheResidence(string(entry.segment), reason, time.Since(entry.cachedAt))
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
	m.hotCacheProbationBytes = 0
	m.hotCacheProtectedBytes = 0
	m.observeHotCacheSizeLocked()
	m.observeMetadataResidencyLocked()
	m.mu.Unlock()
	for _, entry := range entries {
		m.observer.ObserveHotCacheEviction(reason, string(entry.segment))
		m.observer.ObserveHotCacheResidence(string(entry.segment), reason, time.Since(entry.cachedAt))
	}
	return entries
}

func (m *Manager) addHotVolumeLocked(volumeID string, entry *hotVolume) {
	m.hotVolumes[volumeID] = entry
	m.hotCacheBytes += entry.estimatedBytes
	switch entry.segment {
	case hotCacheSegmentProtected:
		m.hotCacheProtectedBytes += entry.estimatedBytes
	default:
		m.hotCacheProbationBytes += entry.estimatedBytes
	}
	m.observeMetadataResidencyLocked()
}

func (m *Manager) removeHotVolumeLocked(volumeID string, entry *hotVolume) {
	if current := m.hotVolumes[volumeID]; current != entry {
		return
	}
	delete(m.hotVolumes, volumeID)
	m.hotCacheBytes -= entry.estimatedBytes
	switch entry.segment {
	case hotCacheSegmentProtected:
		m.hotCacheProtectedBytes -= entry.estimatedBytes
	default:
		m.hotCacheProbationBytes -= entry.estimatedBytes
	}
	if m.hotCacheBytes < 0 {
		m.hotCacheBytes = 0
	}
	if m.hotCacheProbationBytes < 0 {
		m.hotCacheProbationBytes = 0
	}
	if m.hotCacheProtectedBytes < 0 {
		m.hotCacheProtectedBytes = 0
	}
	m.observeMetadataResidencyLocked()
}

func (m *Manager) setHotVolumeSegmentLocked(entry *hotVolume, segment hotCacheSegment) {
	if entry == nil || entry.segment == segment {
		return
	}
	switch entry.segment {
	case hotCacheSegmentProtected:
		m.hotCacheProtectedBytes -= entry.estimatedBytes
	default:
		m.hotCacheProbationBytes -= entry.estimatedBytes
	}
	entry.segment = segment
	switch segment {
	case hotCacheSegmentProtected:
		m.hotCacheProtectedBytes += entry.estimatedBytes
	default:
		m.hotCacheProbationBytes += entry.estimatedBytes
	}
}

func (m *Manager) leastValuableProbationLocked() (string, *hotVolume) {
	var (
		victimID string
		victim   *hotVolume
	)
	for volumeID, entry := range m.hotVolumes {
		if entry == nil || entry.segment != hotCacheSegmentProbation {
			continue
		}
		if victim == nil || lessValuableHotVolume(entry, victim) {
			victimID = volumeID
			victim = entry
		}
	}
	return victimID, victim
}

func lessValuableHotVolume(left, right *hotVolume) bool {
	if left == nil {
		return true
	}
	if right == nil {
		return false
	}
	leftRuntimeBand := hotCacheRuntimeBand(left.mountedDuration)
	rightRuntimeBand := hotCacheRuntimeBand(right.mountedDuration)
	if leftRuntimeBand != rightRuntimeBand {
		return leftRuntimeBand < rightRuntimeBand
	}
	if left.coldOpenDuration != right.coldOpenDuration {
		return left.coldOpenDuration < right.coldOpenDuration
	}
	if left.mountedDuration != right.mountedDuration {
		return left.mountedDuration < right.mountedDuration
	}
	return left.cachedAt.Before(right.cachedAt)
}

func hotCacheRuntimeBand(duration time.Duration) int {
	switch {
	case duration >= hotCacheVeryLongRuntime:
		return 3
	case duration >= hotCacheLongRuntime:
		return 2
	case duration >= hotCacheStableRuntime:
		return 1
	default:
		return 0
	}
}

func (m *Manager) oldestProtectedExceptLocked(exceptVolumeID string) (string, *hotVolume) {
	var (
		oldestID string
		oldest   *hotVolume
	)
	for volumeID, entry := range m.hotVolumes {
		if volumeID == exceptVolumeID || entry == nil || entry.segment != hotCacheSegmentProtected {
			continue
		}
		if oldest == nil || entry.cachedAt.Before(oldest.cachedAt) {
			oldestID = volumeID
			oldest = entry
		}
	}
	return oldestID, oldest
}

func (m *Manager) observeHotCacheSizeLocked() {
	if m == nil || m.observer == nil {
		return
	}
	probationEntries := 0
	protectedEntries := 0
	for _, entry := range m.hotVolumes {
		if entry != nil && entry.segment == hotCacheSegmentProtected {
			protectedEntries++
		} else if entry != nil {
			probationEntries++
		}
	}
	m.observer.SetHotCacheSize(
		len(m.hotVolumes),
		m.hotCacheBytes,
		probationEntries,
		m.hotCacheProbationBytes,
		protectedEntries,
		m.hotCacheProtectedBytes,
	)
}

func (m *Manager) observeMetadataResidencyLocked() {
	if m == nil || m.observer == nil {
		return
	}
	m.observer.SetMetadataResidency(m.activeMetadataBytes, m.hotCacheBytes)
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
