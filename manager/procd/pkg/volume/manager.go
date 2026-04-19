package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
)

type mountInfo struct {
	volumeID       string
	mountPoint     string
	sandboxID      string
	mountedAt      time.Time
	mountSessionID string
	backend        string
	attachmentID   string
}

// Manager manages sandbox volume mounts in a sandbox.
type Manager struct {
	cfg           *Config
	tokenProvider TokenProvider
	logger        *zap.Logger

	mu          sync.RWMutex
	mounts      map[string]*mountInfo
	mountPoints map[string]string
	mounting    map[string]struct{}
	mountStatus map[string]*MountStatus
	statusCond  *sync.Cond

	sandboxID string
	teamID    string

	ctldVolumeClient CtldVolumeClient
}

// NewManager creates a new volume manager.
func NewManager(cfg *Config, tokenProvider TokenProvider, logger *zap.Logger) *Manager {
	mgr := &Manager{
		cfg:           cfg,
		tokenProvider: tokenProvider,
		logger:        logger,
		mounts:        make(map[string]*mountInfo),
		mountPoints:   make(map[string]string),
		mounting:      make(map[string]struct{}),
		mountStatus:   make(map[string]*MountStatus),
	}
	mgr.statusCond = sync.NewCond(&mgr.mu)
	if cfg != nil && strings.TrimSpace(cfg.CtldBaseURL) != "" {
		client, err := NewHTTPCtldVolumeClient(cfg.CtldBaseURL, cfg.CtldTimeout, tokenProvider)
		if err == nil {
			mgr.ctldVolumeClient = client
		} else if logger != nil {
			logger.Warn("Failed to configure ctld volume client", zap.Error(err))
		}
	}
	return mgr
}

// SetCtldVolumeClient overrides the ctld volume client. It is primarily used
// by tests.
func (m *Manager) SetCtldVolumeClient(client CtldVolumeClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctldVolumeClient = client
}

// SetIdentity records the sandbox identity used for dynamic node-local mounts.
func (m *Manager) SetIdentity(sandboxID, teamID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sandboxID != "" {
		m.sandboxID = sandboxID
	}
	if teamID != "" {
		m.teamID = teamID
	}
}

// BootstrapMounts schedules sandbox bootstrap mounts and optionally waits for them
// to reach a terminal state.
func (m *Manager) BootstrapMounts(ctx context.Context, reqs []MountRequest, wait bool, waitTimeout time.Duration) ([]MountStatus, error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	prepared := make([]MountRequest, 0, len(reqs))
	reserved := make([]string, 0, len(reqs))
	batchMountPoints := make(map[string]string, len(reqs))
	batchVolumes := make(map[string]struct{}, len(reqs))

	m.mu.Lock()
	for _, req := range reqs {
		if req.SandboxVolumeID == "" {
			m.rollbackBootstrapReservationsLocked(reserved)
			m.mu.Unlock()
			return nil, fmt.Errorf("missing volume id")
		}
		if err := m.validateMountPoint(req.MountPoint); err != nil {
			m.rollbackBootstrapReservationsLocked(reserved)
			m.mu.Unlock()
			return nil, err
		}

		mountPoint := filepath.Clean(req.MountPoint)
		if _, exists := batchVolumes[req.SandboxVolumeID]; exists {
			m.rollbackBootstrapReservationsLocked(reserved)
			m.mu.Unlock()
			return nil, ErrVolumeAlreadyMounted
		}
		if existing, ok := batchMountPoints[mountPoint]; ok && existing != req.SandboxVolumeID {
			m.rollbackBootstrapReservationsLocked(reserved)
			m.mu.Unlock()
			return nil, ErrMountPointInUse
		}
		if err := m.reserveMountLocked(req.SandboxVolumeID, mountPoint); err != nil {
			m.rollbackBootstrapReservationsLocked(reserved)
			m.mu.Unlock()
			return nil, err
		}

		preparedReq := req
		preparedReq.MountPoint = mountPoint
		prepared = append(prepared, preparedReq)
		reserved = append(reserved, req.SandboxVolumeID)
		batchVolumes[req.SandboxVolumeID] = struct{}{}
		batchMountPoints[mountPoint] = req.SandboxVolumeID
		m.mountStatus[req.SandboxVolumeID] = &MountStatus{
			SandboxVolumeID: req.SandboxVolumeID,
			MountPoint:      mountPoint,
			State:           MountStatePending,
		}
	}
	m.statusCond.Broadcast()
	m.mu.Unlock()

	for _, req := range prepared {
		go m.runBootstrapMount(req)
	}

	if !wait {
		return m.snapshotStatuses(prepared), nil
	}
	return m.waitForMounts(ctx, prepared, waitTimeout), nil
}

// Mount mounts a sandbox volume at the specified mount point.
func (m *Manager) Mount(ctx context.Context, req *MountRequest) (*MountResponse, error) {
	return m.mount(ctx, req, false)
}

func (m *Manager) mount(ctx context.Context, req *MountRequest, reserved bool) (*MountResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("missing request")
	}
	m.applyIdentity(req)
	if req.SandboxVolumeID == "" {
		return nil, fmt.Errorf("missing volume id")
	}
	if err := m.validateMountPoint(req.MountPoint); err != nil {
		return nil, err
	}
	mountPoint := filepath.Clean(req.MountPoint)

	if reserved {
		m.markMountState(req.SandboxVolumeID, mountPoint, MountStateMounting, "", "")
	}

	if err := m.ensureMountPoint(mountPoint); err != nil {
		m.finishMountWithError(req.SandboxVolumeID, mountPoint, err)
		return nil, err
	}

	if !reserved {
		m.mu.Lock()
		if err := m.reserveMountLocked(req.SandboxVolumeID, mountPoint); err != nil {
			m.mu.Unlock()
			return nil, err
		}
		m.mu.Unlock()
	}

	defer func() {
		if reserved {
			return
		}
		m.mu.Lock()
		delete(m.mounting, req.SandboxVolumeID)
		m.statusCond.Broadcast()
		m.mu.Unlock()
	}()

	info, err := m.mountNodeLocal(ctx, req, mountPoint)
	if err != nil {
		m.finishMountWithError(req.SandboxVolumeID, mountPoint, err)
		return nil, err
	}

	m.mu.Lock()
	m.mounts[req.SandboxVolumeID] = info
	m.mountPoints[mountPoint] = req.SandboxVolumeID
	delete(m.mounting, req.SandboxVolumeID)
	m.mountStatus[req.SandboxVolumeID] = &MountStatus{
		SandboxVolumeID:     req.SandboxVolumeID,
		MountPoint:          mountPoint,
		State:               MountStateMounted,
		MountedAt:           info.mountedAt.Format(time.RFC3339),
		MountedDurationSecs: 0,
		MountSessionID:      info.mountSessionID,
		Backend:             info.backend,
	}
	m.statusCond.Broadcast()
	m.mu.Unlock()

	return &MountResponse{
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      mountPoint,
		MountedAt:       info.mountedAt.Format(time.RFC3339),
		MountSessionID:  info.mountSessionID,
		Backend:         info.backend,
	}, nil
}

// Unmount unmounts a sandbox volume.
func (m *Manager) Unmount(ctx context.Context, volumeID, mountSessionID string) error {
	if volumeID == "" {
		return fmt.Errorf("missing volume id")
	}
	if mountSessionID == "" {
		return fmt.Errorf("missing mount session id")
	}

	m.mu.RLock()
	info, ok := m.mounts[volumeID]
	m.mu.RUnlock()
	if !ok {
		return ErrVolumeNotMounted
	}
	if info.mountSessionID != mountSessionID {
		return ErrMountSessionNotFound
	}

	if err := m.unmountNodeLocal(ctx, info); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.mounts, volumeID)
	delete(m.mountPoints, info.mountPoint)
	delete(m.mountStatus, volumeID)
	m.statusCond.Broadcast()
	m.mu.Unlock()

	return nil
}

func (m *Manager) mountNodeLocal(ctx context.Context, req *MountRequest, mountPoint string) (*mountInfo, error) {
	m.mu.RLock()
	client := m.ctldVolumeClient
	m.mu.RUnlock()
	if client == nil {
		return nil, ErrNodeLocalMountUnavailable
	}

	resp, err := client.Attach(ctx, &ctldapi.VolumeAttachRequest{
		SandboxID:       req.SandboxID,
		TeamID:          req.TeamID,
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      mountPoint,
		CacheSize:       effectiveCacheSize(m.cfg, req.VolumeConfig),
		Prefetch:        int32(effectivePrefetch(m.cfg, req.VolumeConfig)),
		BufferSize:      effectiveBufferSize(m.cfg, req.VolumeConfig),
		Writeback:       effectiveWriteback(m.cfg, req.VolumeConfig),
	})
	if err != nil {
		return nil, fmt.Errorf("attach node-local volume through ctld: %w", err)
	}
	if strings.TrimSpace(resp.MountSessionID) == "" {
		return nil, fmt.Errorf("attach node-local volume through ctld: missing mount session id")
	}

	info := &mountInfo{
		volumeID:       req.SandboxVolumeID,
		mountPoint:     mountPoint,
		sandboxID:      req.SandboxID,
		mountedAt:      time.Now(),
		mountSessionID: strings.TrimSpace(resp.MountSessionID),
		backend:        MountBackendNodeLocal,
		attachmentID:   strings.TrimSpace(resp.AttachmentID),
	}
	return info, nil
}

func (m *Manager) unmountNodeLocal(ctx context.Context, info *mountInfo) error {
	m.mu.RLock()
	client := m.ctldVolumeClient
	m.mu.RUnlock()
	if client == nil {
		return ErrNodeLocalMountUnavailable
	}
	return client.Detach(ctx, &ctldapi.VolumeDetachRequest{
		SandboxID:       info.sandboxID,
		SandboxVolumeID: info.volumeID,
		MountPoint:      info.mountPoint,
		AttachmentID:    info.attachmentID,
		MountSessionID:  info.mountSessionID,
	})
}

// GetStatus returns mount statuses.
func (m *Manager) GetStatus() []MountStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make([]MountStatus, 0, len(m.mounts)+len(m.mountStatus))
	now := time.Now()
	for _, entry := range m.mountStatus {
		if entry == nil {
			continue
		}
		item := *entry
		if info, ok := m.mounts[item.SandboxVolumeID]; ok && info != nil {
			item.MountedDurationSecs = int64(now.Sub(info.mountedAt).Seconds())
			if item.Backend == "" {
				item.Backend = info.backend
			}
		}
		status = append(status, item)
	}
	for _, info := range m.mounts {
		if info == nil {
			continue
		}
		if _, ok := m.mountStatus[info.volumeID]; ok {
			continue
		}
		status = append(status, MountStatus{
			SandboxVolumeID:     info.volumeID,
			MountPoint:          info.mountPoint,
			State:               MountStateMounted,
			MountedAt:           info.mountedAt.Format(time.RFC3339),
			MountedDurationSecs: int64(now.Sub(info.mountedAt).Seconds()),
			MountSessionID:      info.mountSessionID,
			Backend:             info.backend,
		})
	}
	sort.Slice(status, func(i, j int) bool {
		if status[i].MountPoint == status[j].MountPoint {
			return status[i].SandboxVolumeID < status[j].SandboxVolumeID
		}
		return status[i].MountPoint < status[j].MountPoint
	})
	return status
}

func (m *Manager) runBootstrapMount(req MountRequest) {
	if _, err := m.mount(context.Background(), &req, true); err != nil {
		return
	}
}

func (m *Manager) applyIdentity(req *MountRequest) {
	if req == nil {
		return
	}

	m.mu.RLock()
	sandboxID := m.sandboxID
	teamID := m.teamID
	m.mu.RUnlock()

	if req.SandboxID == "" {
		req.SandboxID = sandboxID
	}
	if req.TeamID == "" {
		req.TeamID = teamID
	}

	m.SetIdentity(req.SandboxID, req.TeamID)
}

func (m *Manager) reserveMountLocked(volumeID, mountPoint string) error {
	if _, ok := m.mounts[volumeID]; ok {
		return ErrVolumeAlreadyMounted
	}
	if _, ok := m.mounting[volumeID]; ok {
		return ErrVolumeMountInProgress
	}
	if existing, ok := m.mountPoints[mountPoint]; ok && existing != volumeID {
		return ErrMountPointInUse
	}
	m.mounting[volumeID] = struct{}{}
	return nil
}

func (m *Manager) rollbackBootstrapReservationsLocked(volumeIDs []string) {
	for _, volumeID := range volumeIDs {
		delete(m.mounting, volumeID)
		delete(m.mountStatus, volumeID)
	}
	m.statusCond.Broadcast()
}

func (m *Manager) markMountState(volumeID, mountPoint, state, errorCode, errorMessage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := m.mountStatus[volumeID]
	if status == nil {
		status = &MountStatus{SandboxVolumeID: volumeID, MountPoint: mountPoint}
		m.mountStatus[volumeID] = status
	}
	status.MountPoint = mountPoint
	status.State = state
	status.ErrorCode = errorCode
	status.ErrorMessage = errorMessage
	m.statusCond.Broadcast()
}

func (m *Manager) finishMountWithError(volumeID, mountPoint string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mounting, volumeID)
	if status := m.mountStatus[volumeID]; status != nil {
		status.MountPoint = mountPoint
		status.State = MountStateFailed
		status.ErrorCode = mountErrorCode(err)
		status.ErrorMessage = err.Error()
		status.MountedAt = ""
		status.MountSessionID = ""
		status.MountedDurationSecs = 0
	}
	m.statusCond.Broadcast()
}

func mountErrorCode(err error) string {
	switch err {
	case ErrVolumeAlreadyMounted:
		return "already_mounted"
	case ErrVolumeMountInProgress:
		return "mount_in_progress"
	case ErrMountPointInUse:
		return "mount_point_in_use"
	case ErrInvalidMountPoint:
		return "invalid_mount_point"
	default:
		return "mount_failed"
	}
}

func (m *Manager) waitForMounts(ctx context.Context, reqs []MountRequest, waitTimeout time.Duration) []MountStatus {
	waitCtx := ctx
	cancel := func() {}
	if waitTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, waitTimeout)
	}
	defer cancel()

	var timer *time.Timer
	if deadline, ok := waitCtx.Deadline(); ok {
		timer = time.AfterFunc(time.Until(deadline), func() {
			m.mu.Lock()
			m.statusCond.Broadcast()
			m.mu.Unlock()
		})
		defer timer.Stop()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for !m.mountsTerminalLocked(reqs) {
		if waitCtx.Err() != nil {
			break
		}
		m.statusCond.Wait()
	}
	return m.snapshotStatusesLocked(reqs)
}

func (m *Manager) mountsTerminalLocked(reqs []MountRequest) bool {
	for _, req := range reqs {
		status := m.mountStatus[req.SandboxVolumeID]
		if status == nil {
			return false
		}
		if status.State != MountStateMounted && status.State != MountStateFailed {
			return false
		}
	}
	return true
}

func (m *Manager) snapshotStatuses(reqs []MountRequest) []MountStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotStatusesLocked(reqs)
}

func (m *Manager) snapshotStatusesLocked(reqs []MountRequest) []MountStatus {
	status := make([]MountStatus, 0, len(reqs))
	now := time.Now()
	for _, req := range reqs {
		entry := m.mountStatus[req.SandboxVolumeID]
		if entry == nil {
			continue
		}
		item := *entry
		if info, ok := m.mounts[item.SandboxVolumeID]; ok && info != nil {
			item.MountedDurationSecs = int64(now.Sub(info.mountedAt).Seconds())
			if item.Backend == "" {
				item.Backend = info.backend
			}
		}
		status = append(status, item)
	}
	return status
}

// Cleanup unmounts all volumes.
func (m *Manager) Cleanup() {
	m.mu.RLock()
	volumes := make([]*mountInfo, 0, len(m.mounts))
	for _, info := range m.mounts {
		volumes = append(volumes, info)
	}
	m.mu.RUnlock()

	for _, info := range volumes {
		if err := m.Unmount(context.Background(), info.volumeID, info.mountSessionID); err != nil {
			if m.logger != nil {
				m.logger.Warn("Failed to unmount volume during cleanup",
					zap.String("volume_id", info.volumeID),
					zap.Error(err),
				)
			}
		}
	}
}

func (m *Manager) validateMountPoint(path string) error {
	if path == "" {
		return ErrInvalidMountPoint
	}
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) || clean == string(filepath.Separator) {
		return ErrInvalidMountPoint
	}
	if strings.Contains(clean, "..") {
		return ErrInvalidMountPoint
	}
	return nil
}

func (m *Manager) ensureMountPoint(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return ErrInvalidMountPoint
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("stat mount point: %w", err)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}
	return nil
}

func effectiveCacheSize(cfg *Config, override *VolumeConfig) string {
	if override != nil && override.CacheSize != "" {
		return override.CacheSize
	}
	if cfg != nil {
		return cfg.JuiceFSCacheSize
	}
	return ""
}

func effectivePrefetch(cfg *Config, override *VolumeConfig) int {
	if override != nil && override.Prefetch != nil {
		return int(*override.Prefetch)
	}
	if cfg != nil {
		return cfg.JuiceFSPrefetch
	}
	return 0
}

func effectiveBufferSize(cfg *Config, override *VolumeConfig) string {
	if override != nil && override.BufferSize != "" {
		return override.BufferSize
	}
	if cfg != nil {
		return cfg.JuiceFSBufferSize
	}
	return ""
}

func effectiveWriteback(cfg *Config, override *VolumeConfig) bool {
	if override != nil && override.Writeback != nil {
		return *override.Writeback
	}
	if cfg != nil {
		return cfg.JuiceFSWriteback
	}
	return false
}
