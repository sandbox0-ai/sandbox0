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

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type mountInfo struct {
	volumeID       string
	mountPoint     string
	sandboxID      string
	mountedAt      time.Time
	mountSessionID string
	mountSecret    string
	backend        string
	attachmentID   string
	fuseServer     *fuse.Server
	fs             *grpcFS
	cancelWatch    context.CancelFunc
	watchDone      chan struct{}
	remounting     bool
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

	eventSink EventSink

	conn   *grpc.ClientConn
	client pb.FileSystemClient

	ctldVolumeClient CtldVolumeClient

	mountStorageProxyHook   func(context.Context, *MountRequest, string) (*mountInfo, error)
	unmountStorageProxyHook func(context.Context, *mountInfo) error
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

// SetEventSink sets the sink for volume watch events.
func (m *Manager) SetEventSink(sink EventSink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventSink = sink
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

	info, err := m.mountWithSelectedBackend(ctx, req, mountPoint)
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

	if info.cancelWatch != nil {
		info.cancelWatch()
		<-info.watchDone
	}

	if info.fuseServer != nil {
		if err := info.fuseServer.Unmount(); err != nil {
			if m.logger != nil {
				m.logger.Warn("Failed to unmount fuse server", zap.Error(err))
			}
		}
	}

	if err := m.unmountMountedBackend(ctx, info); err != nil {
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

func (m *Manager) mountWithSelectedBackend(ctx context.Context, req *MountRequest, mountPoint string) (*mountInfo, error) {
	if normalizeMountMode(m.mountMode()) == MountModeNodeLocal {
		info, err := m.mountNodeLocal(ctx, req, mountPoint)
		if err == nil {
			return info, nil
		}
		if !m.nodeLocalFallbackEnabled() {
			return nil, err
		}
		if m.logger != nil {
			m.logger.Warn("Node-local volume attach failed, falling back to storage-proxy FUSE",
				zap.String("volume_id", req.SandboxVolumeID),
				zap.String("mount_point", mountPoint),
				zap.Error(err),
			)
		}
	}
	return m.mountStorageProxy(ctx, req, mountPoint)
}

func (m *Manager) mountMode() string {
	if m.cfg == nil {
		return MountModeStorageProxy
	}
	return m.cfg.MountMode
}

func (m *Manager) nodeLocalFallbackEnabled() bool {
	return m.cfg != nil && m.cfg.NodeLocalFallbackToStorage
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
		watchDone:      make(chan struct{}),
	}
	close(info.watchDone)
	return info, nil
}

func (m *Manager) mountStorageProxy(ctx context.Context, req *MountRequest, mountPoint string) (*mountInfo, error) {
	if m.mountStorageProxyHook != nil {
		return m.mountStorageProxyHook(ctx, req, mountPoint)
	}

	client, err := m.getClient(ctx)
	if err != nil {
		return nil, err
	}

	volumeConfig := m.mergeVolumeConfig(req.VolumeConfig)
	mountSessionID, mountSessionSecret, err := m.mountVolumeRemote(ctx, client, req.SandboxVolumeID, volumeConfig)
	if err != nil {
		return nil, err
	}

	cacheTTL := time.Duration(0)
	if m.cfg != nil {
		cacheTTL = m.cfg.CacheTTL
	}
	fs := newGrpcFS(req.SandboxVolumeID, mountSessionID, mountSessionSecret, client, m.tokenProvider, cacheTTL, m.logger)
	server, err := m.mountFuse(fs, mountPoint)
	if err != nil {
		_ = m.unmountVolumeRemote(ctx, client, req.SandboxVolumeID, mountSessionID, mountSessionSecret)
		return nil, err
	}

	info := &mountInfo{
		volumeID:       req.SandboxVolumeID,
		mountPoint:     mountPoint,
		sandboxID:      req.SandboxID,
		mountedAt:      time.Now(),
		mountSessionID: mountSessionID,
		mountSecret:    mountSessionSecret,
		backend:        MountBackendStorageProxyFuse,
		fuseServer:     server,
		fs:             fs,
		watchDone:      make(chan struct{}),
	}
	m.startWatch(info, req)
	return info, nil
}

func (m *Manager) unmountMountedBackend(ctx context.Context, info *mountInfo) error {
	if info == nil {
		return ErrVolumeNotMounted
	}
	switch info.backend {
	case MountBackendNodeLocal:
		return m.unmountNodeLocal(ctx, info)
	default:
		return m.unmountStorageProxy(ctx, info)
	}
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

func (m *Manager) unmountStorageProxy(ctx context.Context, info *mountInfo) error {
	if m.unmountStorageProxyHook != nil {
		return m.unmountStorageProxyHook(ctx, info)
	}
	client, err := m.getClient(ctx)
	if err != nil {
		return err
	}
	return m.unmountVolumeRemote(ctx, client, info.volumeID, info.mountSessionID, info.mountSecret)
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
			m.logger.Warn("Failed to unmount volume during cleanup",
				zap.String("volume_id", info.volumeID),
				zap.Error(err),
			)
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

func (m *Manager) getClient(ctx context.Context) (pb.FileSystemClient, error) {
	m.mu.RLock()
	if m.client != nil {
		client := m.client
		m.mu.RUnlock()
		return client, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client != nil {
		return m.client, nil
	}

	if m.cfg == nil || strings.TrimSpace(m.cfg.ProxyBaseURL) == "" || m.cfg.ProxyPort <= 0 {
		return nil, ErrStorageProxyUnavailable
	}

	addr := fmt.Sprintf("%s:%d", strings.TrimSpace(m.cfg.ProxyBaseURL), m.cfg.ProxyPort)
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(m.cfg.GRPCMaxMsgSize),
			grpc.MaxCallSendMsgSize(m.cfg.GRPCMaxMsgSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial storage-proxy: %w", err)
	}

	m.conn = conn
	m.client = pb.NewFileSystemClient(conn)
	return m.client, nil
}

func (m *Manager) withToken(ctx context.Context) (context.Context, error) {
	if m.tokenProvider == nil {
		return nil, ErrMissingInternalToken
	}
	token := strings.TrimSpace(m.tokenProvider.GetInternalToken())
	if token == "" {
		return nil, ErrMissingInternalToken
	}
	return metadata.AppendToOutgoingContext(ctx, "x-internal-token", token), nil
}

func withSessionCredential(ctx context.Context, sessionID, sessionSecret string) (context.Context, error) {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(sessionSecret) == "" {
		return nil, fmt.Errorf("missing mount session credential")
	}
	return metadata.AppendToOutgoingContext(
		ctx,
		strings.ToLower(internalauth.VolumeSessionIDHeader), sessionID,
		strings.ToLower(internalauth.VolumeSessionSecretHeader), sessionSecret,
	), nil
}

func (m *Manager) mergeVolumeConfig(override *VolumeConfig) *pb.VolumeConfig {
	return &pb.VolumeConfig{
		CacheSize:  effectiveCacheSize(m.cfg, override),
		Prefetch:   int32(effectivePrefetch(m.cfg, override)),
		BufferSize: effectiveBufferSize(m.cfg, override),
		Writeback:  effectiveWriteback(m.cfg, override),
	}
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

func (m *Manager) mountVolumeRemote(ctx context.Context, client pb.FileSystemClient, volumeID string, cfg *pb.VolumeConfig) (string, string, error) {
	callCtx, err := m.withToken(ctx)
	if err != nil {
		return "", "", err
	}
	resp, err := client.MountVolume(callCtx, &pb.MountVolumeRequest{
		VolumeId: volumeID,
		Config:   cfg,
	})
	if err != nil {
		return "", "", fmt.Errorf("mount volume via storage-proxy: %w", err)
	}
	if resp == nil || resp.MountSessionId == "" {
		return "", "", fmt.Errorf("mount volume via storage-proxy: missing mount session id")
	}
	if strings.TrimSpace(resp.MountSessionSecret) == "" {
		return "", "", fmt.Errorf("mount volume via storage-proxy: missing mount session secret")
	}
	return resp.MountSessionId, resp.MountSessionSecret, nil
}

func (m *Manager) unmountVolumeRemote(ctx context.Context, client pb.FileSystemClient, volumeID, mountSessionID, mountSessionSecret string) error {
	callCtx, err := withSessionCredential(ctx, mountSessionID, mountSessionSecret)
	if err != nil {
		callCtx, err = m.withToken(ctx)
	}
	if err != nil {
		return err
	}
	_, err = client.UnmountVolume(callCtx, &pb.UnmountVolumeRequest{
		VolumeId:       volumeID,
		MountSessionId: mountSessionID,
	})
	if err != nil {
		return fmt.Errorf("unmount volume via storage-proxy: %w", err)
	}
	return nil
}

func (m *Manager) ackInvalidate(ctx context.Context, volumeID, mountSessionID, mountSessionSecret, invalidateID string, remountErr error) {
	if volumeID == "" || mountSessionID == "" || invalidateID == "" {
		return
	}
	client, err := m.getClient(ctx)
	if err != nil {
		m.logger.Warn("Failed to get storage-proxy client to ack invalidate", zap.Error(err))
		return
	}
	callCtx, err := withSessionCredential(ctx, mountSessionID, mountSessionSecret)
	if err != nil {
		callCtx, err = m.withToken(ctx)
	}
	if err != nil {
		m.logger.Warn("Failed to get internal token to ack invalidate", zap.Error(err))
		return
	}
	success := remountErr == nil
	errorMessage := ""
	if remountErr != nil {
		errorMessage = remountErr.Error()
	}
	_, err = client.AckInvalidate(callCtx, &pb.AckInvalidateRequest{
		VolumeId:       volumeID,
		MountSessionId: mountSessionID,
		InvalidateId:   invalidateID,
		Success:        success,
		ErrorMessage:   errorMessage,
	})
	if err != nil {
		m.logger.Warn("Failed to ack invalidate", zap.Error(err))
	}
}

func (m *Manager) mountFuse(fs *grpcFS, mountPoint string) (*fuse.Server, error) {
	opt := &fuse.MountOptions{
		FsName:        "sandbox0-volume",
		Name:          "sandbox0-volume",
		MaxBackground: 64,
		EnableLocks:   true,
		AllowOther:    os.Getuid() == 0,
		DirectMount:   true,
		MaxWrite:      128 * 1024,
	}

	server, err := fuse.NewServer(fs, mountPoint, opt)
	if err != nil {
		return nil, fmt.Errorf("mount fuse: %w", err)
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return nil, fmt.Errorf("wait for fuse mount: %w", err)
	}
	return server, nil
}

func (m *Manager) startWatch(info *mountInfo, req *MountRequest) {
	client := info.fs.client
	if client == nil {
		close(info.watchDone)
		return
	}

	watchReq := &pb.WatchRequest{
		VolumeId:    req.SandboxVolumeID,
		PathPrefix:  "",
		Recursive:   true,
		IncludeSelf: req.SandboxID == "",
		SandboxId:   req.SandboxID,
	}

	ctx, cancel := context.WithCancel(context.Background())
	info.cancelWatch = cancel

	go func() {
		defer close(info.watchDone)

		callCtx, err := withSessionCredential(ctx, info.mountSessionID, info.mountSecret)
		if err != nil {
			callCtx, err = m.withToken(ctx)
		}
		if err != nil {
			m.logger.Warn("Missing storage-proxy token for watch", zap.Error(err))
			return
		}
		stream, err := client.WatchVolumeEvents(callCtx, watchReq)
		if err != nil {
			m.logger.Warn("Failed to watch volume events", zap.Error(err))
			return
		}

		for {
			event, err := stream.Recv()
			if err != nil {
				return
			}
			m.emitWatchEvent(info, event)
		}
	}()
}

func (m *Manager) emitWatchEvent(info *mountInfo, event *pb.WatchEvent) {
	if event == nil {
		return
	}

	if event.EventType == pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE && (event.Path == "" || event.Path == "/") {
		go m.remountVolume(info.volumeID, info.mountSessionID, event.InvalidateId)
		return
	}

	m.mu.RLock()
	sink := m.eventSink
	m.mu.RUnlock()
	if sink == nil {
		return
	}

	eventType := mapWatchEventType(event.EventType)
	path := joinMountPath(info.mountPoint, event.Path)
	oldPath := joinMountPath(info.mountPoint, event.OldPath)
	if path == "" && oldPath == "" {
		return
	}

	sink.Emit(fileWatchEvent(eventType, path, oldPath))
}

func joinMountPath(mountPoint, path string) string {
	if path == "" {
		return ""
	}
	trimmed := strings.TrimPrefix(path, "/")
	return filepath.Join(mountPoint, trimmed)
}

func (m *Manager) remountVolume(volumeID, mountSessionID, invalidateID string) {
	m.mu.Lock()
	info, ok := m.mounts[volumeID]
	if !ok || info == nil || info.remounting {
		m.mu.Unlock()
		return
	}
	info.remounting = true
	mountPoint := info.mountPoint
	m.mu.Unlock()
	remountErr := error(nil)

	defer func() {
		m.mu.Lock()
		if info, ok := m.mounts[volumeID]; ok && info != nil {
			info.remounting = false
		}
		m.mu.Unlock()
		if invalidateID != "" {
			m.ackInvalidate(context.Background(), volumeID, mountSessionID, info.mountSecret, invalidateID, remountErr)
		}
	}()

	m.logger.Info("Remounting volume to invalidate cache",
		zap.String("volume_id", volumeID),
		zap.String("mount_point", mountPoint),
	)

	if info.fuseServer != nil {
		if err := info.fuseServer.Unmount(); err != nil {
			m.logger.Warn("Failed to unmount fuse server during remount", zap.Error(err))
		}
	}

	client, err := m.getClient(context.Background())
	if err != nil {
		remountErr = err
		m.logger.Warn("Failed to get storage-proxy client during remount", zap.Error(err))
		return
	}

	fs := newGrpcFS(volumeID, info.mountSessionID, info.mountSecret, client, m.tokenProvider, m.cfg.CacheTTL, m.logger)
	server, err := m.mountFuse(fs, mountPoint)
	if err != nil {
		remountErr = err
		m.logger.Warn("Failed to remount fuse server", zap.Error(err))
		return
	}

	m.mu.Lock()
	if info, ok := m.mounts[volumeID]; ok && info != nil {
		info.fuseServer = server
		info.fs = fs
		info.mountedAt = time.Now()
	}
	m.mu.Unlock()
}
