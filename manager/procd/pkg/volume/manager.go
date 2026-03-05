package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type mountInfo struct {
	volumeID       string
	mountPoint     string
	mountedAt      time.Time
	mountSessionID string
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

	eventSink EventSink

	conn   *grpc.ClientConn
	client pb.FileSystemClient
}

// NewManager creates a new volume manager.
func NewManager(cfg *Config, tokenProvider TokenProvider, logger *zap.Logger) *Manager {
	return &Manager{
		cfg:           cfg,
		tokenProvider: tokenProvider,
		logger:        logger,
		mounts:        make(map[string]*mountInfo),
		mountPoints:   make(map[string]string),
		mounting:      make(map[string]struct{}),
	}
}

// SetEventSink sets the sink for volume watch events.
func (m *Manager) SetEventSink(sink EventSink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventSink = sink
}

// Mount mounts a sandbox volume at the specified mount point.
func (m *Manager) Mount(ctx context.Context, req *MountRequest) (*MountResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("missing request")
	}
	if err := m.validateMountPoint(req.MountPoint); err != nil {
		return nil, err
	}
	mountPoint := filepath.Clean(req.MountPoint)

	if err := m.ensureMountPoint(mountPoint); err != nil {
		return nil, err
	}

	m.mu.Lock()
	if _, ok := m.mounts[req.SandboxVolumeID]; ok {
		m.mu.Unlock()
		return nil, ErrVolumeAlreadyMounted
	}
	if _, ok := m.mounting[req.SandboxVolumeID]; ok {
		m.mu.Unlock()
		return nil, ErrVolumeMountInProgress
	}
	if existing, ok := m.mountPoints[mountPoint]; ok && existing != req.SandboxVolumeID {
		m.mu.Unlock()
		return nil, ErrMountPointInUse
	}
	m.mounting[req.SandboxVolumeID] = struct{}{}
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.mounting, req.SandboxVolumeID)
		m.mu.Unlock()
	}()

	client, err := m.getClient(ctx)
	if err != nil {
		return nil, err
	}

	volumeConfig := m.mergeVolumeConfig(req.VolumeConfig)
	mountSessionID, err := m.mountVolumeRemote(ctx, client, req.SandboxVolumeID, volumeConfig)
	if err != nil {
		return nil, err
	}

	fs := newGrpcFS(req.SandboxVolumeID, client, m.tokenProvider, m.cfg.CacheTTL, m.logger)
	server, err := m.mountFuse(fs, mountPoint)
	if err != nil {
		_ = m.unmountVolumeRemote(ctx, client, req.SandboxVolumeID, mountSessionID)
		return nil, err
	}

	info := &mountInfo{
		volumeID:       req.SandboxVolumeID,
		mountPoint:     mountPoint,
		mountedAt:      time.Now(),
		mountSessionID: mountSessionID,
		fuseServer:     server,
		fs:             fs,
		watchDone:      make(chan struct{}),
	}
	m.startWatch(info, req)

	m.mu.Lock()
	m.mounts[req.SandboxVolumeID] = info
	m.mountPoints[mountPoint] = req.SandboxVolumeID
	m.mu.Unlock()

	return &MountResponse{
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      mountPoint,
		MountedAt:       info.mountedAt.Format(time.RFC3339),
		MountSessionID:  mountSessionID,
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
			m.logger.Warn("Failed to unmount fuse server", zap.Error(err))
		}
	}

	client, err := m.getClient(ctx)
	if err != nil {
		return err
	}
	if err := m.unmountVolumeRemote(ctx, client, volumeID, info.mountSessionID); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.mounts, volumeID)
	delete(m.mountPoints, info.mountPoint)
	m.mu.Unlock()

	return nil
}

// GetStatus returns mount statuses.
func (m *Manager) GetStatus() []MountStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make([]MountStatus, 0, len(m.mounts))
	now := time.Now()
	for _, info := range m.mounts {
		status = append(status, MountStatus{
			SandboxVolumeID:     info.volumeID,
			MountPoint:          info.mountPoint,
			MountedAt:           info.mountedAt.Format(time.RFC3339),
			MountedDurationSecs: int64(now.Sub(info.mountedAt).Seconds()),
			MountSessionID:      info.mountSessionID,
		})
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

func (m *Manager) mergeVolumeConfig(override *VolumeConfig) *pb.VolumeConfig {
	cacheSize := ""
	prefetch := int32(0)
	bufferSize := ""
	writeback := false

	if m.cfg != nil {
		cacheSize = m.cfg.JuiceFSCacheSize
		prefetch = int32(m.cfg.JuiceFSPrefetch)
		bufferSize = m.cfg.JuiceFSBufferSize
		writeback = m.cfg.JuiceFSWriteback
	}

	if override != nil {
		if override.CacheSize != "" {
			cacheSize = override.CacheSize
		}
		if override.Prefetch != nil {
			prefetch = *override.Prefetch
		}
		if override.BufferSize != "" {
			bufferSize = override.BufferSize
		}
		if override.Writeback != nil {
			writeback = *override.Writeback
		}
	}

	return &pb.VolumeConfig{
		CacheSize:  cacheSize,
		Prefetch:   prefetch,
		BufferSize: bufferSize,
		Writeback:  writeback,
	}
}

func (m *Manager) mountVolumeRemote(ctx context.Context, client pb.FileSystemClient, volumeID string, cfg *pb.VolumeConfig) (string, error) {
	callCtx, err := m.withToken(ctx)
	if err != nil {
		return "", err
	}
	resp, err := client.MountVolume(callCtx, &pb.MountVolumeRequest{
		VolumeId: volumeID,
		Config:   cfg,
	})
	if err != nil {
		return "", fmt.Errorf("mount volume via storage-proxy: %w", err)
	}
	if resp == nil || resp.MountSessionId == "" {
		return "", fmt.Errorf("mount volume via storage-proxy: missing mount session id")
	}
	return resp.MountSessionId, nil
}

func (m *Manager) unmountVolumeRemote(ctx context.Context, client pb.FileSystemClient, volumeID, mountSessionID string) error {
	callCtx, err := m.withToken(ctx)
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

func (m *Manager) ackInvalidate(ctx context.Context, volumeID, mountSessionID, invalidateID string, remountErr error) {
	if volumeID == "" || mountSessionID == "" || invalidateID == "" {
		return
	}
	client, err := m.getClient(ctx)
	if err != nil {
		m.logger.Warn("Failed to get storage-proxy client to ack invalidate", zap.Error(err))
		return
	}
	callCtx, err := m.withToken(ctx)
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

		callCtx, err := m.withToken(ctx)
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
			m.ackInvalidate(context.Background(), volumeID, mountSessionID, invalidateID, remountErr)
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

	fs := newGrpcFS(volumeID, client, m.tokenProvider, m.cfg.CacheTTL, m.logger)
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
