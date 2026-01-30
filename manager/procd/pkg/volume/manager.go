package volume

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/file"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/sandbox0-ai/infra/storage-proxy/proto/fs"
)

// TokenProvider provides internal token for gRPC authentication.
type TokenProvider interface {
	GetInternalToken() string
}

// Manager manages SandboxVolume mounts.
type Manager struct {
	mu     sync.RWMutex
	mounts map[string]*MountContext

	config        *Config
	logger        *zap.Logger
	tokenProvider TokenProvider
	eventSink     EventSink
}

// NewManager creates a new SandboxVolume manager.
func NewManager(config *Config, tokenProvider TokenProvider, logger *zap.Logger) *Manager {
	return &Manager{
		mounts:        make(map[string]*MountContext),
		config:        config,
		tokenProvider: tokenProvider,
		logger:        logger,
	}
}

// EventSink receives translated watch events for mounted volumes.
type EventSink interface {
	Emit(event file.WatchEvent)
}

// SetEventSink sets the event sink for watch events.
func (m *Manager) SetEventSink(sink EventSink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventSink = sink
}

// Mount mounts a SandboxVolume.
func (m *Manager) Mount(ctx context.Context, req *MountRequest) (*MountResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already mounted
	if _, exists := m.mounts[req.SandboxVolumeID]; exists {
		return nil, ErrVolumeAlreadyMounted
	}

	// Validate mount point
	if req.MountPoint == "" {
		return nil, ErrInvalidMountPoint
	}

	// Ensure mount point directory exists
	if err := os.MkdirAll(req.MountPoint, 0755); err != nil {
		return nil, fmt.Errorf("create mount point: %w", err)
	}

	// Get preferred Storage Proxy address using node affinity
	proxyAddr := m.getStorageProxyAddress()

	m.logger.Info("Creating gRPC connection to Storage Proxy",
		zap.String("sandboxvolume_id", req.SandboxVolumeID),
		zap.String("proxy_address", proxyAddr),
	)

	// Create gRPC connection with packet marking (SO_MARK=0x2)
	// This allows storage traffic to bypass nftables network policy rules
	conn, err := m.createGRPCConnection(ctx, proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("create grpc connection: %w", err)
	}

	// Create FileSystem client
	client := pb.NewFileSystemClient(conn)

	// Call storage-proxy to mount the volume in JuiceFS
	// This initializes the JuiceFS mount on the storage-proxy side
	volumeConfig := req.VolumeConfig
	if volumeConfig == nil {
		// Use default config from manager if not provided
		volumeConfig = &VolumeConfig{
			CacheSize:  m.config.JuiceFSCacheSize,
			Prefetch:   int32(m.config.JuiceFSPrefetch),
			BufferSize: m.config.JuiceFSBufferSize,
			Writeback:  m.config.JuiceFSWriteback,
			ReadOnly:   false,
		}
	}

	mountReq := &pb.MountVolumeRequest{
		VolumeId: req.SandboxVolumeID,
		Config: &pb.VolumeConfig{
			CacheSize:  volumeConfig.CacheSize,
			Prefetch:   volumeConfig.Prefetch,
			BufferSize: volumeConfig.BufferSize,
			Writeback:  volumeConfig.Writeback,
			ReadOnly:   volumeConfig.ReadOnly,
		},
	}

	mountResp, err := client.MountVolume(ctx, mountReq)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("call storage-proxy MountVolume: %w", err)
	}

	m.logger.Info("Storage Proxy mounted volume",
		zap.String("sandboxvolume_id", req.SandboxVolumeID),
		zap.Int64("mounted_at", mountResp.MountedAt),
	)

	// Cleanup any stale FUSE mounts
	CleanupStaleMounts(req.MountPoint, m.logger)

	// Create RemoteFS and mount FUSE
	remoteFS := NewRemoteFS(client, req.SandboxVolumeID, m.logger)
	fuseServer, err := MountFUSE(req.MountPoint, remoteFS, m.logger)
	if err != nil {
		// Cleanup: unmount from storage-proxy and close connection
		unmountReq := &pb.UnmountVolumeRequest{VolumeId: req.SandboxVolumeID}
		_, _ = client.UnmountVolume(ctx, unmountReq)
		conn.Close()
		return nil, fmt.Errorf("mount fuse: %w", err)
	}

	// Start FUSE server in background
	go func() {
		fuseServer.Wait()
		m.logger.Info("FUSE server stopped",
			zap.String("sandboxvolume_id", req.SandboxVolumeID),
		)
	}()

	// Store mount context
	mountCtx := &MountContext{
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      req.MountPoint,
		SandboxID:       req.SandboxID,
		GrpcConn:        conn,
		GrpcClient:      client,
		FuseServer:      fuseServer,
		MountedAt:       time.Now(),
	}

	m.mounts[req.SandboxVolumeID] = mountCtx

	m.logger.Info("Mounted SandboxVolume with FUSE",
		zap.String("sandboxvolume_id", req.SandboxVolumeID),
		zap.String("mount_point", req.MountPoint),
	)

	m.startRemoteWatch(mountCtx)

	return &MountResponse{
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      req.MountPoint,
		MountedAt:       time.Now().Format(time.RFC3339),
	}, nil
}

// Unmount unmounts a SandboxVolume.
func (m *Manager) Unmount(ctx context.Context, sandboxvolumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mountCtx, exists := m.mounts[sandboxvolumeID]
	if !exists {
		return ErrVolumeNotMounted
	}

	// Unmount FUSE first
	if mountCtx.FuseServer != nil {
		if err := UnmountFUSE(mountCtx.FuseServer.(*fuse.Server), mountCtx.MountPoint); err != nil {
			m.logger.Warn("Failed to unmount FUSE",
				zap.String("sandboxvolume_id", sandboxvolumeID),
				zap.Error(err),
			)
		}
	}

	// Call storage-proxy to unmount the volume
	if mountCtx.GrpcClient != nil {
		client := mountCtx.GrpcClient.(pb.FileSystemClient)
		unmountReq := &pb.UnmountVolumeRequest{
			VolumeId: sandboxvolumeID,
		}

		_, err := client.UnmountVolume(ctx, unmountReq)
		if err != nil {
			m.logger.Warn("Failed to unmount volume on storage-proxy",
				zap.String("sandboxvolume_id", sandboxvolumeID),
				zap.Error(err),
			)
			// Continue with cleanup even if storage-proxy unmount fails
		} else {
			m.logger.Info("Storage Proxy unmounted volume",
				zap.String("sandboxvolume_id", sandboxvolumeID),
			)
		}
	}

	if mountCtx.WatchCancel != nil {
		mountCtx.WatchCancel()
	}

	// Close gRPC connection
	if mountCtx.GrpcConn != nil {
		if conn, ok := mountCtx.GrpcConn.(*grpc.ClientConn); ok {
			if err := conn.Close(); err != nil {
				m.logger.Warn("Failed to close gRPC connection",
					zap.String("sandboxvolume_id", sandboxvolumeID),
					zap.Error(err),
				)
			}
		}
	}

	delete(m.mounts, sandboxvolumeID)

	m.logger.Info("Unmounted SandboxVolume",
		zap.String("sandboxvolume_id", sandboxvolumeID),
	)

	return nil
}

// GetStatus returns the status of all mounts.
func (m *Manager) GetStatus() []MountStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []MountStatus
	for _, mountCtx := range m.mounts {
		result = append(result, MountStatus{
			SandboxVolumeID:    mountCtx.SandboxVolumeID,
			MountPoint:         mountCtx.MountPoint,
			MountedAt:          mountCtx.MountedAt.Format(time.RFC3339),
			MountedDurationSec: int64(time.Since(mountCtx.MountedAt).Seconds()),
		})
	}

	return result
}

// IsMounted checks if a volume is mounted.
func (m *Manager) IsMounted(sandboxvolumeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.mounts[sandboxvolumeID]
	return exists
}

// getStorageProxyAddress returns the preferred Storage Proxy address using node affinity.
func (m *Manager) getStorageProxyAddress() string {
	// Hash-based routing: same node -> same replica
	hash := fnv.New32()
	hash.Write([]byte(m.config.NodeName))
	replicaIndex := hash.Sum32() % uint32(m.config.ProxyReplicas)

	return fmt.Sprintf("storage-proxy-%d.%s:%d", replicaIndex, m.config.ProxyBaseURL, m.config.ProxyPort)
}

// Cleanup unmounts all volumes.
func (m *Manager) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx := context.Background()

	for id, mountCtx := range m.mounts {
		// Unmount FUSE first
		if mountCtx.FuseServer != nil {
			_ = UnmountFUSE(mountCtx.FuseServer.(*fuse.Server), mountCtx.MountPoint)
		}

		// Call storage-proxy to unmount
		if mountCtx.GrpcClient != nil {
			client := mountCtx.GrpcClient.(pb.FileSystemClient)
			unmountReq := &pb.UnmountVolumeRequest{
				VolumeId: id,
			}
			_, _ = client.UnmountVolume(ctx, unmountReq)
		}

		// Close gRPC connection
		if mountCtx.GrpcConn != nil {
			if conn, ok := mountCtx.GrpcConn.(*grpc.ClientConn); ok {
				_ = conn.Close()
			}
		}

		if mountCtx.WatchCancel != nil {
			mountCtx.WatchCancel()
		}

		delete(m.mounts, id)
	}

	m.logger.Info("All volumes unmounted")
}

// createGRPCConnection creates a gRPC connection with packet marking and auth.
// This allows the connection to bypass nftables rules for storage traffic.
// SO_MARK is used to mark packets for special routing that bypasses
// the sandbox's network policy restrictions.
func (m *Manager) createGRPCConnection(ctx context.Context, proxyAddr string) (*grpc.ClientConn, error) {
	soMark := m.config.SOMark
	if soMark == 0 {
		soMark = 0x2 // fallback to default if 0
	}

	// Custom dialer that sets SO_MARK socket option
	dialer := &net.Dialer{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// Set SO_MARK to bypass nftables rules
				// 0x24 is SO_MARK on Linux (syscall.SO_MARK)
				// This packet mark allows storage traffic to bypass
				// the sandbox's network isolation rules
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x24, soMark)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	maxMsgSize := m.config.GRPCMaxMsgSize
	if maxMsgSize == 0 {
		maxMsgSize = 100 * 1024 * 1024 // 100MB default
	}

	conn, err := grpc.DialContext(ctx, proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", addr)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
		grpc.WithUnaryInterceptor(m.authInterceptor()),
		grpc.WithStreamInterceptor(m.streamAuthInterceptor()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return conn, nil
}

func (m *Manager) startRemoteWatch(mountCtx *MountContext) {
	m.mu.RLock()
	sink := m.eventSink
	m.mu.RUnlock()
	if sink == nil || mountCtx == nil || mountCtx.GrpcClient == nil {
		return
	}
	client, ok := mountCtx.GrpcClient.(pb.FileSystemClient)
	if !ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	mountCtx.WatchCancel = cancel

	req := &pb.WatchRequest{
		VolumeId:    mountCtx.SandboxVolumeID,
		PathPrefix:  "/",
		Recursive:   true,
		IncludeSelf: true,
		SandboxId:   mountCtx.SandboxID,
	}

	go func() {
		stream, err := client.WatchVolumeEvents(ctx, req)
		if err != nil {
			m.logger.Warn("Failed to start watch stream",
				zap.String("sandboxvolume_id", mountCtx.SandboxVolumeID),
				zap.Error(err),
			)
			return
		}

		for {
			event, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
					return
				}
				m.logger.Warn("Watch stream error",
					zap.String("sandboxvolume_id", mountCtx.SandboxVolumeID),
					zap.Error(err),
				)
				return
			}
			sink.Emit(translateWatchEvent(mountCtx, event))
		}
	}()
}

func translateWatchEvent(mountCtx *MountContext, event *pb.WatchEvent) file.WatchEvent {
	return file.WatchEvent{
		Type:    mapWatchEventType(event.GetEventType()),
		Path:    mapWatchEventPath(mountCtx.MountPoint, event.GetPath()),
		OldPath: mapWatchEventPath(mountCtx.MountPoint, event.GetOldPath()),
	}
}

func mapWatchEventType(eventType pb.WatchEventType) file.EventType {
	switch eventType {
	case pb.WatchEventType_WATCH_EVENT_TYPE_CREATE:
		return file.EventCreate
	case pb.WatchEventType_WATCH_EVENT_TYPE_WRITE:
		return file.EventWrite
	case pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE:
		return file.EventRemove
	case pb.WatchEventType_WATCH_EVENT_TYPE_RENAME:
		return file.EventRename
	case pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD:
		return file.EventChmod
	case pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE:
		return file.EventInvalidate
	default:
		return file.EventInvalidate
	}
}

func mapWatchEventPath(mountPoint, eventPath string) string {
	if mountPoint == "" {
		return eventPath
	}
	if eventPath == "" || eventPath == "/" {
		return mountPoint
	}
	trimmed := strings.TrimPrefix(eventPath, "/")
	return filepath.Join(mountPoint, trimmed)
}

// authInterceptor returns a unary interceptor that adds auth token to requests.
func (m *Manager) authInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = m.addAuthMetadata(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamAuthInterceptor returns a stream interceptor that adds auth token to requests.
func (m *Manager) streamAuthInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = m.addAuthMetadata(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// addAuthMetadata adds authentication token to context metadata.
func (m *Manager) addAuthMetadata(ctx context.Context) context.Context {
	if m.tokenProvider == nil {
		return ctx
	}

	token := m.tokenProvider.GetInternalToken()
	if token == "" {
		return ctx
	}

	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(ctx, md)
}
