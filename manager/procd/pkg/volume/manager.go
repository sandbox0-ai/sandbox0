package volume

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

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
		// Use default config if not provided
		volumeConfig = &VolumeConfig{
			CacheSize:  "100",
			Prefetch:   3,
			BufferSize: "300",
			Writeback:  true,
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

	return fmt.Sprintf("storage-proxy-%d.%s:8080", replicaIndex, m.config.ProxyBaseURL)
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

		delete(m.mounts, id)
	}

	m.logger.Info("All volumes unmounted")
}

// createGRPCConnection creates a gRPC connection with packet marking and auth.
// This allows the connection to bypass nftables rules for storage traffic.
// SO_MARK=0x2 is used to mark packets for special routing that bypasses
// the sandbox's network policy restrictions.
func (m *Manager) createGRPCConnection(ctx context.Context, proxyAddr string) (*grpc.ClientConn, error) {
	// Custom dialer that sets SO_MARK socket option
	dialer := &net.Dialer{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// Set SO_MARK = 0x2 to bypass nftables rules
				// 0x24 is SO_MARK on Linux (syscall.SO_MARK)
				// This packet mark allows storage traffic to bypass
				// the sandbox's network isolation rules
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x24, 0x2)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	conn, err := grpc.DialContext(ctx, proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", addr)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB max message size
			grpc.MaxCallSendMsgSize(100*1024*1024), // 100MB max message size
		),
		grpc.WithUnaryInterceptor(m.authInterceptor()),
		grpc.WithStreamInterceptor(m.streamAuthInterceptor()),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return conn, nil
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
