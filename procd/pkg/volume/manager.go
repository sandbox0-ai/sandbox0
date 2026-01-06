package volume

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// Manager manages SandboxVolume mounts.
type Manager struct {
	mu     sync.RWMutex
	mounts map[string]*MountContext

	config *Config
	logger *zap.Logger
}

// NewManager creates a new SandboxVolume manager.
func NewManager(config *Config, logger *zap.Logger) *Manager {
	return &Manager{
		mounts: make(map[string]*MountContext),
		config: config,
		logger: logger,
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

	// NOTE: In production, this would:
	// 1. Create gRPC connection with packet marking (SO_MARK=0x2)
	// 2. Create RemoteFS FUSE filesystem
	// 3. Mount FUSE at the mount point
	// 4. Start FUSE server goroutine

	/*
	   Production code would look like:

	   // Create gRPC connection with packet marking
	   conn, err := m.createGRPCConnection(proxyAddr)
	   if err != nil {
	       return nil, fmt.Errorf("create grpc connection: %w", err)
	   }

	   client := fs.NewFileSystemClient(conn)

	   // Create RemoteFS
	   remoteFS := &RemoteFS{
	       client:          client,
	       sandboxvolumeID: req.SandboxVolumeID,
	       token:           req.Token,
	       rootInode:       "1",
	   }

	   // Mount FUSE
	   fuseConn, err := fuse.Mount(req.MountPoint,
	       fuse.FSName("sandbox0"),
	       fuse.Subtype("remote"),
	       fuse.LocalVolume(),
	       fuse.AllowOther(),
	   )
	   if err != nil {
	       return nil, fmt.Errorf("fuse mount: %w", err)
	   }

	   // Start FUSE server
	   go fs.Serve(fuseConn, remoteFS)
	*/

	// Store mount context
	mountCtx := &MountContext{
		SandboxVolumeID: req.SandboxVolumeID,
		MountPoint:      req.MountPoint,
		Token:           req.Token,
		fuseConnected:   true,
		grpcConnected:   true,
		MountedAt:       time.Now(),
	}

	m.mounts[req.SandboxVolumeID] = mountCtx

	m.logger.Info("Mounted SandboxVolume",
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

	// NOTE: In production, this would:
	// 1. Cancel FUSE server goroutine
	// 2. Close FUSE connection
	// 3. Unmount filesystem
	// 4. Close gRPC connection

	/*
	   Production code:

	   mountCtx.FuseServerCancel()

	   if err := mountCtx.FuseConn.Close(); err != nil {
	       m.logger.Warn("Failed to close fuse conn", zap.Error(err))
	   }

	   if err := syscall.Unmount(mountCtx.MountPoint, 0); err != nil {
	       return fmt.Errorf("unmount: %w", err)
	   }
	*/

	// Try to unmount (may fail if not actually mounted)
	_ = syscall.Unmount(mountCtx.MountPoint, 0)

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

	for id, mountCtx := range m.mounts {
		_ = syscall.Unmount(mountCtx.MountPoint, 0)
		delete(m.mounts, id)
	}

	m.logger.Info("All volumes unmounted")
}

/*
// createGRPCConnection creates a gRPC connection with packet marking.
// This allows the connection to bypass nftables rules for storage traffic.
func (m *Manager) createGRPCConnection(proxyAddr string) (*grpc.ClientConn, error) {
	// Custom dialer that sets SO_MARK socket option
	dialer := &net.Dialer{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				// Set SO_MARK = 0x2 to bypass nftables rules
				// 0x24 is SO_MARK on Linux
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x24, 0x2)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	return grpc.Dial(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", addr)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB max message size
		),
	)
}
*/
