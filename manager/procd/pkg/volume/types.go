// Package volume provides SandboxVolume management via FUSE and gRPC.
package volume

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrVolumeAlreadyMounted is returned when a volume is already mounted.
	ErrVolumeAlreadyMounted = errors.New("sandboxvolume already mounted")

	// ErrVolumeNotMounted is returned when a volume is not mounted.
	ErrVolumeNotMounted = errors.New("sandboxvolume not mounted")

	// ErrInvalidMountPoint is returned when the mount point is invalid.
	ErrInvalidMountPoint = errors.New("invalid mount point")

	// ErrMountTimeout is returned when mount times out.
	ErrMountTimeout = errors.New("mount timeout")

	// ErrUnmountFailed is returned when unmount fails.
	ErrUnmountFailed = errors.New("unmount failed")

	// ErrConnectionFailed is returned when gRPC connection fails.
	ErrConnectionFailed = errors.New("grpc connection failed")
)

// MountRequest represents a request to mount a SandboxVolume.
type MountRequest struct {
	SandboxVolumeID string        `json:"sandboxvolume_id"`
	SandboxID       string        `json:"sandbox_id"`
	MountPoint      string        `json:"mount_point"`
	VolumeConfig    *VolumeConfig `json:"volume_config,omitempty"`
}

// VolumeConfig represents JuiceFS volume configuration.
type VolumeConfig struct {
	CacheSize  string `json:"cache_size"`
	Prefetch   int32  `json:"prefetch"`
	BufferSize string `json:"buffer_size"`
	Writeback  bool   `json:"writeback"`
	ReadOnly   bool   `json:"read_only"`
}

// MountResponse represents the response for a mount request.
type MountResponse struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
	MountedAt       string `json:"mounted_at"`
}

// UnmountRequest represents a request to unmount a SandboxVolume.
type UnmountRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

// MountStatus represents the status of a mount.
type MountStatus struct {
	SandboxVolumeID    string `json:"sandboxvolume_id"`
	MountPoint         string `json:"mount_point"`
	MountedAt          string `json:"mounted_at"`
	MountedDurationSec int64  `json:"mounted_duration_sec"`
}

// MountContext represents an active mount.
type MountContext struct {
	SandboxVolumeID string
	MountPoint      string
	SandboxID       string

	// gRPC connection and client
	GrpcConn   any // *grpc.ClientConn
	GrpcClient any // pb.FileSystemClient

	// FUSE server
	FuseServer any // *fuse.Server

	// Remote watch cancellation
	WatchCancel func()

	MountedAt time.Time

	mu sync.RWMutex
}

// Config holds SandboxVolume manager configuration.
type Config struct {
	ProxyBaseURL  string
	ProxyPort     int
	ProxyReplicas int
	NodeName      string
	CacheMaxBytes int64
	CacheTTL      time.Duration

	// JuiceFS defaults
	JuiceFSCacheSize  string
	JuiceFSPrefetch   int
	JuiceFSBufferSize string
	JuiceFSWriteback  bool

	// gRPC settings
	GRPCMaxMsgSize int
	SOMark         int
}
