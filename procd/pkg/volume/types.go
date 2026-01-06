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
	SandboxVolumeID string `json:"sandboxvolume_id"`
	SandboxID       string `json:"sandbox_id"`
	MountPoint      string `json:"mount_point"`
	Token           string `json:"token"`
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
	Token           string

	// FUSE connection (would be *fuse.Conn in real implementation)
	fuseConnected bool

	// gRPC client (would be fs.FileSystemClient in real implementation)
	grpcConnected bool

	MountedAt time.Time

	mu sync.RWMutex
}

// Config holds SandboxVolume manager configuration.
type Config struct {
	ProxyBaseURL  string
	ProxyReplicas int
	NodeName      string
	CacheMaxBytes int64
	CacheTTL      time.Duration
}
