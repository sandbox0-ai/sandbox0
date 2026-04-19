package volume

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
)

// Config holds configuration for the volume manager.
type Config struct {
	ProxyBaseURL   string
	ProxyPort      int
	CacheMaxBytes  int64
	CacheTTL       time.Duration
	GRPCMaxMsgSize int

	// Default S0FS cache config for mounted volumes.
	VolumeCacheSize  string
	VolumePrefetch   int
	VolumeBufferSize string
	VolumeWriteback  bool
}

// VolumeConfig holds the config for a single mount request.
type VolumeConfig struct {
	CacheSize  string `json:"cache_size,omitempty"`
	Prefetch   *int32 `json:"prefetch,omitempty"`
	BufferSize string `json:"buffer_size,omitempty"`
	Writeback  *bool  `json:"writeback,omitempty"`
}

// MountRequest represents a request to mount a sandbox volume.
type MountRequest struct {
	SandboxVolumeID string        `json:"sandboxvolume_id"`
	SandboxID       string        `json:"sandbox_id,omitempty"`
	MountPoint      string        `json:"mount_point"`
	VolumeConfig    *VolumeConfig `json:"volume_config,omitempty"`
}

// MountResponse represents a mount response.
type MountResponse struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
	MountedAt       string `json:"mounted_at"`
	MountSessionID  string `json:"mount_session_id"`
}

// UnmountRequest represents a request to unmount a sandbox volume.
type UnmountRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountSessionID  string `json:"mount_session_id"`
}

// MountStatus represents the current status of a mount.
type MountStatus struct {
	SandboxVolumeID     string `json:"sandboxvolume_id"`
	MountPoint          string `json:"mount_point"`
	State               string `json:"state"`
	MountedAt           string `json:"mounted_at"`
	MountedDurationSecs int64  `json:"mounted_duration_sec"`
	MountSessionID      string `json:"mount_session_id"`
	ErrorCode           string `json:"error_code,omitempty"`
	ErrorMessage        string `json:"error_message,omitempty"`
}

const (
	MountStatePending  = "pending"
	MountStateMounting = "mounting"
	MountStateMounted  = "mounted"
	MountStateFailed   = "failed"
)

// TokenProvider supplies the internal token for storage-proxy gRPC calls.
type TokenProvider interface {
	GetInternalToken() string
}

// EventSink receives volume watch events for file watchers.
type EventSink interface {
	Emit(event file.WatchEvent)
}
