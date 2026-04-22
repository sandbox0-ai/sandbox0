package db

import (
	"encoding/json"
	"time"
)

// SandboxVolume represents a sandbox volume metadata stored in the database
type SandboxVolume struct {
	ID     string `json:"id"`
	TeamID string `json:"team_id"`
	UserID string `json:"user_id"`
	// SourceVolumeID references the volume this one was forked from.
	SourceVolumeID *string `json:"source_volume_id,omitempty"`
	// Default external POSIX identity used for HTTP/sync paths that do not carry actor identity.
	DefaultPosixUID *int64 `json:"default_posix_uid,omitempty"`
	DefaultPosixGID *int64 `json:"default_posix_gid,omitempty"`

	AccessMode string `json:"access_mode"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// S0FSCommittedHead stores the current committed immutable manifest pointer for one volume.
type S0FSCommittedHead struct {
	VolumeID      string    `json:"volume_id"`
	ManifestSeq   uint64    `json:"manifest_seq"`
	CheckpointSeq uint64    `json:"checkpoint_seq"`
	ManifestKey   string    `json:"manifest_key"`
	UpdatedAt     time.Time `json:"updated_at"`
}

const (
	SandboxVolumeOwnerKindSandbox = "sandbox"
)

// SandboxVolumeOwner stores durable lifecycle ownership for manager-created
// system volumes.
type SandboxVolumeOwner struct {
	VolumeID             string     `json:"volume_id"`
	OwnerKind            string     `json:"owner_kind"`
	OwnerSandboxID       string     `json:"owner_sandbox_id"`
	OwnerClusterID       string     `json:"owner_cluster_id"`
	Purpose              string     `json:"purpose"`
	CreatedAt            time.Time  `json:"created_at"`
	CleanupRequestedAt   *time.Time `json:"cleanup_requested_at,omitempty"`
	CleanupReason        *string    `json:"cleanup_reason,omitempty"`
	LastCleanupAttemptAt *time.Time `json:"last_cleanup_attempt_at,omitempty"`
	LastCleanupError     *string    `json:"last_cleanup_error,omitempty"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

// OwnedSandboxVolume combines volume metadata with its system ownership row.
type OwnedSandboxVolume struct {
	Volume SandboxVolume      `json:"volume"`
	Owner  SandboxVolumeOwner `json:"owner"`
}

// Snapshot represents a point-in-time copy of a SandboxVolume
type Snapshot struct {
	ID       string `json:"id"`
	VolumeID string `json:"volume_id"`
	TeamID   string `json:"team_id"`
	UserID   string `json:"user_id"`

	// S0FS metadata
	RootInode   int64 `json:"root_inode"`   // Snapshot root directory inode
	SourceInode int64 `json:"source_inode"` // Source volume root inode at snapshot time

	// Metadata
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	SizeBytes   int64  `json:"size_bytes"`

	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// VolumeMount represents a volume mount point for cross-cluster coordination
type VolumeMount struct {
	ID        string `json:"id"`
	VolumeID  string `json:"volume_id"`
	ClusterID string `json:"cluster_id"`
	PodID     string `json:"pod_id"`

	LastHeartbeat time.Time        `json:"last_heartbeat"`
	MountedAt     time.Time        `json:"mounted_at"`
	MountOptions  *json.RawMessage `json:"mount_options,omitempty"`
}

type VolumeHandoff struct {
	VolumeID        string    `json:"volume_id"`
	SourceClusterID string    `json:"source_cluster_id"`
	SourcePodID     string    `json:"source_pod_id"`
	TargetClusterID string    `json:"target_cluster_id"`
	TargetPodID     string    `json:"target_pod_id"`
	TargetCtldAddr  string    `json:"target_ctld_addr"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

// SnapshotCoordination tracks the state of a snapshot creation across clusters
type SnapshotCoordination struct {
	ID       string `json:"id"`
	VolumeID string `json:"volume_id"`

	// Will be filled after successful snapshot creation
	SnapshotID *string `json:"snapshot_id,omitempty"`

	// Coordination state
	Status         string `json:"status"` // pending, flushing, completed, failed, timeout
	ExpectedNodes  int    `json:"expected_nodes"`
	CompletedNodes int    `json:"completed_nodes"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Coordination status constants
const (
	CoordStatusPending   = "pending"
	CoordStatusFlushing  = "flushing"
	CoordStatusCompleted = "completed"
	CoordStatusFailed    = "failed"
	CoordStatusTimeout   = "timeout"
)

// FlushResponse represents a node's response to a flush request
type FlushResponse struct {
	ID        string `json:"id"`
	CoordID   string `json:"coord_id"`
	ClusterID string `json:"cluster_id"`
	PodID     string `json:"pod_id"`

	Success      bool       `json:"success"`
	FlushedAt    *time.Time `json:"flushed_at,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}
