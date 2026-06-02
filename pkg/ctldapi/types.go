package ctldapi

import "github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"

// SandboxResourceUsage is the whole-sandbox usage view returned by ctld.
type SandboxResourceUsage struct {
	ContainerMemoryUsage      int64 `json:"container_memory_usage"`
	ContainerMemoryLimit      int64 `json:"container_memory_limit"`
	ContainerMemoryWorkingSet int64 `json:"container_memory_working_set"`
	TotalMemoryRSS            int64 `json:"total_memory_rss"`
	TotalMemoryVMS            int64 `json:"total_memory_vms"`
	TotalOpenFiles            int   `json:"total_open_files"`
	TotalThreadCount          int   `json:"total_thread_count"`
	TotalIOReadBytes          int64 `json:"total_io_read_bytes"`
	TotalIOWriteBytes         int64 `json:"total_io_write_bytes"`
	ContextCount              int   `json:"context_count"`
	RunningContextCount       int   `json:"running_context_count"`
	PausedContextCount        int   `json:"paused_context_count"`
}

// PauseResponse is returned by ctld pause endpoints.
type PauseResponse struct {
	Paused        bool                  `json:"paused"`
	Error         string                `json:"error,omitempty"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
}

// ResumeResponse is returned by ctld resume endpoints.
type ResumeResponse struct {
	Resumed bool   `json:"resumed"`
	Error   string `json:"error,omitempty"`
}

type ProbeResponse = sandboxprobe.Response

// BindVolumePortalRequest binds one pre-published pod portal to a concrete
// sandbox volume at claim time.
type BindVolumePortalRequest struct {
	Namespace               string `json:"namespace"`
	PodName                 string `json:"pod_name"`
	PodUID                  string `json:"pod_uid"`
	PortalName              string `json:"portal_name,omitempty"`
	MountPath               string `json:"mount_path"`
	SandboxID               string `json:"sandbox_id"`
	TeamID                  string `json:"team_id"`
	SandboxVolumeID         string `json:"sandboxvolume_id"`
	TransferSourceClusterID string `json:"transfer_source_cluster_id,omitempty"`
	TransferSourcePodID     string `json:"transfer_source_pod_id,omitempty"`
}

// BindVolumePortalResponse describes the node-local mount session created by ctld.
type BindVolumePortalResponse struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
	MountedAt       string `json:"mounted_at"`
	Error           string `json:"error,omitempty"`
}

// UnbindVolumePortalRequest releases a bound portal and flushes local state.
type UnbindVolumePortalRequest struct {
	Namespace       string `json:"namespace"`
	PodName         string `json:"pod_name"`
	PodUID          string `json:"pod_uid"`
	PortalName      string `json:"portal_name,omitempty"`
	MountPath       string `json:"mount_path"`
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type UnbindVolumePortalResponse struct {
	Unbound bool   `json:"unbound"`
	Error   string `json:"error,omitempty"`
}

// CheckVolumePortalsRequest checks that pod-local portal mounts have been
// published by kubelet before the sandbox is considered claim-ready.
type CheckVolumePortalsRequest struct {
	PodUID  string            `json:"pod_uid"`
	Portals []VolumePortalRef `json:"portals,omitempty"`
}

type VolumePortalRef struct {
	PortalName string `json:"portal_name,omitempty"`
	MountPath  string `json:"mount_path,omitempty"`
}

type CheckVolumePortalsResponse struct {
	Ready   bool     `json:"ready"`
	Missing []string `json:"missing,omitempty"`
	Error   string   `json:"error,omitempty"`
}

type PrepareVolumePortalHandoffRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type PrepareVolumePortalHandoffResponse struct {
	Prepared bool   `json:"prepared"`
	Error    string `json:"error,omitempty"`
}

type CompleteVolumePortalHandoffRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type CompleteVolumePortalHandoffResponse struct {
	Completed bool   `json:"completed"`
	Error     string `json:"error,omitempty"`
}

type AbortVolumePortalHandoffRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type AbortVolumePortalHandoffResponse struct {
	Aborted bool   `json:"aborted"`
	Error   string `json:"error,omitempty"`
}

type PrepareVolumeSnapshotCheckpointRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type PrepareVolumeSnapshotCheckpointResponse struct {
	Prepared bool   `json:"prepared"`
	Error    string `json:"error,omitempty"`
}

type CompleteVolumeSnapshotCheckpointRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type CompleteVolumeSnapshotCheckpointResponse struct {
	Completed bool   `json:"completed"`
	Error     string `json:"error,omitempty"`
}

type AbortVolumeSnapshotCheckpointRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type AbortVolumeSnapshotCheckpointResponse struct {
	Aborted bool   `json:"aborted"`
	Error   string `json:"error,omitempty"`
}

type AttachVolumeOwnerRequest struct {
	TeamID          string `json:"team_id"`
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type AttachVolumeOwnerResponse struct {
	Attached bool   `json:"attached"`
	Error    string `json:"error,omitempty"`
}

type ReleaseVolumeOwnerRequest struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
}

type ReleaseVolumeOwnerResponse struct {
	Released bool   `json:"released"`
	Busy     bool   `json:"busy,omitempty"`
	Error    string `json:"error,omitempty"`
}

// PrepareRootFSRequest prepares a node-local s0fs mount that can be used as an
// overlayfs upperdir/workdir pair for a sandbox writable rootfs.
type PrepareRootFSRequest struct {
	SandboxID      string `json:"sandbox_id"`
	TeamID         string `json:"team_id"`
	RootFSVolumeID string `json:"rootfs_volume_id"`
}

type PrepareRootFSResponse struct {
	Prepared       bool   `json:"prepared"`
	SandboxID      string `json:"sandbox_id,omitempty"`
	RootFSVolumeID string `json:"rootfs_volume_id,omitempty"`
	MountPoint     string `json:"mount_point,omitempty"`
	UpperDir       string `json:"upper_dir,omitempty"`
	WorkDir        string `json:"work_dir,omitempty"`
	MountedAt      string `json:"mounted_at,omitempty"`
	Error          string `json:"error,omitempty"`
}

type CheckpointRootFSRequest struct {
	SandboxID string `json:"sandbox_id"`
}

type CheckpointRootFSResponse struct {
	Checkpointed bool   `json:"checkpointed"`
	Error        string `json:"error,omitempty"`
}

type ReleaseRootFSRequest struct {
	SandboxID string `json:"sandbox_id"`
}

type ReleaseRootFSResponse struct {
	Released bool   `json:"released"`
	Error    string `json:"error,omitempty"`
}
