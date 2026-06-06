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

// BindSandboxRootFSRequest asks node-local ctld to prepare the sandbox rootfs
// view for one runtime pod generation.
type BindSandboxRootFSRequest struct {
	Namespace          string `json:"namespace"`
	PodName            string `json:"pod_name"`
	PodUID             string `json:"pod_uid"`
	ContainerID        string `json:"container_id,omitempty"`
	SandboxID          string `json:"sandbox_id"`
	TeamID             string `json:"team_id"`
	FilesystemID       string `json:"filesystem_id"`
	RuntimeGeneration  int64  `json:"runtime_generation"`
	BaseImageRef       string `json:"base_image_ref,omitempty"`
	BaseImageDigest    string `json:"base_image_digest,omitempty"`
	CarrierImageDigest string `json:"carrier_image_digest,omitempty"`
	TargetPath         string `json:"target_path,omitempty"`
	TargetHostPath     string `json:"target_host_path,omitempty"`
	RootFSVolumeName   string `json:"rootfs_volume_name,omitempty"`
	BaseRootPath       string `json:"base_root_path,omitempty"`
}

// BindSandboxRootFSResponse describes the prepared rootfs mount point.
type BindSandboxRootFSResponse struct {
	FilesystemID string `json:"filesystem_id"`
	MountPoint   string `json:"mount_point"`
	MountedAt    string `json:"mounted_at"`
	Error        string `json:"error,omitempty"`
}

// FlushSandboxRootFSRequest commits mutable rootfs upperdir state before clean.
type FlushSandboxRootFSRequest struct {
	Namespace         string `json:"namespace"`
	PodName           string `json:"pod_name"`
	PodUID            string `json:"pod_uid"`
	SandboxID         string `json:"sandbox_id"`
	TeamID            string `json:"team_id,omitempty"`
	FilesystemID      string `json:"filesystem_id"`
	RuntimeGeneration int64  `json:"runtime_generation"`
}

type FlushSandboxRootFSResponse struct {
	Flushed      bool   `json:"flushed"`
	FilesystemID string `json:"filesystem_id"`
	Error        string `json:"error,omitempty"`
}

// ReleaseSandboxRootFSRequest releases node-local rootfs state for one runtime
// generation. It is idempotent and may be called after pod deletion.
type ReleaseSandboxRootFSRequest struct {
	Namespace         string `json:"namespace"`
	PodName           string `json:"pod_name"`
	PodUID            string `json:"pod_uid"`
	SandboxID         string `json:"sandbox_id"`
	TeamID            string `json:"team_id,omitempty"`
	FilesystemID      string `json:"filesystem_id"`
	RuntimeGeneration int64  `json:"runtime_generation"`
}

type ReleaseSandboxRootFSResponse struct {
	Released     bool   `json:"released"`
	FilesystemID string `json:"filesystem_id"`
	Error        string `json:"error,omitempty"`
}
