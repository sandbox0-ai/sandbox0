package ctldapi

import (
	"encoding/json"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
)

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

// RootFSContainerRef identifies the sandbox container whose writable rootfs
// should be inspected or checkpointed by node-local ctld.
type RootFSContainerRef struct {
	Namespace     string `json:"namespace"`
	PodName       string `json:"pod_name"`
	PodUID        string `json:"pod_uid,omitempty"`
	ContainerName string `json:"container_name"`
	ContainerID   string `json:"container_id,omitempty"`
}

// RootFSInfo is the containerd metadata needed to validate and restore a
// sandbox rootfs checkpoint.
type RootFSInfo struct {
	ContainerID         string   `json:"container_id"`
	ContainerName       string   `json:"container_name"`
	PodNamespace        string   `json:"pod_namespace"`
	PodName             string   `json:"pod_name"`
	PodUID              string   `json:"pod_uid,omitempty"`
	Runtime             string   `json:"runtime,omitempty"`
	RuntimeHandler      string   `json:"runtime_handler,omitempty"`
	Snapshotter         string   `json:"snapshotter"`
	SnapshotKey         string   `json:"snapshot_key"`
	SnapshotParent      string   `json:"snapshot_parent,omitempty"`
	SnapshotParentChain []string `json:"snapshot_parent_chain,omitempty"`
	BaseImageRef        string   `json:"base_image_ref,omitempty"`
	BaseImageDigest     string   `json:"base_image_digest,omitempty"`
	// BaseImageConfig is populated only inside ctld's node-local runtime so the
	// controller can preserve OCI runtime defaults. HTTP handlers redact it.
	BaseImageConfig json.RawMessage `json:"base_image_config,omitempty"`
}

// RootFSCheckpointDescriptor is the immutable COW head sealed by ctld. Objects
// contains every CAS object produced or reused by this runtime session; parent-
// only objects remain reachable through layer ancestry.
type RootFSCheckpointDescriptor struct {
	Reference          rootfshead.HeadReference  `json:"reference"`
	Image              rootfshead.ImageReference `json:"image"`
	Objects            []rootfshead.Object       `json:"objects,omitempty"`
	CreatedBytes       int64                     `json:"created_bytes,omitempty"`
	CreatedObjectCount int64                     `json:"created_object_count,omitempty"`
	DirtyPaths         int                       `json:"dirty_paths,omitempty"`
	SealDurationMS     int64                     `json:"seal_duration_ms,omitempty"`
}

// MaterializeRootFSHeadRequest asks the selected node to reconstruct one
// digest-pinned metadata-only image directly in its containerd content store.
type MaterializeRootFSHeadRequest struct {
	Head  rootfshead.HeadReference  `json:"head"`
	Image rootfshead.ImageReference `json:"image"`
}

type MaterializeRootFSHeadResponse struct {
	Materialized bool   `json:"materialized"`
	Image        string `json:"image,omitempty"`
	Error        string `json:"error,omitempty"`
}

// RootFSPortalPath maps an unbound volume portal's visible mount path to the
// node-local backing directory that should be checkpointed as rootfs content.
type RootFSPortalPath struct {
	PortalName  string `json:"portal_name,omitempty"`
	MountPath   string `json:"mount_path"`
	BackingPath string `json:"backing_path"`
}

type InspectRootFSRequest struct {
	Target RootFSContainerRef `json:"target"`
}

type InspectRootFSResponse struct {
	Info  RootFSInfo `json:"info,omitempty"`
	Error string     `json:"error,omitempty"`
}

type SaveRootFSRequest struct {
	Target                    RootFSContainerRef        `json:"target"`
	HeadID                    string                    `json:"head_id,omitempty"`
	SandboxID                 string                    `json:"sandbox_id"`
	TeamID                    string                    `json:"team_id"`
	FilesystemID              string                    `json:"filesystem_id,omitempty"`
	Parent                    *rootfshead.HeadReference `json:"parent,omitempty"`
	ExpectedRuntimeGeneration int64                     `json:"expected_runtime_generation,omitempty"`
	ExcludedPaths             []string                  `json:"excluded_paths,omitempty"`
	PortalPaths               []RootFSPortalPath        `json:"portal_paths,omitempty"`
}

type SaveRootFSResponse struct {
	Info       RootFSInfo                 `json:"info,omitempty"`
	Checkpoint RootFSCheckpointDescriptor `json:"checkpoint,omitempty"`
	Error      string                     `json:"error,omitempty"`
}

type PrepareRootFSSnapshotRequest struct {
	Target        RootFSContainerRef        `json:"target"`
	HeadID        string                    `json:"head_id"`
	SandboxID     string                    `json:"sandbox_id"`
	TeamID        string                    `json:"team_id"`
	FilesystemID  string                    `json:"filesystem_id"`
	Parent        *rootfshead.HeadReference `json:"parent,omitempty"`
	ExcludedPaths []string                  `json:"excluded_paths,omitempty"`
	PortalPaths   []RootFSPortalPath        `json:"portal_paths,omitempty"`
}

type PrepareRootFSSnapshotResponse struct {
	Handle     string                     `json:"handle,omitempty"`
	Info       RootFSInfo                 `json:"info,omitempty"`
	Checkpoint RootFSCheckpointDescriptor `json:"checkpoint,omitempty"`
	Error      string                     `json:"error,omitempty"`
}

type PublishRootFSSnapshotRequest struct {
	Handle string `json:"handle"`
}

type PublishRootFSSnapshotResponse struct {
	Info       RootFSInfo                 `json:"info,omitempty"`
	Checkpoint RootFSCheckpointDescriptor `json:"checkpoint,omitempty"`
	Published  bool                       `json:"published"`
	Error      string                     `json:"error,omitempty"`
}

type AbortRootFSSnapshotRequest struct {
	Handle string `json:"handle"`
}

type AbortRootFSSnapshotResponse struct {
	Aborted bool   `json:"aborted"`
	Error   string `json:"error,omitempty"`
}

type BindRootFSSyncRequest struct {
	Target        RootFSContainerRef        `json:"target"`
	SandboxID     string                    `json:"sandbox_id"`
	TeamID        string                    `json:"team_id"`
	FilesystemID  string                    `json:"filesystem_id"`
	Parent        *rootfshead.HeadReference `json:"parent,omitempty"`
	ExcludedPaths []string                  `json:"excluded_paths,omitempty"`
	PortalPaths   []RootFSPortalPath        `json:"portal_paths,omitempty"`
}

type BindRootFSSyncResponse struct {
	Bound bool   `json:"bound"`
	Error string `json:"error,omitempty"`
}

// BindVolumePortalRequest binds one pre-published pod portal to a concrete
// sandbox volume at claim time.
type BindVolumePortalRequest struct {
	Namespace       string `json:"namespace"`
	PodName         string `json:"pod_name"`
	PodUID          string `json:"pod_uid"`
	PortalName      string `json:"portal_name,omitempty"`
	MountPath       string `json:"mount_path"`
	SandboxID       string `json:"sandbox_id"`
	TeamID          string `json:"team_id"`
	SandboxVolumeID string `json:"sandboxvolume_id"`
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
	// RetainHot is set only for an intentional pause. Delete and stale-runtime
	// cleanup must close the engine instead of polluting the resume cache.
	RetainHot bool `json:"retain_hot,omitempty"`
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
