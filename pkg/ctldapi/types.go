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

// RootFSContainerRef identifies the sandbox container whose writable rootfs
// should be inspected or checkpointed by node-local ctld.
type RootFSContainerRef struct {
	Namespace     string `json:"namespace"`
	PodName       string `json:"pod_name"`
	PodUID        string `json:"pod_uid,omitempty"`
	ContainerName string `json:"container_name"`
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
}

type RootFSDiffDescriptor struct {
	MediaType string `json:"media_type"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	ObjectKey string `json:"object_key,omitempty"`
}

// RootFSLayerDescriptor identifies one immutable rootfs diff layer in a
// sandbox rootfs head chain.
type RootFSLayerDescriptor struct {
	LayerID       string               `json:"layer_id"`
	ParentLayerID string               `json:"parent_layer_id,omitempty"`
	Descriptor    RootFSDiffDescriptor `json:"descriptor"`
}

type InspectRootFSRequest struct {
	Target RootFSContainerRef `json:"target"`
}

type InspectRootFSResponse struct {
	Info  RootFSInfo `json:"info,omitempty"`
	Error string     `json:"error,omitempty"`
}

type SaveRootFSRequest struct {
	Target                    RootFSContainerRef `json:"target"`
	SandboxID                 string             `json:"sandbox_id"`
	TeamID                    string             `json:"team_id"`
	ExpectedRuntimeGeneration int64              `json:"expected_runtime_generation,omitempty"`
	ParentLayerID             string             `json:"parent_layer_id,omitempty"`
	ObjectKey                 string             `json:"object_key,omitempty"`
	ExcludedPaths             []string           `json:"excluded_paths,omitempty"`
}

type SaveRootFSResponse struct {
	Info       RootFSInfo           `json:"info,omitempty"`
	Descriptor RootFSDiffDescriptor `json:"descriptor,omitempty"`
	Error      string               `json:"error,omitempty"`
}

type ApplyRootFSRequest struct {
	Target                      RootFSContainerRef      `json:"target"`
	ExpectedRuntime             string                  `json:"expected_runtime,omitempty"`
	ExpectedRuntimeHandler      string                  `json:"expected_runtime_handler,omitempty"`
	ExpectedSnapshotter         string                  `json:"expected_snapshotter,omitempty"`
	ExpectedBaseImageDigest     string                  `json:"expected_base_image_digest,omitempty"`
	ExpectedSnapshotParent      string                  `json:"expected_snapshot_parent,omitempty"`
	ExpectedSnapshotParentChain []string                `json:"expected_snapshot_parent_chain,omitempty"`
	BaselineLayerID             string                  `json:"baseline_layer_id,omitempty"`
	Layers                      []RootFSLayerDescriptor `json:"layers,omitempty"`
	Descriptor                  RootFSDiffDescriptor    `json:"descriptor"`
	ExcludedPaths               []string                `json:"excluded_paths,omitempty"`
}

type ApplyRootFSResponse struct {
	Info       RootFSInfo              `json:"info,omitempty"`
	Descriptor RootFSDiffDescriptor    `json:"descriptor,omitempty"`
	Layers     []RootFSLayerDescriptor `json:"layers,omitempty"`
	Applied    bool                    `json:"applied"`
	Error      string                  `json:"error,omitempty"`
}

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
