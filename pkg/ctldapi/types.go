package ctldapi

import (
	"time"

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
}

type BindRootFSSyncRequest struct {
	Target            RootFSContainerRef        `json:"target"`
	SandboxID         string                    `json:"sandbox_id"`
	TeamID            string                    `json:"team_id"`
	RuntimeGeneration int64                     `json:"runtime_generation"`
	Parent            *rootfshead.HeadReference `json:"parent,omitempty"`
	ExpectedBase      *rootfshead.BaseIdentity  `json:"expected_base,omitempty"`
	ExcludedPaths     []string                  `json:"excluded_paths,omitempty"`
}

type RootFSSyncStatus struct {
	SandboxID           string                    `json:"sandbox_id"`
	RuntimeGeneration   int64                     `json:"runtime_generation"`
	InitialScanComplete bool                      `json:"initial_scan_complete"`
	DirtyPaths          int                       `json:"dirty_paths"`
	DirtyBytes          int64                     `json:"dirty_bytes"`
	ActiveCaptures      int                       `json:"active_captures"`
	WatcherErrors       uint64                    `json:"watcher_errors"`
	Reconciliations     uint64                    `json:"reconciliations"`
	NeedsFullReconcile  bool                      `json:"needs_full_reconcile"`
	LastError           string                    `json:"last_error,omitempty"`
	Sealing             bool                      `json:"sealing"`
	Sealed              bool                      `json:"sealed"`
	SealedReference     *rootfshead.HeadReference `json:"sealed_reference,omitempty"`
}

type BindRootFSSyncResponse struct {
	Info   RootFSInfo       `json:"info,omitempty"`
	Status RootFSSyncStatus `json:"status,omitempty"`
	Error  string           `json:"error,omitempty"`
}

type GetRootFSSyncStatusRequest struct {
	SandboxID         string `json:"sandbox_id"`
	RuntimeGeneration int64  `json:"runtime_generation"`
}

type GetRootFSSyncStatusResponse struct {
	Status RootFSSyncStatus `json:"status,omitempty"`
	Error  string           `json:"error,omitempty"`
}

type SealRootFSHeadRequest struct {
	SandboxID                 string                    `json:"sandbox_id"`
	TeamID                    string                    `json:"team_id"`
	HeadID                    string                    `json:"head_id"`
	ExpectedRuntimeGeneration int64                     `json:"expected_runtime_generation"`
	ExpectedParent            *rootfshead.HeadReference `json:"expected_parent,omitempty"`
}

type RootFSSealTimings struct {
	Reconcile time.Duration `json:"reconcile"`
	Flush     time.Duration `json:"flush"`
	Total     time.Duration `json:"total"`
}

type SealRootFSHeadResponse struct {
	Reference      rootfshead.HeadReference  `json:"reference,omitempty"`
	Head           rootfshead.Head           `json:"head,omitempty"`
	Image          rootfshead.ImageReference `json:"image,omitempty"`
	CreatedBytes   int64                     `json:"created_bytes"`
	CreatedObjects int64                     `json:"created_objects"`
	Timings        RootFSSealTimings         `json:"timings,omitempty"`
	Error          string                    `json:"error,omitempty"`
}

type AcknowledgeRootFSHeadRequest struct {
	SandboxID         string `json:"sandbox_id"`
	TeamID            string `json:"team_id"`
	RuntimeGeneration int64  `json:"runtime_generation"`
	HeadID            string `json:"head_id"`
	Published         bool   `json:"published"`
	RuntimeContinues  bool   `json:"runtime_continues"`
}

type AcknowledgeRootFSHeadResponse struct {
	Acknowledged bool             `json:"acknowledged"`
	Status       RootFSSyncStatus `json:"status,omitempty"`
	Error        string           `json:"error,omitempty"`
}

type MaterializeRootFSHeadRequest struct {
	Reference       rootfshead.HeadReference  `json:"reference"`
	Image           rootfshead.ImageReference `json:"image"`
	CarrierSlot     string                    `json:"carrier_slot,omitempty"`
	TargetImageName string                    `json:"target_image_name,omitempty"`
}

// ImportRootFSImageRequest captures the complete merged root of an OCI-backed
// import container into one immutable S0FS ImageFS Head.
type ImportRootFSImageRequest struct {
	Target       RootFSContainerRef `json:"target"`
	RevisionID   string             `json:"revision_id"`
	TeamID       string             `json:"team_id"`
	HeadID       string             `json:"head_id"`
	BaseImageRef string             `json:"base_image_ref"`
}

type ImportRootFSImageResponse struct {
	Reference      rootfshead.HeadReference  `json:"reference,omitempty"`
	Head           rootfshead.Head           `json:"head,omitempty"`
	Image          rootfshead.ImageReference `json:"image,omitempty"`
	SourceDigest   string                    `json:"source_digest,omitempty"`
	OCIConfig      []byte                    `json:"oci_config,omitempty"`
	CreatedBytes   int64                     `json:"created_bytes"`
	CreatedObjects int64                     `json:"created_objects"`
	Duration       time.Duration             `json:"duration,omitempty"`
	Error          string                    `json:"error,omitempty"`
}

type ReleaseCarrierGateRequest struct {
	Namespace string `json:"namespace"`
	PodName   string `json:"pod_name"`
	PodUID    string `json:"pod_uid"`
	Slot      string `json:"slot"`
}

type ReleaseCarrierGateResponse struct {
	Released bool   `json:"released"`
	Error    string `json:"error,omitempty"`
}

type MaterializeRootFSHeadResponse struct {
	ImageName    string `json:"image_name,omitempty"`
	Materialized bool   `json:"materialized"`
	Error        string `json:"error,omitempty"`
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
