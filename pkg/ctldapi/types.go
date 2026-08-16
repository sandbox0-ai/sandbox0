package ctldapi

import "github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"

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

type RootFSDiffDescriptor struct {
	MediaType string `json:"media_type"`
	Digest    string `json:"digest"`
	DiffID    string `json:"diff_id,omitempty"`
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

type PrepareRootFSSnapshotRequest struct {
	Target        RootFSContainerRef `json:"target"`
	ParentLayerID string             `json:"parent_layer_id,omitempty"`
	ExcludedPaths []string           `json:"excluded_paths,omitempty"`
}

type PrepareRootFSSnapshotResponse struct {
	Handle     string               `json:"handle,omitempty"`
	Info       RootFSInfo           `json:"info,omitempty"`
	Descriptor RootFSDiffDescriptor `json:"descriptor,omitempty"`
	Error      string               `json:"error,omitempty"`
}

type PublishRootFSSnapshotRequest struct {
	Handle                    string `json:"handle"`
	SandboxID                 string `json:"sandbox_id"`
	TeamID                    string `json:"team_id"`
	ExpectedRuntimeGeneration int64  `json:"expected_runtime_generation,omitempty"`
	ObjectKey                 string `json:"object_key,omitempty"`
}

type PublishRootFSSnapshotResponse struct {
	Info       RootFSInfo           `json:"info,omitempty"`
	Descriptor RootFSDiffDescriptor `json:"descriptor,omitempty"`
	Published  bool                 `json:"published"`
	Error      string               `json:"error,omitempty"`
}

type AbortRootFSSnapshotRequest struct {
	Handle string `json:"handle"`
}

type AbortRootFSSnapshotResponse struct {
	Aborted bool   `json:"aborted"`
	Error   string `json:"error,omitempty"`
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
