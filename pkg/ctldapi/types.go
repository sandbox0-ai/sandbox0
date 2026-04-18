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

// VolumeAttachRequest asks ctld to expose a node-local volume mount inside a
// running sandbox mount namespace.
type VolumeAttachRequest struct {
	SandboxID       string `json:"sandbox_id,omitempty"`
	TeamID          string `json:"team_id,omitempty"`
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
	AccessMode      string `json:"access_mode,omitempty"`
	CacheSize       string `json:"cache_size,omitempty"`
	Prefetch        int32  `json:"prefetch,omitempty"`
	BufferSize      string `json:"buffer_size,omitempty"`
	Writeback       bool   `json:"writeback,omitempty"`
}

// VolumeAttachResponse is returned after ctld has attached a volume to a
// sandbox. The mount session is owned by ctld and is used by procd to close the
// attach lifecycle without exposing storage credentials to the sandbox.
type VolumeAttachResponse struct {
	Attached       bool   `json:"attached"`
	AttachmentID   string `json:"attachment_id,omitempty"`
	MountSessionID string `json:"mount_session_id,omitempty"`
	Error          string `json:"error,omitempty"`
}

// VolumeDetachRequest asks ctld to detach a previously attached volume.
type VolumeDetachRequest struct {
	SandboxID       string `json:"sandbox_id,omitempty"`
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
	AttachmentID    string `json:"attachment_id,omitempty"`
	MountSessionID  string `json:"mount_session_id,omitempty"`
}

// VolumeDetachResponse is returned after ctld detaches a volume from a sandbox.
type VolumeDetachResponse struct {
	Detached bool   `json:"detached"`
	Error    string `json:"error,omitempty"`
}
