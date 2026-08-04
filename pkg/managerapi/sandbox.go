package managerapi

import "time"

const (
	SandboxStatusStarting    = "starting"
	SandboxStatusRunning     = "running"
	SandboxStatusPaused      = "paused"
	SandboxStatusFailed      = "failed"
	SandboxStatusTerminating = "terminating"
)

// Sandbox is the manager response consumed by data-plane gateways.
type Sandbox struct {
	ID                string                 `json:"id"`
	TemplateID        string                 `json:"template_id"`
	TeamID            string                 `json:"team_id"`
	UserID            string                 `json:"user_id"`
	InternalAddr      string                 `json:"internal_addr"`
	Status            string                 `json:"status"`
	Paused            bool                   `json:"paused"`
	AutoResume        bool                   `json:"auto_resume"`
	Resources         *SandboxResourceConfig `json:"resources,omitempty"`
	Services          []SandboxAppService    `json:"services,omitempty"`
	Mounts            []ClaimMount           `json:"mounts,omitempty"`
	PodName           string                 `json:"pod_name"`
	RuntimeGeneration int64                  `json:"runtime_generation"`
	ExpiresAt         *time.Time             `json:"expires_at"`
	HardExpiresAt     *time.Time             `json:"hard_expires_at"`
	ClaimedAt         time.Time              `json:"claimed_at"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

// ClaimMount binds one SandboxVolume to a declared sandbox mount point.
type ClaimMount struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
}

// SandboxResourceConfig is an instance-level resource override.
type SandboxResourceConfig struct {
	Memory string `json:"memory,omitempty"`
}

// ResumeSandboxResponse is returned after a paused sandbox runtime is restored.
type ResumeSandboxResponse struct {
	SandboxID      string `json:"sandbox_id"`
	Resumed        bool   `json:"resumed"`
	RestoredMemory string `json:"restored_memory,omitempty"`
}
