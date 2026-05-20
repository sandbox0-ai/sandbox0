package functions

import (
	"encoding/json"
	"time"
)

const ProductionAlias = "production"

const (
	DefaultMinWarm               = 0
	DefaultMaxActive             = 20
	DefaultTargetConcurrency     = 80
	DefaultScaleDownAfterSeconds = 300
	MaximumMinWarm               = 100
	MaximumMaxActive             = 1000
	MaximumTargetConcurrency     = 1000
	MaximumScaleDownAfterSeconds = 86400
	MinimumScaleDownAfterSeconds = 30
)

type Function struct {
	ID               string      `json:"id"`
	TeamID           string      `json:"team_id"`
	Name             string      `json:"name"`
	Slug             string      `json:"slug"`
	DomainLabel      string      `json:"domain_label"`
	ActiveRevisionID *string     `json:"active_revision_id,omitempty"`
	Enabled          bool        `json:"enabled"`
	Autoscaling      Autoscaling `json:"autoscaling"`
	CreatedBy        string      `json:"created_by,omitempty"`
	CreatedAt        time.Time   `json:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
	DeletedAt        *time.Time  `json:"deleted_at,omitempty"`
}

type Revision struct {
	ID               string          `json:"id"`
	FunctionID       string          `json:"function_id"`
	TeamID           string          `json:"team_id"`
	RevisionNumber   int             `json:"revision_number"`
	SourceSandboxID  string          `json:"source_sandbox_id"`
	SourceServiceID  string          `json:"source_service_id"`
	SourceTemplateID string          `json:"source_template_id"`
	RestoreMounts    []RestoreMount  `json:"restore_mounts,omitempty"`
	ServiceSnapshot  json.RawMessage `json:"service_snapshot"`
	RuntimeSandboxID *string         `json:"runtime_sandbox_id,omitempty"`
	RuntimeContextID *string         `json:"runtime_context_id,omitempty"`
	RuntimeUpdatedAt *time.Time      `json:"runtime_updated_at,omitempty"`
	CreatedBy        string          `json:"created_by,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

type RestoreMount struct {
	SandboxVolumeID       string `json:"sandboxvolume_id"`
	SourceSandboxVolumeID string `json:"source_sandboxvolume_id,omitempty"`
	SnapshotID            string `json:"snapshot_id,omitempty"`
	MountPoint            string `json:"mount_point"`
}

type Alias struct {
	FunctionID     string    `json:"function_id"`
	Alias          string    `json:"alias"`
	RevisionID     string    `json:"revision_id"`
	RevisionNumber int       `json:"revision_number"`
	UpdatedBy      string    `json:"updated_by,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Autoscaling struct {
	MinWarm               int `json:"min_warm"`
	MaxActive             int `json:"max_active"`
	TargetConcurrency     int `json:"target_concurrency"`
	ScaleDownAfterSeconds int `json:"scale_down_after_seconds"`
}

func DefaultAutoscaling() Autoscaling {
	return Autoscaling{
		MinWarm:               DefaultMinWarm,
		MaxActive:             DefaultMaxActive,
		TargetConcurrency:     DefaultTargetConcurrency,
		ScaleDownAfterSeconds: DefaultScaleDownAfterSeconds,
	}
}

func NormalizeAutoscaling(value Autoscaling) Autoscaling {
	defaults := DefaultAutoscaling()
	if value.MinWarm < 0 {
		value.MinWarm = defaults.MinWarm
	}
	if value.MaxActive <= 0 {
		value.MaxActive = defaults.MaxActive
	}
	if value.TargetConcurrency <= 0 {
		value.TargetConcurrency = defaults.TargetConcurrency
	}
	if value.ScaleDownAfterSeconds <= 0 {
		value.ScaleDownAfterSeconds = defaults.ScaleDownAfterSeconds
	}
	if value.MinWarm > MaximumMinWarm {
		value.MinWarm = MaximumMinWarm
	}
	if value.MaxActive > MaximumMaxActive {
		value.MaxActive = MaximumMaxActive
	}
	if value.TargetConcurrency > MaximumTargetConcurrency {
		value.TargetConcurrency = MaximumTargetConcurrency
	}
	if value.ScaleDownAfterSeconds < MinimumScaleDownAfterSeconds {
		value.ScaleDownAfterSeconds = MinimumScaleDownAfterSeconds
	}
	if value.ScaleDownAfterSeconds > MaximumScaleDownAfterSeconds {
		value.ScaleDownAfterSeconds = MaximumScaleDownAfterSeconds
	}
	if value.MinWarm > value.MaxActive {
		value.MinWarm = value.MaxActive
	}
	return value
}

type RuntimeState string

const (
	RuntimeStateDisabled RuntimeState = "disabled"
	RuntimeStateIdle     RuntimeState = "idle"
	RuntimeStateActive   RuntimeState = "active"
)

type RuntimeInstanceState string

const (
	RuntimeInstanceStateStarting RuntimeInstanceState = "starting"
	RuntimeInstanceStateReady    RuntimeInstanceState = "ready"
	RuntimeInstanceStateDraining RuntimeInstanceState = "draining"
	RuntimeInstanceStateFailed   RuntimeInstanceState = "failed"
)

type RuntimeInstance struct {
	ID         string               `json:"id"`
	TeamID     string               `json:"team_id"`
	FunctionID string               `json:"function_id"`
	RevisionID string               `json:"revision_id"`
	SandboxID  string               `json:"sandbox_id"`
	ContextID  *string              `json:"context_id,omitempty"`
	State      RuntimeInstanceState `json:"state"`
	LastError  *string              `json:"last_error,omitempty"`
	ReadyAt    *time.Time           `json:"ready_at,omitempty"`
	LastUsedAt *time.Time           `json:"last_used_at,omitempty"`
	DrainingAt *time.Time           `json:"draining_at,omitempty"`
	FailedAt   *time.Time           `json:"failed_at,omitempty"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
}

type RuntimeStatus struct {
	FunctionID       string            `json:"function_id"`
	RevisionID       string            `json:"revision_id"`
	RevisionNumber   int               `json:"revision_number"`
	State            RuntimeState      `json:"state"`
	Autoscaling      Autoscaling       `json:"autoscaling"`
	RuntimeSandboxID *string           `json:"runtime_sandbox_id,omitempty"`
	RuntimeContextID *string           `json:"runtime_context_id,omitempty"`
	RuntimeUpdatedAt *time.Time        `json:"runtime_updated_at,omitempty"`
	Instances        []RuntimeInstance `json:"instances,omitempty"`
}
