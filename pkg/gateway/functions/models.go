package functions

import (
	"encoding/json"
	"time"
)

const ProductionAlias = "production"

type Function struct {
	ID               string    `json:"id"`
	TeamID           string    `json:"team_id"`
	Name             string    `json:"name"`
	Slug             string    `json:"slug"`
	DomainLabel      string    `json:"domain_label"`
	ActiveRevisionID *string   `json:"active_revision_id,omitempty"`
	CreatedBy        string    `json:"created_by,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
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
	SandboxVolumeID string `json:"sandboxvolume_id"`
	MountPoint      string `json:"mount_point"`
}

type Alias struct {
	FunctionID     string    `json:"function_id"`
	Alias          string    `json:"alias"`
	RevisionID     string    `json:"revision_id"`
	RevisionNumber int       `json:"revision_number"`
	UpdatedBy      string    `json:"updated_by,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}
