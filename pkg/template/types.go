package template

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

// Template represents a SandboxTemplate stored in PostgreSQL.
type Template struct {
	TemplateID string                          `json:"template_id"`
	Scope      string                          `json:"scope"`             // public, team
	TeamID     string                          `json:"team_id,omitempty"` // only for scope=team
	UserID     string                          `json:"user_id,omitempty"` // creator/updater user id (best-effort)
	Spec       v1alpha1.SandboxTemplateSpec    `json:"spec"`
	Status     *v1alpha1.SandboxTemplateStatus `json:"status,omitempty"`
	CreatedAt  time.Time                       `json:"created_at"`
	UpdatedAt  time.Time                       `json:"updated_at"`
}

// TemplateAllocation represents how a template is allocated to a cluster.
type TemplateAllocation struct {
	TemplateID   string     `json:"template_id"`
	Scope        string     `json:"scope"`             // public, team
	TeamID       string     `json:"team_id,omitempty"` // only for scope=team
	ClusterID    string     `json:"cluster_id"`
	MinIdle      int32      `json:"min_idle"`
	MaxIdle      int32      `json:"max_idle"`
	LastSyncedAt *time.Time `json:"last_synced_at,omitempty"`
	SyncStatus   string     `json:"sync_status"`
	SyncError    *string    `json:"sync_error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// Cluster represents a registered data-plane cluster.
type Cluster struct {
	ClusterID          string     `json:"cluster_id"`
	ClusterName        string     `json:"cluster_name"`
	InternalGatewayURL string     `json:"internal_gateway_url"`
	Weight             int        `json:"weight"`
	Enabled            bool       `json:"enabled"`
	LastSeenAt         *time.Time `json:"last_seen_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}
