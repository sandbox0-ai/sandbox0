package reconciler

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

// TemplateStore provides read access to templates for reconciliation.
type TemplateStore interface {
	ListTemplates(ctx context.Context) ([]*template.Template, error)
}

// AllocationStore provides allocation updates for reconciliation.
type AllocationStore interface {
	UpsertAllocation(ctx context.Context, alloc *template.TemplateAllocation) error
	UpdateAllocationSyncStatus(ctx context.Context, scope, teamID, templateID, clusterID, status string, syncError *string) error
	DeleteAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) error
	ListAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) ([]*template.TemplateAllocation, error)
}

// ClusterStore provides access to cluster metadata.
type ClusterStore interface {
	ListEnabledClusters(ctx context.Context) ([]*template.Cluster, error)
	UpdateClusterLastSeen(ctx context.Context, clusterID string) error
}

// ClusterSummary represents the cluster capacity and status.
type ClusterSummary struct {
	ClusterID             string `json:"cluster_id"`
	NodeCount             int    `json:"node_count"`
	TotalNodeCount        int    `json:"total_node_count"`
	SandboxNodeCount      int    `json:"sandbox_node_count"`
	IdlePodCount          int32  `json:"idle_pod_count"`
	ActivePodCount        int32  `json:"active_pod_count"`
	PendingActivePodCount int32  `json:"pending_active_pod_count"`
	TotalPodCount         int32  `json:"total_pod_count"`
}

// TemplateStat represents statistics for a single template.
type TemplateStat struct {
	TemplateID         string `json:"template_id"`
	IdleCount          int32  `json:"idle_count"`
	ActiveCount        int32  `json:"active_count"`
	PendingActiveCount int32  `json:"pending_active_count"`
	MinIdle            int32  `json:"min_idle"`
	MaxIdle            int32  `json:"max_idle"`
}

// TemplateStats represents statistics for all templates in a cluster.
type TemplateStats struct {
	Templates []TemplateStat `json:"templates"`
}

// ClusterClient provides access to cluster-level APIs via cluster-gateway.
type ClusterClient interface {
	GetClusterSummary(ctx context.Context, baseURL string) (*ClusterSummary, error)
	GetTemplateStats(ctx context.Context, baseURL string) (*TemplateStats, error)
	CreateOrUpdateTemplate(ctx context.Context, baseURL string, template *v1alpha1.SandboxTemplate) error
	DeleteTemplate(ctx context.Context, baseURL string, templateID string) error
}

// TemplateApplier applies templates directly to the local cluster.
type TemplateApplier interface {
	ListTemplates(ctx context.Context) ([]*v1alpha1.SandboxTemplate, error)
	GetTemplate(ctx context.Context, id string) (*v1alpha1.SandboxTemplate, error)
	CreateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error)
	UpdateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error)
	DeleteTemplate(ctx context.Context, id string) error
}
