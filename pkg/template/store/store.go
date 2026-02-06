package store

import (
	"context"

	"github.com/sandbox0-ai/infra/pkg/template"
)

// TemplateStore provides CRUD operations for templates.
type TemplateStore interface {
	CreateTemplate(ctx context.Context, template *template.Template) error
	GetTemplate(ctx context.Context, scope, teamID, templateID string) (*template.Template, error)
	GetTemplateForTeam(ctx context.Context, teamID, templateID string) (*template.Template, error)
	ListTemplates(ctx context.Context) ([]*template.Template, error)
	ListVisibleTemplates(ctx context.Context, teamID string) ([]*template.Template, error)
	UpdateTemplate(ctx context.Context, template *template.Template) error
	DeleteTemplate(ctx context.Context, scope, teamID, templateID string) error
}

// AllocationStore provides CRUD operations for template allocations.
type AllocationStore interface {
	UpsertAllocation(ctx context.Context, alloc *template.TemplateAllocation) error
	ListAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) ([]*template.TemplateAllocation, error)
	UpdateAllocationSyncStatus(ctx context.Context, scope, teamID, templateID, clusterID, status string, syncError *string) error
	DeleteAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) error
}
