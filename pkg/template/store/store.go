package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
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

// TemplateBuildStore persists asynchronous template image builds.
type TemplateBuildStore interface {
	// CreateTemplateBuild atomically creates the visible template and its
	// durable build. Replayed idempotent requests return the existing template
	// with created=false.
	CreateTemplateBuild(ctx context.Context, tpl *template.Template, build *template.TemplateBuild) (createdTemplate *template.Template, created bool, err error)
	GetTemplateByIdempotencyKey(ctx context.Context, scope, teamID, idempotencyKey string) (*template.Template, error)
	ClaimTemplateBuild(ctx context.Context, targetClusterID, workerID string, leaseDuration time.Duration) (*template.TemplateBuild, error)
	RenewTemplateBuildLease(ctx context.Context, buildID, workerID string, leaseDuration time.Duration) error
	MarkTemplateBuildCaptured(ctx context.Context, buildID, workerID, snapshotID string, captureMetadata json.RawMessage, capturedAt time.Time) error
	PublishTemplateBuild(ctx context.Context, buildID, workerID string, spec v1alpha1.SandboxTemplateSpec, outputImage string) error
	FailTemplateBuild(ctx context.Context, buildID, workerID, reason, message string) error
	ReleaseTemplateBuild(ctx context.Context, buildID, workerID string, retryAt time.Time, lastError string) error
	TemplateBuildCancelled(ctx context.Context, buildID string) (bool, error)
	FinishTemplateBuild(ctx context.Context, buildID, workerID string) error
	CancelTemplateBuildAndDeleteTemplate(ctx context.Context, scope, teamID, templateID string) (bool, error)
}

// TemplateBuildLifecycleStore terminates builds that can no longer capture
// their source sandbox because a data-plane cluster is being removed.
type TemplateBuildLifecycleStore interface {
	FailCapturingTemplateBuildsForCluster(ctx context.Context, clusterID, reason, message string) (int64, error)
}

// TemplateCreationStore owns the final creation-state transition after a
// reconciler has verified that a template is claimable.
type TemplateCreationStore interface {
	MarkTemplateCreationReady(ctx context.Context, scope, teamID, templateID, buildID string, completedAt time.Time) (bool, error)
}
