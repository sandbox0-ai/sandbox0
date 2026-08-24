package store

import (
	"context"
	"encoding/json"
	"time"

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

// ImageSourceCursor is the stable keyset position used by active-active
// template image import discovery.
type ImageSourceCursor struct {
	Scope      string
	TeamID     string
	TemplateID string
}

// ImageSource is the bounded immutable input needed to discover a template's
// OCI-to-block import requirements.
type ImageSource struct {
	Cursor           ImageSourceCursor
	Image            string
	EphemeralStorage string
}

// ImageSourceStore enumerates ready image-based templates without loading
// captured block-COW templates or maintaining a second template projection.
type ImageSourceStore interface {
	ListImageSourcesForRootFSImport(context.Context, ImageSourceCursor, int) ([]ImageSource, error)
}

// TemplateBuildStore persists asynchronous template RootFS builds.
type TemplateBuildStore interface {
	// CreateTemplateBuild atomically creates the visible template and its
	// durable build. Replayed idempotent requests return the existing template
	// with created=false.
	CreateTemplateBuild(ctx context.Context, tpl *template.Template, build *template.TemplateBuild) (createdTemplate *template.Template, created bool, err error)
	GetTemplateByIdempotencyKey(ctx context.Context, scope, teamID, idempotencyKey string) (*template.Template, error)
	ClaimTemplateBuild(ctx context.Context, targetClusterID, workerID string, leaseDuration time.Duration) (*template.TemplateBuild, error)
	RenewTemplateBuildLease(ctx context.Context, buildID, workerID string, leaseDuration time.Duration) error
	MarkTemplateBuildCaptured(ctx context.Context, buildID, workerID, snapshotID string, captureMetadata json.RawMessage, capturedAt time.Time) error
	PublishRootFSTemplateBuild(ctx context.Context, buildID, workerID string, source template.RootFSTemplateSource, capturedAt time.Time) error
	FailTemplateBuild(ctx context.Context, buildID, workerID, reason, message string) error
	ReleaseTemplateBuild(ctx context.Context, buildID, workerID string, retryAt time.Time, lastError string) error
	TemplateBuildCancelled(ctx context.Context, buildID string) (bool, error)
	FinishTemplateBuild(ctx context.Context, buildID, workerID string) error
	CancelTemplateBuildAndDeleteTemplate(ctx context.Context, scope, teamID, templateID string) (bool, error)
}

// TemplateRootFSDeletionStore owns snapshot cleanup tombstones after a
// visible template or canceled build releases its internal capture.
type TemplateRootFSDeletionStore interface {
	ClaimTemplateRootFSDeletion(ctx context.Context, workerID string, leaseDuration time.Duration) (*template.TemplateRootFSDeletion, error)
	FinishTemplateRootFSDeletion(ctx context.Context, snapshotID, workerID string) error
	ReleaseTemplateRootFSDeletion(ctx context.Context, snapshotID, workerID string, retryAt time.Time, lastError string) error
}

// TemplateBuildLifecycleStore terminates builds that can no longer capture
// their source sandbox because a data-plane cluster is being removed.
type TemplateBuildLifecycleStore interface {
	FailCapturingTemplateBuildsForCluster(ctx context.Context, clusterID, reason, message string) (int64, error)
}
