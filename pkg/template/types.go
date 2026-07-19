package template

import (
	"encoding/json"
	"errors"
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

	// CreationBuildID and idempotency fields are control-plane state and are
	// intentionally excluded from the public template representation.
	CreationBuildID               string `json:"-"`
	CreationIdempotencyKey        string `json:"-"`
	CreationRequestHash           string `json:"-"`
	CreationImageClusterID        string `json:"-"`
	CreationImageLogicalSizeBytes int64  `json:"-"`
}

// ReadyForClaim reports whether a template may be used to create sandboxes.
// Traditional image-based templates omit creation status and are ready.
func (t *Template) ReadyForClaim() bool {
	if t == nil || t.Status == nil || t.Status.Creation == nil {
		return true
	}
	return t.Status.Creation.State == v1alpha1.TemplateCreationStateReady
}

// ReadyForReconcile reports whether the template has a complete image spec
// that may be projected into data-plane clusters.
func (t *Template) ReadyForReconcile() bool {
	if t == nil || t.Status == nil || t.Status.Creation == nil {
		return true
	}
	creation := t.Status.Creation
	return creation.State == v1alpha1.TemplateCreationStateReady ||
		(creation.State == v1alpha1.TemplateCreationStateCreating &&
			creation.Stage == v1alpha1.TemplateCreationStageReconciling)
}

// TemplateBuild is one durable, cluster-targeted template image build.
type TemplateBuild struct {
	BuildID               string
	Scope                 string
	TeamID                string
	UserID                string
	TemplateID            string
	SourceSandboxID       string
	TargetClusterID       string
	DesiredSpec           v1alpha1.SandboxTemplateSpec
	RequestHash           string
	IdempotencyKey        string
	Status                string
	Stage                 v1alpha1.TemplateCreationStage
	SnapshotID            string
	CaptureMetadata       json.RawMessage
	OutputImage           string
	ImageManifestDigest   string
	ImageLogicalSizeBytes int64
	ImageQuotaReservedAt  time.Time
	ImagePushStartedAt    time.Time
	AttemptCount          int
	NextAttemptAt         time.Time
	LeaseOwner            string
	LeaseExpiresAt        time.Time
	CancelRequestedAt     time.Time
	LastError             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// TemplateImageCleanup is a durable registry deletion job. Team template
// object and managed-image byte quota remain committed until this job is
// acknowledged after registry deletion.
type TemplateImageCleanup struct {
	CleanupID             string
	Scope                 string
	TeamID                string
	TemplateID            string
	TargetClusterID       string
	OutputImage           string
	ImageLogicalSizeBytes int64
	Status                string
	AttemptCount          int
	NextAttemptAt         time.Time
	LeaseOwner            string
	LeaseExpiresAt        time.Time
	LastError             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// SandboxTemplateSource is the durable template context captured when a
// sandbox was claimed. It is used only between trusted control-plane services.
type SandboxTemplateSource struct {
	SandboxID  string                       `json:"sandbox_id"`
	TeamID     string                       `json:"team_id"`
	UserID     string                       `json:"user_id,omitempty"`
	ClusterID  string                       `json:"cluster_id"`
	TemplateID string                       `json:"template_id"`
	Spec       v1alpha1.SandboxTemplateSpec `json:"spec"`
}

const (
	TemplateBuildStatusQueued    = "queued"
	TemplateBuildStatusRunning   = "running"
	TemplateBuildStatusCancelled = "cancelled"
)

var (
	// ErrTemplateAlreadyExists indicates the logical template ID is occupied.
	ErrTemplateAlreadyExists = errors.New("template already exists")
	// ErrTemplateIdempotencyConflict indicates reuse of an idempotency key with
	// a different normalized request.
	ErrTemplateIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	// ErrTemplateBuildLeaseLost indicates a worker no longer owns a build lease.
	ErrTemplateBuildLeaseLost = errors.New("template build lease lost")
	// ErrTemplateNotReady indicates asynchronous creation has not produced a
	// claimable template.
	ErrTemplateNotReady = errors.New("template is not ready")
	// ErrTemplateSourceNotFound indicates the source sandbox does not exist.
	ErrTemplateSourceNotFound = errors.New("template source sandbox not found")
	// ErrTemplateSourceForbidden indicates the source belongs to another team.
	ErrTemplateSourceForbidden = errors.New("template source sandbox is forbidden")
	// ErrTemplateSourceNotReady indicates the source cannot currently be captured.
	ErrTemplateSourceNotReady = errors.New("template source sandbox is not ready")
	// ErrTemplateSourceUnavailable indicates the owning data plane is unavailable.
	ErrTemplateSourceUnavailable = errors.New("template source sandbox is unavailable")
	// ErrTemplateImageCleanupPending prevents a logical template ID from being
	// reused while its prior registry artifact is still being deleted.
	ErrTemplateImageCleanupPending = errors.New("template image cleanup is pending")
)

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
	ClusterID         string     `json:"cluster_id"`
	ClusterName       string     `json:"cluster_name"`
	ClusterGatewayURL string     `json:"cluster_gateway_url"`
	Weight            int        `json:"weight"`
	Enabled           bool       `json:"enabled"`
	LastSeenAt        *time.Time `json:"last_seen_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}
