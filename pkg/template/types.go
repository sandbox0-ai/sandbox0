package template

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

// RootFSTemplateStorageFormatBlockCOWV1 is the only runtime-native template
// capture format. It identifies an immutable regional block generation rather
// than an OCI diff layer.
const RootFSTemplateStorageFormatBlockCOWV1 = "block-cow-v1"

const (
	// TemplateBuildCaptureVersionOCI identifies the legacy OCI-layer handoff.
	TemplateBuildCaptureVersionOCI = 1
	// TemplateBuildCaptureVersionBlockCOW identifies the runtime-native block
	// generation handoff. Workers must not claim another version's publication.
	TemplateBuildCaptureVersionBlockCOW = 2
)

// AnnotationCopiedRootFS marks cluster templates whose image includes a
// writable rootfs captured from another sandbox.
const AnnotationCopiedRootFS = "sandbox0.ai/template-copied-rootfs"

// HasCopiedRootFS reports whether projected template metadata requires a new
// sandbox runtime to discard session identity copied into its base image.
func HasCopiedRootFS(annotations map[string]string) bool {
	return strings.EqualFold(strings.TrimSpace(annotations[AnnotationCopiedRootFS]), "true")
}

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
	CreationBuildID        string                `json:"-"`
	CreationIdempotencyKey string                `json:"-"`
	CreationRequestHash    string                `json:"-"`
	RootFS                 *RootFSTemplateSource `json:"-"`
}

// RootFSTemplateSource is the internal immutable RootFS attestation retained
// by a template created from a sandbox. Public callers can neither submit nor
// mutate these fields.
type RootFSTemplateSource struct {
	StorageFormat      string
	SnapshotID         string
	GenerationID       string
	SourceOCIDigest    string
	BaseArtifactDigest string
	FormatGeneration   int
	Platform           ocispec.Platform
}

// Validate rejects partial, mutable, or unattested RootFS template sources.
func (s RootFSTemplateSource) Validate() error {
	if s.StorageFormat != RootFSTemplateStorageFormatBlockCOWV1 {
		return fmt.Errorf("unsupported template RootFS storage format %q", s.StorageFormat)
	}
	for name, value := range map[string]string{
		"snapshot_id": s.SnapshotID, "generation_id": s.GenerationID,
		"source_oci_digest": s.SourceOCIDigest, "base_artifact_digest": s.BaseArtifactDigest,
		"platform_os": s.Platform.OS, "platform_architecture": s.Platform.Architecture,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return fmt.Errorf("%s must be canonical and non-empty", name)
		}
	}
	if s.FormatGeneration <= 0 {
		return fmt.Errorf("format_generation must be positive")
	}
	if parsed, err := digest.Parse(s.SourceOCIDigest); err != nil || parsed.String() != s.SourceOCIDigest {
		return fmt.Errorf("source_oci_digest must be canonical")
	}
	if parsed, err := digest.Parse(s.BaseArtifactDigest); err != nil || parsed.String() != s.BaseArtifactDigest {
		return fmt.Errorf("base_artifact_digest must be canonical")
	}
	if strings.TrimSpace(s.Platform.Variant) != s.Platform.Variant {
		return fmt.Errorf("platform_variant must be canonical")
	}
	return nil
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

// TemplateBuild is one durable, cluster-targeted template RootFS build.
type TemplateBuild struct {
	BuildID           string
	Scope             string
	TeamID            string
	UserID            string
	TemplateID        string
	SourceSandboxID   string
	TargetClusterID   string
	DesiredSpec       v1alpha1.SandboxTemplateSpec
	RequestHash       string
	IdempotencyKey    string
	Status            string
	Stage             v1alpha1.TemplateCreationStage
	SnapshotID        string
	CaptureMetadata   json.RawMessage
	OutputImage       string
	AttemptCount      int
	NextAttemptAt     time.Time
	LeaseOwner        string
	LeaseExpiresAt    time.Time
	CancelRequestedAt time.Time
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// TemplateRootFSDeletion is a durable cleanup tombstone created before a
// template releases its internal RootFS snapshot.
type TemplateRootFSDeletion struct {
	SnapshotID     string
	TeamID         string
	AttemptCount   int
	LeaseOwner     string
	LeaseExpiresAt time.Time
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
	// ErrTemplateRootFSPublicationUncertain indicates PostgreSQL may have
	// committed the retained RootFS binding despite returning an error. Callers
	// must not release or delete the capture based on this result.
	ErrTemplateRootFSPublicationUncertain = errors.New("template RootFS publication outcome is uncertain")
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
