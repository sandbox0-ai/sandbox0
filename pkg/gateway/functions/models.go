package functions

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const ProductionAlias = "production"

const (
	DefaultMinWarm                         = 0
	DefaultMaxActive                       = 20
	DefaultTargetConcurrency               = 80
	DefaultScaleDownAfterSeconds           = 300
	DefaultFailedRuntimeRetentionSeconds   = 300
	DefaultDrainingRuntimeRetentionSeconds = 30
	DefaultStartingRuntimeRetentionSeconds = 600
	MaximumMinWarm                         = 100
	MaximumMaxActive                       = 1000
	MaximumTargetConcurrency               = 1000
	MaximumScaleDownAfterSeconds           = 86400
	MinimumScaleDownAfterSeconds           = 30
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
	ID             string             `json:"id"`
	FunctionID     string             `json:"function_id"`
	TeamID         string             `json:"team_id"`
	RevisionNumber int                `json:"revision_number"`
	SourceType     RevisionSourceType `json:"source_type"`

	// Spec is the immutable execution contract for this revision. Publish flows
	// such as sandbox service publishing compile into this model before storage.
	Spec FunctionRevisionSpec `json:"revision_spec"`

	// Provenance records how the spec was produced. It is not used for runtime
	// execution and may differ between sandbox, CI, and future artifact flows.
	Provenance json.RawMessage `json:"provenance,omitempty"`

	// Legacy fields are retained as compatibility mirrors while the API moves
	// to FunctionRevisionSpec as the runtime contract.
	SourceSandboxID  string          `json:"source_sandbox_id,omitempty"`
	SourceServiceID  string          `json:"source_service_id,omitempty"`
	SourceTemplateID string          `json:"source_template_id,omitempty"`
	RestoreMounts    []RestoreMount  `json:"restore_mounts,omitempty"`
	ServiceSnapshot  json.RawMessage `json:"service_snapshot,omitempty"`
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

type RevisionSourceType string

const (
	RevisionSourceTypeSandboxService RevisionSourceType = "sandbox_service"
	RevisionSourceTypeRevisionSpec   RevisionSourceType = "revision_spec"
	RevisionSourceTypeArtifact       RevisionSourceType = "artifact"
)

type FunctionRevisionSpec struct {
	TemplateID     string                  `json:"template_id"`
	RuntimeService json.RawMessage         `json:"runtime_service"`
	Mounts         []FunctionRevisionMount `json:"mounts,omitempty"`
	StaticAssets   []FunctionStaticAsset   `json:"static_assets,omitempty"`
	EnvRefs        []FunctionEnvRef        `json:"env_refs,omitempty"`
}

type FunctionRevisionMount struct {
	Name            string                      `json:"name,omitempty"`
	MountPoint      string                      `json:"mount_point"`
	Mode            FunctionRevisionMountMode   `json:"mode,omitempty"`
	Materialization string                      `json:"materialization,omitempty"`
	Source          FunctionRevisionMountSource `json:"source"`
}

type FunctionRevisionMountMode string

const (
	FunctionRevisionMountModeReadOnly  FunctionRevisionMountMode = "read_only"
	FunctionRevisionMountModeReadWrite FunctionRevisionMountMode = "read_write"
)

type FunctionRevisionMountSource struct {
	Type                  FunctionRevisionMountSourceType `json:"type"`
	SandboxVolumeID       string                          `json:"sandboxvolume_id,omitempty"`
	SourceSandboxVolumeID string                          `json:"source_sandboxvolume_id,omitempty"`
	SnapshotID            string                          `json:"snapshot_id,omitempty"`
	ArtifactID            string                          `json:"artifact_id,omitempty"`
	Digest                string                          `json:"digest,omitempty"`
}

type FunctionRevisionMountSourceType string

const (
	FunctionRevisionMountSourceSandboxVolume  FunctionRevisionMountSourceType = "sandbox_volume"
	FunctionRevisionMountSourceArtifact       FunctionRevisionMountSourceType = "artifact"
	FunctionRevisionMountSourceVolumeSnapshot FunctionRevisionMountSourceType = "volume_snapshot"
)

type FunctionStaticAsset struct {
	ArtifactID  string `json:"artifact_id"`
	RoutePrefix string `json:"route_prefix"`
	Digest      string `json:"digest,omitempty"`
}

type FunctionEnvRef struct {
	Name      string `json:"name"`
	SourceRef string `json:"source_ref"`
}

type SandboxServiceProvenance struct {
	SandboxID  string `json:"sandbox_id"`
	ServiceID  string `json:"service_id"`
	TemplateID string `json:"template_id"`
}

type RevisionProvenance struct {
	Type           RevisionSourceType        `json:"type"`
	SandboxService *SandboxServiceProvenance `json:"sandbox_service,omitempty"`
}

func NewSandboxServiceRevisionSpec(templateID string, service any, mounts []RestoreMount) (FunctionRevisionSpec, error) {
	serviceBytes, err := json.Marshal(service)
	if err != nil {
		return FunctionRevisionSpec{}, err
	}
	spec := FunctionRevisionSpec{
		TemplateID:     strings.TrimSpace(templateID),
		RuntimeService: serviceBytes,
		Mounts:         RevisionMountsFromRestoreMounts(mounts),
	}
	return spec, spec.Validate()
}

func RevisionMountsFromRestoreMounts(mounts []RestoreMount) []FunctionRevisionMount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]FunctionRevisionMount, 0, len(mounts))
	for _, mount := range mounts {
		out = append(out, FunctionRevisionMount{
			MountPoint: strings.TrimSpace(mount.MountPoint),
			Mode:       FunctionRevisionMountModeReadWrite,
			Source: FunctionRevisionMountSource{
				Type:                  FunctionRevisionMountSourceSandboxVolume,
				SandboxVolumeID:       strings.TrimSpace(mount.SandboxVolumeID),
				SourceSandboxVolumeID: strings.TrimSpace(mount.SourceSandboxVolumeID),
				SnapshotID:            strings.TrimSpace(mount.SnapshotID),
			},
		})
	}
	return out
}

func RestoreMountsFromRevisionMounts(mounts []FunctionRevisionMount) []RestoreMount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]RestoreMount, 0, len(mounts))
	for _, mount := range mounts {
		if strings.TrimSpace(mount.Source.SandboxVolumeID) == "" {
			continue
		}
		out = append(out, RestoreMount{
			SandboxVolumeID:       strings.TrimSpace(mount.Source.SandboxVolumeID),
			SourceSandboxVolumeID: strings.TrimSpace(mount.Source.SourceSandboxVolumeID),
			SnapshotID:            strings.TrimSpace(mount.Source.SnapshotID),
			MountPoint:            strings.TrimSpace(mount.MountPoint),
		})
	}
	return out
}

func (s FunctionRevisionSpec) Validate() error {
	if strings.TrimSpace(s.TemplateID) == "" {
		return fmt.Errorf("revision_spec.template_id is required")
	}
	if len(s.RuntimeService) == 0 || string(s.RuntimeService) == "null" {
		return fmt.Errorf("revision_spec.runtime_service is required")
	}
	for i, mount := range s.Mounts {
		if strings.TrimSpace(mount.MountPoint) == "" {
			return fmt.Errorf("revision_spec.mounts[%d].mount_point is required", i)
		}
		switch mount.Source.Type {
		case FunctionRevisionMountSourceSandboxVolume:
			if strings.TrimSpace(mount.Source.SandboxVolumeID) == "" {
				return fmt.Errorf("revision_spec.mounts[%d].source.sandboxvolume_id is required", i)
			}
		case FunctionRevisionMountSourceArtifact:
			if strings.TrimSpace(mount.Source.ArtifactID) == "" {
				return fmt.Errorf("revision_spec.mounts[%d].source.artifact_id is required", i)
			}
		case FunctionRevisionMountSourceVolumeSnapshot:
			if strings.TrimSpace(mount.Source.SnapshotID) == "" {
				return fmt.Errorf("revision_spec.mounts[%d].source.snapshot_id is required", i)
			}
			if strings.TrimSpace(mount.Source.SandboxVolumeID) == "" {
				return fmt.Errorf("revision_spec.mounts[%d].source.sandboxvolume_id is required for volume snapshot mounts", i)
			}
		default:
			return fmt.Errorf("revision_spec.mounts[%d].source.type is invalid", i)
		}
	}
	return nil
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

type RuntimePhase string

const (
	RuntimePhaseDisabled     RuntimePhase = "disabled"
	RuntimePhaseIdle         RuntimePhase = "idle"
	RuntimePhaseProvisioning RuntimePhase = "provisioning"
	RuntimePhaseStarting     RuntimePhase = "starting"
	RuntimePhaseReady        RuntimePhase = "ready"
	RuntimePhaseDraining     RuntimePhase = "draining"
	RuntimePhaseFailed       RuntimePhase = "failed"
)

type RuntimeInstanceState string

const (
	RuntimeInstanceStateStarting RuntimeInstanceState = "starting"
	RuntimeInstanceStateReady    RuntimeInstanceState = "ready"
	RuntimeInstanceStateDraining RuntimeInstanceState = "draining"
	RuntimeInstanceStateFailed   RuntimeInstanceState = "failed"
)

type RuntimeReadinessState string

const (
	RuntimeReadinessStateUnknown  RuntimeReadinessState = "unknown"
	RuntimeReadinessStateChecking RuntimeReadinessState = "checking"
	RuntimeReadinessStateReady    RuntimeReadinessState = "ready"
	RuntimeReadinessStateFailed   RuntimeReadinessState = "failed"
)

type RuntimeInstance struct {
	ID                string                `json:"id"`
	TeamID            string                `json:"team_id"`
	FunctionID        string                `json:"function_id"`
	RevisionID        string                `json:"revision_id"`
	SandboxID         string                `json:"sandbox_id"`
	ContextID         *string               `json:"context_id,omitempty"`
	State             RuntimeInstanceState  `json:"state"`
	ReadinessState    RuntimeReadinessState `json:"readiness_state"`
	StartupDurationMS *int                  `json:"startup_duration_ms,omitempty"`
	LastError         *string               `json:"last_error,omitempty"`
	LastErrorAt       *time.Time            `json:"last_error_at,omitempty"`
	ReadyAt           *time.Time            `json:"ready_at,omitempty"`
	LastUsedAt        *time.Time            `json:"last_used_at,omitempty"`
	DrainingAt        *time.Time            `json:"draining_at,omitempty"`
	FailedAt          *time.Time            `json:"failed_at,omitempty"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
}

type RuntimeEvent struct {
	ID                string                `json:"id"`
	TeamID            string                `json:"team_id"`
	FunctionID        string                `json:"function_id"`
	RevisionID        string                `json:"revision_id"`
	RuntimeInstanceID *string               `json:"runtime_instance_id,omitempty"`
	RuntimeSandboxID  *string               `json:"runtime_sandbox_id,omitempty"`
	RuntimeContextID  *string               `json:"runtime_context_id,omitempty"`
	Phase             RuntimePhase          `json:"phase"`
	ReadinessState    RuntimeReadinessState `json:"readiness_state"`
	Reason            string                `json:"reason,omitempty"`
	Message           string                `json:"message,omitempty"`
	StartupDurationMS *int                  `json:"startup_duration_ms,omitempty"`
	CreatedAt         time.Time             `json:"created_at"`
}

type RuntimeStatus struct {
	FunctionID        string                `json:"function_id"`
	RevisionID        string                `json:"revision_id"`
	RevisionNumber    int                   `json:"revision_number"`
	State             RuntimeState          `json:"state"`
	Phase             RuntimePhase          `json:"phase"`
	Autoscaling       Autoscaling           `json:"autoscaling"`
	ReadinessState    RuntimeReadinessState `json:"readiness_state"`
	RuntimeSandboxID  *string               `json:"runtime_sandbox_id,omitempty"`
	RuntimeContextID  *string               `json:"runtime_context_id,omitempty"`
	RuntimeUpdatedAt  *time.Time            `json:"runtime_updated_at,omitempty"`
	StartupDurationMS *int                  `json:"startup_duration_ms,omitempty"`
	LastError         *string               `json:"last_error,omitempty"`
	LastErrorAt       *time.Time            `json:"last_error_at,omitempty"`
	Instances         []RuntimeInstance     `json:"instances,omitempty"`
	RecentEvents      []RuntimeEvent        `json:"recent_events,omitempty"`
}
