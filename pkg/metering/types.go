package metering

import (
	"context"
	"encoding/json"
	"time"
)

const (
	SchemaName = "metering"

	ProductSandbox      = "sandbox"
	ProductFunction     = "function"
	ProductManagedAgent = "managed_agent"

	EventTypeSandboxClaimed    = "sandbox.claimed"
	EventTypeSandboxPaused     = "sandbox.paused"
	EventTypeSandboxResumed    = "sandbox.resumed"
	EventTypeSandboxTerminated = "sandbox.terminated"

	EventTypeVolumeCreated    = "volume.created"
	EventTypeVolumeDeleted    = "volume.deleted"
	EventTypeVolumeForked     = "volume.forked"
	EventTypeSnapshotCreated  = "snapshot.created"
	EventTypeSnapshotDeleted  = "snapshot.deleted"
	EventTypeSnapshotRestored = "snapshot.restored"

	WindowTypeSandboxActiveSeconds = "sandbox.active_seconds"
	WindowTypeSandboxPausedSeconds = "sandbox.paused_seconds"
	WindowTypeSandboxIngressBytes  = "sandbox.ingress_bytes"
	WindowTypeSandboxEgressBytes   = "sandbox.egress_bytes"

	WindowTypeSandboxComputeMillicpuMilliseconds = "sandbox.compute_mcpu_milliseconds"
	WindowTypeSandboxMemoryMiBMilliseconds       = "sandbox.memory_mib_milliseconds"
	WindowTypeSandboxVolumeByteHours             = "sandbox.volume_byte_hours"
	WindowTypeSandboxSnapshotByteHours           = "sandbox.snapshot_byte_hours"

	WindowTypeFunctionRequestCount                = "function.request_count"
	WindowTypeFunctionRequestDurationMilliseconds = "function.request_duration_milliseconds"
	WindowTypeFunctionActiveRuntimeMilliseconds   = "function.active_runtime_milliseconds"
	WindowTypeFunctionActiveMillicpuMilliseconds  = "function.active_mcpu_milliseconds"
	WindowTypeFunctionActiveMiBMilliseconds       = "function.active_mib_milliseconds"
	WindowTypeFunctionIngressBytes                = "function.ingress_bytes"
	WindowTypeFunctionEgressBytes                 = "function.egress_bytes"
	WindowTypeFunctionVolumeByteHours             = "function.volume_byte_hours"
	WindowTypeFunctionSnapshotByteHours           = "function.snapshot_byte_hours"

	WindowTypeManagedAgentSessionRunningMilliseconds = "managed_agent.session_running_milliseconds"

	WindowUnitSeconds              = "seconds"
	WindowUnitMilliseconds         = "milliseconds"
	WindowUnitBytes                = "bytes"
	WindowUnitByteHours            = "byte_hours"
	WindowUnitCount                = "count"
	WindowUnitMillicpuMilliseconds = "millicpu_milliseconds"
	WindowUnitMiBMilliseconds      = "mib_milliseconds"

	SubjectTypeSandbox             = "sandbox"
	SubjectTypeVolume              = "volume"
	SubjectTypeSnapshot            = "snapshot"
	SubjectTypeFunction            = "function"
	SubjectTypeManagedAgentSession = "managed_agent_session"
)

type Event struct {
	Sequence    int64           `json:"sequence"`
	EventID     string          `json:"event_id"`
	Producer    string          `json:"producer"`
	RegionID    string          `json:"region_id,omitempty"`
	EventType   string          `json:"event_type"`
	SubjectType string          `json:"subject_type"`
	SubjectID   string          `json:"subject_id"`
	TeamID      string          `json:"team_id,omitempty"`
	UserID      string          `json:"user_id,omitempty"`
	SandboxID   string          `json:"sandbox_id,omitempty"`
	VolumeID    string          `json:"volume_id,omitempty"`
	SnapshotID  string          `json:"snapshot_id,omitempty"`
	TemplateID  string          `json:"template_id,omitempty"`
	ClusterID   string          `json:"cluster_id,omitempty"`
	OccurredAt  time.Time       `json:"occurred_at"`
	RecordedAt  time.Time       `json:"recorded_at"`
	Data        json.RawMessage `json:"data,omitempty"`
}

type Status struct {
	RegionID             string     `json:"region_id,omitempty"`
	LatestEventSequence  int64      `json:"latest_event_sequence"`
	LatestWindowSequence int64      `json:"latest_window_sequence"`
	CompleteBefore       *time.Time `json:"complete_before,omitempty"`
	ProducerCount        int        `json:"producer_count"`
}

type ProducerWatermark struct {
	Producer       string    `json:"producer"`
	RegionID       string    `json:"region_id,omitempty"`
	CompleteBefore time.Time `json:"complete_before"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Window struct {
	Sequence    int64           `json:"sequence"`
	WindowID    string          `json:"window_id"`
	Producer    string          `json:"producer"`
	RegionID    string          `json:"region_id,omitempty"`
	WindowType  string          `json:"window_type"`
	SubjectType string          `json:"subject_type"`
	SubjectID   string          `json:"subject_id"`
	TeamID      string          `json:"team_id,omitempty"`
	UserID      string          `json:"user_id,omitempty"`
	SandboxID   string          `json:"sandbox_id,omitempty"`
	VolumeID    string          `json:"volume_id,omitempty"`
	SnapshotID  string          `json:"snapshot_id,omitempty"`
	TemplateID  string          `json:"template_id,omitempty"`
	ClusterID   string          `json:"cluster_id,omitempty"`
	WindowStart time.Time       `json:"window_start"`
	WindowEnd   time.Time       `json:"window_end"`
	Value       int64           `json:"value"`
	Unit        string          `json:"unit"`
	RecordedAt  time.Time       `json:"recorded_at"`
	Data        json.RawMessage `json:"data,omitempty"`
}

type SandboxProjectionState struct {
	SandboxID                 string     `json:"sandbox_id"`
	Namespace                 string     `json:"namespace"`
	TeamID                    string     `json:"team_id,omitempty"`
	UserID                    string     `json:"user_id,omitempty"`
	TemplateID                string     `json:"template_id,omitempty"`
	ClusterID                 string     `json:"cluster_id,omitempty"`
	OwnerKind                 string     `json:"owner_kind,omitempty"`
	FunctionID                string     `json:"function_id,omitempty"`
	FunctionRevisionID        string     `json:"function_revision_id,omitempty"`
	FunctionRuntimeInstanceID string     `json:"function_runtime_instance_id,omitempty"`
	ResourceMillicpu          int64      `json:"resource_millicpu,omitempty"`
	ResourceMemoryMiB         int64      `json:"resource_memory_mib,omitempty"`
	ClaimedAt                 *time.Time `json:"claimed_at,omitempty"`
	ActiveSince               *time.Time `json:"active_since,omitempty"`
	Paused                    bool       `json:"paused"`
	PausedAt                  *time.Time `json:"paused_at,omitempty"`
	TerminatedAt              *time.Time `json:"terminated_at,omitempty"`
	LastObservedAt            time.Time  `json:"last_observed_at"`
	LastResourceVer           string     `json:"last_resource_version,omitempty"`
}

type EventRecorder interface {
	AppendEvent(ctx context.Context, event *Event) error
	AppendWindow(ctx context.Context, window *Window) error
	UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error
}
