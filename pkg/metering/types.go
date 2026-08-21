package metering

import (
	"context"
	"encoding/json"
	"time"
)

const (
	ProductSandbox = "sandbox"

	ProducerRootFSStorage = "manager.rootfs.storage"

	EventTypeSandboxClaimed    = "sandbox.claimed"
	EventTypeSandboxPaused     = "sandbox.paused"
	EventTypeSandboxResumed    = "sandbox.resumed"
	EventTypeSandboxTerminated = "sandbox.terminated"

	WindowTypeSandboxIngressBytes = "sandbox.ingress_bytes"
	WindowTypeSandboxEgressBytes  = "sandbox.egress_bytes"

	WindowTypeSandboxRuntimeMiBMilliseconds = "sandbox.runtime_mib_milliseconds"
	WindowTypeSandboxRootFSByteHours        = "sandbox.rootfs_byte_hours"
	WindowTypeSandboxObjectStoreGetRequests = "sandbox.object_store_get_requests"
	WindowTypeSandboxObjectStorePutRequests = "sandbox.object_store_put_requests"

	WindowUnitMilliseconds    = "milliseconds"
	WindowUnitBytes           = "bytes"
	WindowUnitByteHours       = "byte_hours"
	WindowUnitCount           = "count"
	WindowUnitMiBMilliseconds = "mib_milliseconds"

	SubjectTypeSandbox           = "sandbox"
	SubjectTypeRootFS            = "rootfs"
	SubjectTypeObjectStoreBucket = "object_store_bucket"
	SubjectTypeTemplate          = "template"
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
	LatestEventCursor    string     `json:"latest_event_cursor,omitempty"`
	LatestWindowCursor   string     `json:"latest_window_cursor,omitempty"`
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
	SandboxID            string     `json:"sandbox_id"`
	Namespace            string     `json:"namespace"`
	TeamID               string     `json:"team_id,omitempty"`
	UserID               string     `json:"user_id,omitempty"`
	TemplateID           string     `json:"template_id,omitempty"`
	ClusterID            string     `json:"cluster_id,omitempty"`
	OwnerKind            string     `json:"owner_kind,omitempty"`
	ResourceMillicpu     int64      `json:"resource_millicpu,omitempty"`
	ResourceMemoryMiB    int64      `json:"resource_memory_mib,omitempty"`
	ClaimedAt            *time.Time `json:"claimed_at,omitempty"`
	ActiveSince          *time.Time `json:"active_since,omitempty"`
	Paused               bool       `json:"paused"`
	PausedAt             *time.Time `json:"paused_at,omitempty"`
	TerminatedAt         *time.Time `json:"terminated_at,omitempty"`
	LastObservedAt       time.Time  `json:"last_observed_at"`
	LastResourceVer      string     `json:"last_resource_version,omitempty"`
	SourceRevision       int64      `json:"source_revision,omitempty"`
	SourceLifecycleEpoch int64      `json:"source_lifecycle_epoch,omitempty"`
}

type StorageProjectionState struct {
	SubjectType             string    `json:"subject_type"`
	SubjectID               string    `json:"subject_id"`
	Product                 string    `json:"product,omitempty"`
	OwnerKind               string    `json:"owner_kind,omitempty"`
	TeamID                  string    `json:"team_id,omitempty"`
	UserID                  string    `json:"user_id,omitempty"`
	SandboxID               string    `json:"sandbox_id,omitempty"`
	VolumeID                string    `json:"volume_id,omitempty"`
	SnapshotID              string    `json:"snapshot_id,omitempty"`
	ClusterID               string    `json:"cluster_id,omitempty"`
	RegionID                string    `json:"region_id,omitempty"`
	SizeBytes               int64     `json:"size_bytes"`
	ObservedAt              time.Time `json:"observed_at"`
	UnbilledByteNanoseconds int64     `json:"unbilled_byte_nanoseconds,omitempty"`
}

// StorageProjectionStateTombstone records a versioned deletion in the
// ClickHouse storage projection.
type StorageProjectionStateTombstone struct {
	State     *StorageProjectionState `json:"state"`
	DeletedAt time.Time               `json:"deleted_at"`
}

// StorageProjectionMutation preserves the outbox order of upserts and
// tombstones that target the same ClickHouse projection table.
type StorageProjectionMutation struct {
	State     *StorageProjectionState
	Deleted   bool
	DeletedAt time.Time
}

// ProjectionBatch is one or more ordered PostgreSQL outbox transactions
// decoded for an idempotent ClickHouse delivery attempt. Watermarks are
// applied only after every data and state row in the batch.
type ProjectionBatch struct {
	Events           []*Event
	Windows          []*Window
	Watermarks       []*ProducerWatermark
	SandboxStates    []*SandboxProjectionState
	StorageMutations []*StorageProjectionMutation
}

type StorageObservation struct {
	SubjectType       string    `json:"subject_type"`
	SubjectID         string    `json:"subject_id"`
	Product           string    `json:"product,omitempty"`
	OwnerKind         string    `json:"owner_kind,omitempty"`
	TeamID            string    `json:"team_id,omitempty"`
	UserID            string    `json:"user_id,omitempty"`
	SandboxID         string    `json:"sandbox_id,omitempty"`
	VolumeID          string    `json:"volume_id,omitempty"`
	SnapshotID        string    `json:"snapshot_id,omitempty"`
	ClusterID         string    `json:"cluster_id,omitempty"`
	RegionID          string    `json:"region_id,omitempty"`
	SizeBytes         int64     `json:"size_bytes"`
	ResourceCreatedAt time.Time `json:"resource_created_at,omitempty"`
	ObservedAt        time.Time `json:"observed_at"`
}

type EventRecorder interface {
	AppendEvent(ctx context.Context, event *Event) error
	AppendWindow(ctx context.Context, window *Window) error
	UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error
}
