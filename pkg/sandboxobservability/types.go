package sandboxobservability

import (
	"context"
	"errors"
	"time"
)

const CurrentEventSchemaVersion = 2

// AuditDeliveryMode controls when an audit event has sufficient durable
// custody for its operation to proceed or complete. ClickHouse remains
// canonical in every mode; durable_async acknowledges the local delivery
// buffer before canonical replay completes.
type AuditDeliveryMode string

const (
	AuditDeliveryModeDurableAsync  AuditDeliveryMode = "durable_async"
	AuditDeliveryModeCanonicalSync AuditDeliveryMode = "canonical_sync"
)

// NormalizeAuditDeliveryMode applies the low-latency default used for
// non-mutating API requests and data-plane flows.
func NormalizeAuditDeliveryMode(mode AuditDeliveryMode) AuditDeliveryMode {
	switch mode {
	case "", AuditDeliveryModeDurableAsync:
		return AuditDeliveryModeDurableAsync
	default:
		return AuditDeliveryModeCanonicalSync
	}
}

// ErrBackendDisabled is returned when historical sandbox observability storage is not configured.
var ErrBackendDisabled = errors.New("sandbox observability backend is disabled")

// ErrBackendUnavailable is returned when configured historical storage cannot serve a request.
var ErrBackendUnavailable = errors.New("sandbox observability backend is unavailable")

// ErrInvalidCursor is returned when a query cursor cannot be decoded.
var ErrInvalidCursor = errors.New("invalid sandbox observability cursor")

// ErrInvalidQuery is returned when an observability query is not valid.
var ErrInvalidQuery = errors.New("invalid sandbox observability query")

type Source string

const (
	SourceClusterGateway Source = "cluster_gateway"
	SourceManager        Source = "manager"
	SourceNetd           Source = "netd"
	SourceProcd          Source = "procd"
	SourceCtld           Source = "ctld"
	SourceStorageProxy   Source = "storage_proxy"
)

type EventType string

const (
	EventTypeLifecycle    EventType = "lifecycle"
	EventTypeNetworkAudit EventType = "network_audit"
	EventTypeRuntimeStats EventType = "runtime_stats"
	EventTypeAPIAccess    EventType = "api_access"
	EventTypeProcess      EventType = "process"
	EventTypeFile         EventType = "file"
)

type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeDenied    Outcome = "denied"
	OutcomeError     Outcome = "error"
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
	OutcomeAccepted  Outcome = "accepted"
	OutcomeUnknown   Outcome = "unknown"
)

// EventPhase distinguishes an attempted operation from its eventual result or
// an independently observed effect.
type EventPhase string

const (
	EventPhaseAttempt EventPhase = "attempt"
	EventPhaseResult  EventPhase = "result"
	EventPhaseEffect  EventPhase = "effect"
)

// ActorKind identifies the trusted principal responsible for an audit event.
type ActorKind string

const (
	ActorKindHuman              ActorKind = "human"
	ActorKindAPIKey             ActorKind = "api_key"
	ActorKindService            ActorKind = "service"
	ActorKindSandboxWorkload    ActorKind = "sandbox_workload"
	ActorKindSSHUser            ActorKind = "ssh_user"
	ActorKindExposureCredential ActorKind = "exposure_credential"
	ActorKindAnonymous          ActorKind = "anonymous"
)

// AuditActor is derived from authenticated server-side identity. Producers
// must never populate it from untrusted request bodies or headers.
type AuditActor struct {
	Kind       ActorKind `json:"kind"`
	ID         string    `json:"id,omitempty"`
	UserID     string    `json:"user_id,omitempty"`
	APIKeyID   string    `json:"api_key_id,omitempty"`
	AuthMethod string    `json:"auth_method,omitempty"`
}

// AuditResource identifies the object affected by an audit action.
type AuditResource struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	Subresource string `json:"subresource,omitempty"`
}

// AuditRequest carries bounded request correlation metadata. It intentionally
// excludes request and response bodies, credentials, and authorization values.
type AuditRequest struct {
	RequestID  string `json:"request_id,omitempty"`
	TraceID    string `json:"trace_id,omitempty"`
	SourceIP   string `json:"source_ip,omitempty"`
	UserAgent  string `json:"user_agent,omitempty"`
	HTTPMethod string `json:"http_method,omitempty"`
	Route      string `json:"route,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
}

// AuditProducer identifies the authenticated component that observed an event.
type AuditProducer struct {
	Service  string `json:"service"`
	Instance string `json:"instance,omitempty"`
	Sequence int64  `json:"sequence,omitempty"`
}

// AuditSignatureStatus reports the query-time verification result for an
// event signature. It is not part of the signed or persisted payload.
type AuditSignatureStatus string

const (
	AuditSignatureStatusVerified    AuditSignatureStatus = "verified"
	AuditSignatureStatusInvalid     AuditSignatureStatus = "invalid"
	AuditSignatureStatusUnavailable AuditSignatureStatus = "unavailable"
)

// AuditIntegrity protects the canonical event payload. Signatures are created
// only after cluster-gateway has replaced producer-controlled identity fields.
type AuditIntegrity struct {
	Algorithm       string               `json:"algorithm"`
	PayloadHash     string               `json:"payload_hash"`
	Signature       string               `json:"signature"`
	SigningKeyID    string               `json:"signing_key_id"`
	SignatureStatus AuditSignatureStatus `json:"signature_status,omitempty"`
	EventIDConflict bool                 `json:"event_id_conflict,omitempty"`
}

// Event is a canonical per-sandbox audit fact stored in ClickHouse. IngestedAt
// is storage metadata; pagination and watch cursors belong to query results,
// not to the signed event payload.
type Event struct {
	EventID       string         `json:"event_id"`
	SchemaVersion int            `json:"schema_version"`
	TeamID        string         `json:"team_id"`
	SandboxID     string         `json:"sandbox_id"`
	RegionID      string         `json:"region_id"`
	ClusterID     string         `json:"cluster_id"`
	OccurredAt    time.Time      `json:"occurred_at"`
	IngestedAt    time.Time      `json:"ingested_at"`
	Source        Source         `json:"source"`
	EventType     EventType      `json:"event_type"`
	Phase         EventPhase     `json:"phase"`
	Outcome       Outcome        `json:"outcome"`
	Actor         AuditActor     `json:"actor"`
	Action        string         `json:"action"`
	Resource      AuditResource  `json:"resource"`
	OperationID   string         `json:"operation_id"`
	ParentEventID string         `json:"parent_event_id,omitempty"`
	Producer      AuditProducer  `json:"producer"`
	Request       AuditRequest   `json:"request,omitempty"`
	Integrity     AuditIntegrity `json:"integrity"`
	Attributes    map[string]any `json:"attributes,omitempty"`
}

// EventQuery describes typed filters accepted by the public historical query API.
type EventQuery struct {
	TeamID       string
	SandboxID    string
	StartTime    *time.Time
	EndTime      *time.Time
	Limit        int
	Cursor       string
	Source       Source
	EventType    EventType
	Outcome      Outcome
	ActorKind    ActorKind
	ActorID      string
	Action       string
	ResourceType string
	OperationID  string
	EventID      string
}

type EventListResult struct {
	Events     []Event `json:"events"`
	NextCursor string  `json:"next_cursor,omitempty"`
	Watermark  string  `json:"watermark,omitempty"`
}

type LogStream string

const (
	LogStreamStdout LogStream = "stdout"
	LogStreamStderr LogStream = "stderr"
	LogStreamPTY    LogStream = "pty"
)

// LogEntry is a durable per-sandbox process log projection.
type LogEntry struct {
	TeamID     string         `json:"team_id"`
	SandboxID  string         `json:"sandbox_id"`
	RegionID   string         `json:"region_id"`
	ClusterID  string         `json:"cluster_id"`
	ContextID  string         `json:"context_id,omitempty"`
	ProcessID  string         `json:"process_id,omitempty"`
	OccurredAt time.Time      `json:"occurred_at"`
	IngestedAt time.Time      `json:"ingested_at"`
	Stream     LogStream      `json:"stream,omitempty"`
	Message    string         `json:"message"`
	Cursor     string         `json:"cursor"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type LogQuery struct {
	TeamID    string
	SandboxID string
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
	Cursor    string
	ContextID string
	Stream    LogStream
}

type LogListResult struct {
	Logs       []LogEntry `json:"logs"`
	NextCursor string     `json:"next_cursor,omitempty"`
	Watermark  string     `json:"watermark,omitempty"`
}

// RuntimeMetricName identifies one of the bounded sandbox runtime series.
type RuntimeMetricName string

const (
	RuntimeMetricCPUUtilization       RuntimeMetricName = "sandbox.cpu.utilization"
	RuntimeMetricCPUUsage             RuntimeMetricName = "sandbox.cpu.usage"
	RuntimeMetricCPUTime              RuntimeMetricName = "sandbox.cpu.time"
	RuntimeMetricCPULimit             RuntimeMetricName = "sandbox.cpu.limit"
	RuntimeMetricMemoryUsage          RuntimeMetricName = "sandbox.memory.usage"
	RuntimeMetricMemoryWorkingSet     RuntimeMetricName = "sandbox.memory.working_set"
	RuntimeMetricMemoryAvailable      RuntimeMetricName = "sandbox.memory.available"
	RuntimeMetricMemoryLimit          RuntimeMetricName = "sandbox.memory.limit"
	RuntimeMetricMemoryUtilization    RuntimeMetricName = "sandbox.memory.utilization"
	RuntimeMetricNetworkIO            RuntimeMetricName = "sandbox.network.io"
	RuntimeMetricNetworkErrors        RuntimeMetricName = "sandbox.network.errors"
	RuntimeMetricProcessCount         RuntimeMetricName = "sandbox.process.count"
	RuntimeMetricRootFSWritableUsage  RuntimeMetricName = "sandbox.rootfs.writable.usage"
	RuntimeMetricRootFSWritableInodes RuntimeMetricName = "sandbox.rootfs.writable.inodes"
)

type RuntimeMetricKind string

const (
	RuntimeMetricKindGauge   RuntimeMetricKind = "gauge"
	RuntimeMetricKindCounter RuntimeMetricKind = "counter"
)

type RuntimeMetricUnit string

const (
	RuntimeMetricUnitRatio          RuntimeMetricUnit = "ratio"
	RuntimeMetricUnitCores          RuntimeMetricUnit = "cores"
	RuntimeMetricUnitSecond         RuntimeMetricUnit = "seconds"
	RuntimeMetricUnitBytes          RuntimeMetricUnit = "bytes"
	RuntimeMetricUnitCount          RuntimeMetricUnit = "count"
	RuntimeMetricUnitBytesPerSecond RuntimeMetricUnit = "bytes_per_second"
	RuntimeMetricUnitCountPerSecond RuntimeMetricUnit = "count_per_second"
)

type RuntimeMetricDirection string

const (
	RuntimeMetricDirectionReceive  RuntimeMetricDirection = "receive"
	RuntimeMetricDirectionTransmit RuntimeMetricDirection = "transmit"
)

type RuntimeMetricMissingReason string

const (
	RuntimeMetricMissingUnavailable     RuntimeMetricMissingReason = "unavailable"
	RuntimeMetricMissingUnsupported     RuntimeMetricMissingReason = "unsupported"
	RuntimeMetricMissingCollectionError RuntimeMetricMissingReason = "collection_error"
)

// RuntimeMetricMissing records an expected series that a collector could not observe.
type RuntimeMetricMissing struct {
	Metric     RuntimeMetricName          `json:"metric"`
	Dimensions map[string]string          `json:"dimensions,omitempty"`
	Reason     RuntimeMetricMissingReason `json:"reason"`
	Detail     string                     `json:"detail,omitempty"`
}

// RuntimeCPUValues contains sandbox-wide CPU values observed at one instant.
type RuntimeCPUValues struct {
	Utilization *float64 `json:"utilization,omitempty"`
	Usage       *float64 `json:"usage,omitempty"`
	TimeSeconds *float64 `json:"time_seconds,omitempty"`
	LimitCores  *float64 `json:"limit_cores,omitempty"`
}

// RuntimeMemoryValues contains sandbox-wide memory values observed at one instant.
type RuntimeMemoryValues struct {
	UsageBytes      *uint64  `json:"usage_bytes,omitempty"`
	WorkingSetBytes *uint64  `json:"working_set_bytes,omitempty"`
	AvailableBytes  *uint64  `json:"available_bytes,omitempty"`
	LimitBytes      *uint64  `json:"limit_bytes,omitempty"`
	Utilization     *float64 `json:"utilization,omitempty"`
}

// RuntimeNetworkValues contains monotonic sandbox network counters.
type RuntimeNetworkValues struct {
	ReceiveBytes   *uint64 `json:"receive_bytes,omitempty"`
	TransmitBytes  *uint64 `json:"transmit_bytes,omitempty"`
	ReceiveErrors  *uint64 `json:"receive_errors,omitempty"`
	TransmitErrors *uint64 `json:"transmit_errors,omitempty"`
}

// RuntimeProcessValues contains sandbox-wide process state.
type RuntimeProcessValues struct {
	Count *uint64 `json:"count,omitempty"`
}

// RuntimeRootFSWritableValues contains writable rootfs consumption when supported.
type RuntimeRootFSWritableValues struct {
	UsageBytes *uint64 `json:"usage_bytes,omitempty"`
	Inodes     *uint64 `json:"inodes,omitempty"`
}

// RuntimeSample is the low-cardinality, sandbox-wide ingest record. Counter deltas
// are valid only while both RuntimeGeneration and SeriesEpoch remain unchanged.
type RuntimeSample struct {
	TeamID            string                       `json:"team_id"`
	SandboxID         string                       `json:"sandbox_id"`
	RegionID          string                       `json:"region_id"`
	ClusterID         string                       `json:"cluster_id"`
	RuntimeGeneration int64                        `json:"runtime_generation"`
	SeriesEpoch       string                       `json:"series_epoch"`
	ObservedAt        time.Time                    `json:"observed_at"`
	IngestedAt        time.Time                    `json:"ingested_at"`
	SampleID          string                       `json:"sample_id"`
	CPU               *RuntimeCPUValues            `json:"cpu,omitempty"`
	Memory            *RuntimeMemoryValues         `json:"memory,omitempty"`
	Network           *RuntimeNetworkValues        `json:"network,omitempty"`
	Process           *RuntimeProcessValues        `json:"process,omitempty"`
	RootFSWritable    *RuntimeRootFSWritableValues `json:"rootfs_writable,omitempty"`
	Missing           []RuntimeMetricMissing       `json:"missing,omitempty"`
}

type RuntimeMetricStatistic string

const (
	RuntimeMetricStatisticAuto    RuntimeMetricStatistic = "auto"
	RuntimeMetricStatisticAverage RuntimeMetricStatistic = "average"
	RuntimeMetricStatisticMinimum RuntimeMetricStatistic = "minimum"
	RuntimeMetricStatisticMaximum RuntimeMetricStatistic = "maximum"
	RuntimeMetricStatisticLast    RuntimeMetricStatistic = "last"
	RuntimeMetricStatisticRate    RuntimeMetricStatistic = "rate"
)

type RuntimeSeriesQuery struct {
	TeamID    string
	SandboxID string
	StartTime time.Time
	EndTime   time.Time
	Metrics   []RuntimeMetricName
	Step      time.Duration
	Statistic RuntimeMetricStatistic
	MaxPoints int
}

type RuntimeSeriesPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

type RuntimeSeriesSegment struct {
	Points []RuntimeSeriesPoint `json:"points"`
}

type RuntimeSeries struct {
	Metric     RuntimeMetricName      `json:"metric"`
	Kind       RuntimeMetricKind      `json:"kind"`
	Unit       RuntimeMetricUnit      `json:"unit"`
	Statistic  RuntimeMetricStatistic `json:"statistic"`
	Dimensions map[string]string      `json:"dimensions,omitempty"`
	Segments   []RuntimeSeriesSegment `json:"segments"`
}

type RuntimeSeriesGapReason string

const (
	RuntimeSeriesGapUnavailable     RuntimeSeriesGapReason = "unavailable"
	RuntimeSeriesGapUnsupported     RuntimeSeriesGapReason = "unsupported"
	RuntimeSeriesGapCollectionError RuntimeSeriesGapReason = "collection_error"
	RuntimeSeriesGapNoData          RuntimeSeriesGapReason = "no_data"
	RuntimeSeriesGapSeriesReset     RuntimeSeriesGapReason = "series_reset"
)

type RuntimeSeriesGap struct {
	Metric     RuntimeMetricName      `json:"metric"`
	Dimensions map[string]string      `json:"dimensions,omitempty"`
	StartTime  time.Time              `json:"start_time"`
	EndTime    time.Time              `json:"end_time"`
	Reason     RuntimeSeriesGapReason `json:"reason"`
}

type RuntimeSeriesFreshnessStatus string

const (
	RuntimeSeriesFreshnessFresh   RuntimeSeriesFreshnessStatus = "fresh"
	RuntimeSeriesFreshnessStale   RuntimeSeriesFreshnessStatus = "stale"
	RuntimeSeriesFreshnessMissing RuntimeSeriesFreshnessStatus = "missing"
)

type RuntimeSeriesFreshness struct {
	NewestObservedAt *time.Time                   `json:"newest_observed_at,omitempty"`
	AgeSeconds       *float64                     `json:"age_seconds,omitempty"`
	Status           RuntimeSeriesFreshnessStatus `json:"status"`
}

type RuntimeSeriesResult struct {
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	StepSeconds int64                  `json:"step_seconds"`
	Series      []RuntimeSeries        `json:"series"`
	Freshness   RuntimeSeriesFreshness `json:"freshness"`
	Gaps        []RuntimeSeriesGap     `json:"gaps"`
	Partial     bool                   `json:"partial"`
}

type RuntimeMetricDescriptor struct {
	Name        RuntimeMetricName `json:"name"`
	Kind        RuntimeMetricKind `json:"kind"`
	Unit        RuntimeMetricUnit `json:"unit"`
	Dimensions  []string          `json:"dimensions"`
	Description string            `json:"description"`
}

type RuntimeMetricCatalog struct {
	Metrics []RuntimeMetricDescriptor `json:"metrics"`
}

type WatchOptions struct {
	Cursor          string
	Limit           int
	AfterIngestedAt *time.Time
}

// Repository is the storage/query boundary for historical sandbox observability.
type Repository interface {
	ListEvents(ctx context.Context, query EventQuery) (*EventListResult, error)
	ListLogs(ctx context.Context, query LogQuery) (*LogListResult, error)
	ListRuntimeSeries(ctx context.Context, query RuntimeSeriesQuery) (*RuntimeSeriesResult, error)
}

// WatchRepository streams observability records in ingestion order.
type WatchRepository interface {
	WatchEvents(ctx context.Context, query EventQuery, opts WatchOptions) (*EventListResult, error)
	WatchLogs(ctx context.Context, query LogQuery, opts WatchOptions) (*LogListResult, error)
}

// Writer is the ingest boundary for asynchronous observability projections.
type Writer interface {
	InsertEvents(ctx context.Context, events []Event) error
	InsertLogs(ctx context.Context, logs []LogEntry) error
	InsertRuntimeSamples(ctx context.Context, samples []RuntimeSample) error
}

type DisabledRepository struct{}

func NewDisabledRepository() DisabledRepository {
	return DisabledRepository{}
}

func (DisabledRepository) ListEvents(context.Context, EventQuery) (*EventListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) ListLogs(context.Context, LogQuery) (*LogListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) ListRuntimeSeries(context.Context, RuntimeSeriesQuery) (*RuntimeSeriesResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) WatchEvents(context.Context, EventQuery, WatchOptions) (*EventListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) WatchLogs(context.Context, LogQuery, WatchOptions) (*LogListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) InsertEvents(context.Context, []Event) error {
	return ErrBackendDisabled
}

func (DisabledRepository) InsertLogs(context.Context, []LogEntry) error {
	return ErrBackendDisabled
}

func (DisabledRepository) InsertRuntimeSamples(context.Context, []RuntimeSample) error {
	return ErrBackendDisabled
}

func ValidSource(source Source) bool {
	switch source {
	case SourceClusterGateway, SourceManager, SourceNetd, SourceProcd, SourceCtld, SourceStorageProxy:
		return true
	default:
		return false
	}
}

func ValidLogStream(stream LogStream) bool {
	switch stream {
	case LogStreamStdout, LogStreamStderr, LogStreamPTY:
		return true
	default:
		return false
	}
}

func ValidEventType(eventType EventType) bool {
	switch eventType {
	case EventTypeLifecycle, EventTypeNetworkAudit, EventTypeRuntimeStats, EventTypeAPIAccess, EventTypeProcess, EventTypeFile:
		return true
	default:
		return false
	}
}

func ValidOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeDenied, OutcomeError, OutcomeSucceeded, OutcomeFailed, OutcomeAccepted, OutcomeUnknown:
		return true
	default:
		return false
	}
}

func ValidEventPhase(phase EventPhase) bool {
	switch phase {
	case EventPhaseAttempt, EventPhaseResult, EventPhaseEffect:
		return true
	default:
		return false
	}
}

func ValidActorKind(kind ActorKind) bool {
	switch kind {
	case ActorKindHuman, ActorKindAPIKey, ActorKindService, ActorKindSandboxWorkload,
		ActorKindSSHUser, ActorKindExposureCredential, ActorKindAnonymous:
		return true
	default:
		return false
	}
}
