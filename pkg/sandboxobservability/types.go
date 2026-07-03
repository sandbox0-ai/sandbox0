package sandboxobservability

import (
	"context"
	"errors"
	"time"
)

// ErrBackendDisabled is returned when historical sandbox observability storage is not configured.
var ErrBackendDisabled = errors.New("sandbox observability backend is disabled")

// ErrBackendUnavailable is returned when configured historical storage cannot serve a request.
var ErrBackendUnavailable = errors.New("sandbox observability backend is unavailable")

// ErrInvalidCursor is returned when a query cursor cannot be decoded.
var ErrInvalidCursor = errors.New("invalid sandbox observability cursor")

type Source string

const (
	SourceManager Source = "manager"
	SourceNetd    Source = "netd"
	SourceProcd   Source = "procd"
)

type EventType string

const (
	EventTypeLifecycle    EventType = "lifecycle"
	EventTypeNetworkAudit EventType = "network_audit"
	EventTypeRuntimeStats EventType = "runtime_stats"
)

type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeDenied    Outcome = "denied"
	OutcomeError     Outcome = "error"
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
)

// Event is the durable per-sandbox historical observability and audit projection.
type Event struct {
	TeamID     string         `json:"team_id"`
	SandboxID  string         `json:"sandbox_id"`
	RegionID   string         `json:"region_id"`
	ClusterID  string         `json:"cluster_id"`
	OccurredAt time.Time      `json:"occurred_at"`
	IngestedAt time.Time      `json:"ingested_at"`
	Source     Source         `json:"source"`
	EventType  EventType      `json:"event_type"`
	Outcome    Outcome        `json:"outcome,omitempty"`
	Cursor     string         `json:"cursor"`
	Watermark  string         `json:"watermark"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// EventQuery describes typed filters accepted by the public historical query API.
type EventQuery struct {
	TeamID    string
	SandboxID string
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
	Cursor    string
	Source    Source
	EventType EventType
	Outcome   Outcome
	AuditOnly bool
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

// MetricSample is a durable per-sandbox numeric observability sample.
type MetricSample struct {
	TeamID     string         `json:"team_id"`
	SandboxID  string         `json:"sandbox_id"`
	RegionID   string         `json:"region_id"`
	ClusterID  string         `json:"cluster_id"`
	ContextID  string         `json:"context_id,omitempty"`
	OccurredAt time.Time      `json:"occurred_at"`
	IngestedAt time.Time      `json:"ingested_at"`
	Name       string         `json:"name"`
	Unit       string         `json:"unit,omitempty"`
	Value      float64        `json:"value"`
	Cursor     string         `json:"cursor"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type MetricQuery struct {
	TeamID    string
	SandboxID string
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
	Cursor    string
	Names     []string
	ContextID string
}

type MetricListResult struct {
	Samples    []MetricSample `json:"samples"`
	NextCursor string         `json:"next_cursor,omitempty"`
	Watermark  string         `json:"watermark,omitempty"`
}

// Repository is the storage/query boundary for historical sandbox observability.
type Repository interface {
	ListEvents(ctx context.Context, query EventQuery) (*EventListResult, error)
	ListAuditEvents(ctx context.Context, query EventQuery) (*EventListResult, error)
	ListLogs(ctx context.Context, query LogQuery) (*LogListResult, error)
	ListMetricSamples(ctx context.Context, query MetricQuery) (*MetricListResult, error)
}

// Writer is the ingest boundary for asynchronous observability projections.
type Writer interface {
	InsertEvents(ctx context.Context, events []Event) error
	InsertLogs(ctx context.Context, logs []LogEntry) error
	InsertMetricSamples(ctx context.Context, samples []MetricSample) error
}

type DisabledRepository struct{}

func NewDisabledRepository() DisabledRepository {
	return DisabledRepository{}
}

func (DisabledRepository) ListEvents(context.Context, EventQuery) (*EventListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) ListAuditEvents(context.Context, EventQuery) (*EventListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) ListLogs(context.Context, LogQuery) (*LogListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) ListMetricSamples(context.Context, MetricQuery) (*MetricListResult, error) {
	return nil, ErrBackendDisabled
}

func (DisabledRepository) InsertEvents(context.Context, []Event) error {
	return ErrBackendDisabled
}

func (DisabledRepository) InsertLogs(context.Context, []LogEntry) error {
	return ErrBackendDisabled
}

func (DisabledRepository) InsertMetricSamples(context.Context, []MetricSample) error {
	return ErrBackendDisabled
}

func ValidSource(source Source) bool {
	switch source {
	case SourceManager, SourceNetd, SourceProcd:
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
	case EventTypeLifecycle, EventTypeNetworkAudit, EventTypeRuntimeStats:
		return true
	default:
		return false
	}
}

func ValidOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeDenied, OutcomeError, OutcomeSucceeded, OutcomeFailed:
		return true
	default:
		return false
	}
}
