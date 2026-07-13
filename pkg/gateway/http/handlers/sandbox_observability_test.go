package handlers

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

type fakeSandboxObservabilityRepo struct {
	eventsResult        *sandboxobservability.EventListResult
	logsResult          *sandboxobservability.LogListResult
	runtimeResult       *sandboxobservability.RuntimeSeriesResult
	eventsErr           error
	logsErr             error
	runtimeErr          error
	ingestErr           error
	lastQuery           sandboxobservability.EventQuery
	lastLogQuery        sandboxobservability.LogQuery
	lastRuntimeQuery    sandboxobservability.RuntimeSeriesQuery
	ingestEvents        []sandboxobservability.Event
	ingestLogs          []sandboxobservability.LogEntry
	ingestRuntime       []sandboxobservability.RuntimeSample
	eventsCalled        bool
	logsCalled          bool
	runtimeCalled       bool
	ingestCalled        bool
	ingestLogsCalled    bool
	ingestRuntimeCalled bool
}

func (f *fakeSandboxObservabilityRepo) ListEvents(_ context.Context, query sandboxobservability.EventQuery) (*sandboxobservability.EventListResult, error) {
	f.eventsCalled = true
	f.lastQuery = query
	return f.eventsResult, f.eventsErr
}

func (f *fakeSandboxObservabilityRepo) ListLogs(_ context.Context, query sandboxobservability.LogQuery) (*sandboxobservability.LogListResult, error) {
	f.logsCalled = true
	f.lastLogQuery = query
	return f.logsResult, f.logsErr
}

func (f *fakeSandboxObservabilityRepo) ListRuntimeSeries(_ context.Context, query sandboxobservability.RuntimeSeriesQuery) (*sandboxobservability.RuntimeSeriesResult, error) {
	f.runtimeCalled = true
	f.lastRuntimeQuery = query
	return f.runtimeResult, f.runtimeErr
}

func (f *fakeSandboxObservabilityRepo) InsertEvents(_ context.Context, events []sandboxobservability.Event) error {
	f.ingestCalled = true
	f.ingestEvents = append(f.ingestEvents, events...)
	return f.ingestErr
}

func (f *fakeSandboxObservabilityRepo) InsertLogs(_ context.Context, logs []sandboxobservability.LogEntry) error {
	f.ingestLogsCalled = true
	f.ingestLogs = append(f.ingestLogs, logs...)
	return f.ingestErr
}

func (f *fakeSandboxObservabilityRepo) InsertRuntimeSamples(_ context.Context, samples []sandboxobservability.RuntimeSample) error {
	f.ingestRuntimeCalled = true
	f.ingestRuntime = append(f.ingestRuntime, samples...)
	return f.ingestErr
}

func TestSandboxObservabilityHandlerDisabledBackend(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := NewSandboxObservabilityHandler(nil, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var resp spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != spec.CodeUnavailable {
		t.Fatalf("error = %+v, want unavailable", resp.Error)
	}
}

func TestSandboxObservabilityHandlerParsesTypedQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	start := "2026-07-01T01:02:03Z"
	end := "2026-07-01T02:02:03Z"
	repo := &fakeSandboxObservabilityRepo{
		eventsResult: &sandboxobservability.EventListResult{
			Events: []sandboxobservability.Event{{
				TeamID:     "team-1",
				SandboxID:  "sb-1",
				RegionID:   "aws-us-east-1",
				ClusterID:  "cluster-a",
				OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC),
				IngestedAt: time.Date(2026, 7, 1, 1, 2, 4, 0, time.UTC),
				Source:     sandboxobservability.SourceNetd,
				EventType:  sandboxobservability.EventTypeNetworkAudit,
				Outcome:    sandboxobservability.OutcomeDenied,
				Cursor:     "netd:10",
				Watermark:  "netd:10",
			}},
			NextCursor: "netd:11",
			Watermark:  "netd:10",
		},
	}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())

	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events?start_time="+start+"&end_time="+end+"&limit=5000&cursor=abc&source=netd&event_type=network_audit&outcome=denied")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !repo.eventsCalled {
		t.Fatal("expected ListEvents to be called")
	}
	if repo.lastQuery.TeamID != "team-1" || repo.lastQuery.SandboxID != "sb-1" {
		t.Fatalf("query identity = %+v", repo.lastQuery)
	}
	if repo.lastQuery.Limit != maxSandboxObservabilityLimit {
		t.Fatalf("limit = %d, want %d", repo.lastQuery.Limit, maxSandboxObservabilityLimit)
	}
	if repo.lastQuery.Cursor != "abc" ||
		repo.lastQuery.Source != sandboxobservability.SourceNetd ||
		repo.lastQuery.EventType != sandboxobservability.EventTypeNetworkAudit ||
		repo.lastQuery.Outcome != sandboxobservability.OutcomeDenied {
		t.Fatalf("unexpected query filters: %+v", repo.lastQuery)
	}
	if repo.lastQuery.StartTime == nil || repo.lastQuery.EndTime == nil {
		t.Fatalf("expected time filters: %+v", repo.lastQuery)
	}
}

func TestSandboxObservabilityHandlerParsesLogQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{
		logsResult: &sandboxobservability.LogListResult{
			Logs: []sandboxobservability.LogEntry{{
				TeamID:     "team-1",
				SandboxID:  "sb-1",
				OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC),
				IngestedAt: time.Date(2026, 7, 1, 1, 2, 4, 0, time.UTC),
				ContextID:  "ctx-1",
				Stream:     sandboxobservability.LogStreamStdout,
				Message:    "hello",
				Cursor:     "log:1",
			}},
		},
	}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListLogs, "/api/v1/sandboxes/sb-1/observability/logs?context_id=ctx-1&stream=stdout&limit=2")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !repo.logsCalled {
		t.Fatal("expected ListLogs to be called")
	}
	if repo.lastLogQuery.TeamID != "team-1" || repo.lastLogQuery.SandboxID != "sb-1" ||
		repo.lastLogQuery.ContextID != "ctx-1" ||
		repo.lastLogQuery.Stream != sandboxobservability.LogStreamStdout ||
		repo.lastLogQuery.Limit != 2 {
		t.Fatalf("unexpected log query: %+v", repo.lastLogQuery)
	}
}

func TestSandboxObservabilityHandlerParsesRuntimeMetricQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{
		runtimeResult: &sandboxobservability.RuntimeSeriesResult{
			StartTime:   time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC),
			EndTime:     time.Date(2026, 7, 1, 2, 0, 0, 0, time.UTC),
			StepSeconds: 15,
			Series:      []sandboxobservability.RuntimeSeries{},
			Freshness:   sandboxobservability.RuntimeSeriesFreshness{Status: sandboxobservability.RuntimeSeriesFreshnessMissing},
			Gaps:        []sandboxobservability.RuntimeSeriesGap{},
			Partial:     true,
		},
	}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.GetRuntimeMetrics, "/api/v1/sandboxes/sb-1/metrics?start_time=2026-07-01T01:00:00Z&end_time=2026-07-01T02:00:00Z&metrics=sandbox.cpu.utilization,sandbox.network.io&step_seconds=15&statistic=maximum&max_points=120")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !repo.runtimeCalled {
		t.Fatal("expected ListRuntimeSeries to be called")
	}
	if repo.lastRuntimeQuery.TeamID != "team-1" || repo.lastRuntimeQuery.SandboxID != "sb-1" ||
		repo.lastRuntimeQuery.Step != 15*time.Second || repo.lastRuntimeQuery.MaxPoints != 120 ||
		repo.lastRuntimeQuery.Statistic != sandboxobservability.RuntimeMetricStatisticMaximum {
		t.Fatalf("unexpected runtime query: %+v", repo.lastRuntimeQuery)
	}
	wantNames := []sandboxobservability.RuntimeMetricName{sandboxobservability.RuntimeMetricCPUUtilization, sandboxobservability.RuntimeMetricNetworkIO}
	if len(repo.lastRuntimeQuery.Metrics) != len(wantNames) {
		t.Fatalf("metric names = %+v, want %+v", repo.lastRuntimeQuery.Metrics, wantNames)
	}
	for i, want := range wantNames {
		if repo.lastRuntimeQuery.Metrics[i] != want {
			t.Fatalf("metric names = %+v, want %+v", repo.lastRuntimeQuery.Metrics, wantNames)
		}
	}
}

func TestSandboxObservabilityHandlerReturnsRuntimeMetricCatalogWithoutBackend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewSandboxObservabilityHandler(nil, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.GetRuntimeMetricsCatalog, "/api/v1/sandboxes/sb-1/metrics/catalog")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), string(sandboxobservability.RuntimeMetricCPUUtilization)) {
		t.Fatalf("catalog response missing CPU metric: %s", rec.Body.String())
	}
}

func TestSandboxObservabilityHandlerRejectsInvalidRuntimeMetricQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "unknown metric", query: "metrics=sandbox.cpu.unknown"},
		{name: "invalid statistic", query: "statistic=sum"},
		{name: "zero step", query: "step_seconds=0"},
		{name: "too many points", query: "max_points=1001"},
		{name: "empty window", query: "start_time=2026-07-01T01:00:00Z&end_time=2026-07-01T01:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			repo := &fakeSandboxObservabilityRepo{}
			handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
			rec := serveSandboxObservabilityRequest(t, handler.GetRuntimeMetrics, "/api/v1/sandboxes/sb-1/metrics?"+tt.query)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if repo.runtimeCalled {
				t.Fatal("repository should not be called for invalid runtime query")
			}
		})
	}
}

func TestSandboxObservabilityHandlerRejectsInvalidQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events?start_time=bad")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if repo.eventsCalled {
		t.Fatal("repository should not be called for invalid query")
	}
}

func TestSandboxObservabilityHandlerWatchDisabledBackend(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := NewSandboxObservabilityHandler(nil, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListLogs, "/api/v1/sandboxes/sb-1/observability/logs?watch=true")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSandboxObservabilityHandlerWatchRejectsEndTime(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events?watch=true&end_time=2026-07-01T01:02:03Z")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if repo.eventsCalled {
		t.Fatal("repository should not be called for invalid watch query")
	}
}

func TestSandboxObservabilityHandlerMapsRepositoryError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{eventsErr: errors.New("boom")}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestSandboxObservabilityHandlerMapsBackendUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{eventsErr: sandboxobservability.ErrBackendUnavailable}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSandboxObservabilityHandlerMapsInvalidCursor(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{eventsErr: sandboxobservability.ErrInvalidCursor}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListEvents, "/api/v1/sandboxes/sb-1/observability/events?cursor=bad")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSandboxObservabilityHandlerIngestEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{}
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop(), WithAuditIngestPolicy(AuditIngestPolicy{
		RegionID: "region-1", ClusterID: "cluster-1", SigningKey: key,
		Now: func() time.Time { return time.Date(2026, 7, 1, 1, 3, 0, 0, time.UTC) },
	}))
	rec := serveSandboxObservabilityIngestRequest(t, "/internal/v1/sandbox-observability/events", handler.IngestEvents, `{"events":[{"event_id":"11111111-1111-4111-8111-111111111111","team_id":"team-1","sandbox_id":"sb-1","occurred_at":"2026-07-01T01:02:03Z","source":"netd","event_type":"network_audit","phase":"effect","outcome":"completed","producer":{"service":"netd"},"attributes":{"action":"use-adapter","protocol_operations_truncated":true,"not_allowed":"drop-me"}}]}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if !repo.ingestCalled || len(repo.ingestEvents) != 1 {
		t.Fatalf("ingest called=%v events=%d", repo.ingestCalled, len(repo.ingestEvents))
	}
	event := repo.ingestEvents[0]
	if event.Source != sandboxobservability.SourceNetd || event.Actor.Kind != sandboxobservability.ActorKindSandboxWorkload || event.Action != "network.connect" {
		t.Fatalf("normalized event = %+v", event)
	}
	if truncated, _ := event.Attributes["protocol_operations_truncated"].(bool); !truncated {
		t.Fatalf("protocol_operations_truncated = %#v, want true", event.Attributes["protocol_operations_truncated"])
	}
	if _, ok := event.Attributes["not_allowed"]; ok {
		t.Fatalf("disallowed audit attribute was retained: %#v", event.Attributes)
	}
	if err := sandboxobservability.VerifyEventIntegrity(event, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("event integrity = %v", err)
	}
}

func TestSanitizeNetworkAuditAttributesBoundsEncodedSize(t *testing.T) {
	longValue := strings.Repeat("\x01\x02<&/very-long-path-and-reason", 4096)
	attributes := make(map[string]any)
	for key := range networkAuditStringAttributes {
		attributes[key] = longValue
	}
	for key := range networkAuditNumberAttributes {
		attributes[key] = 1.7976931348623157e+308
	}
	for key := range networkAuditBoolAttributes {
		attributes[key] = true
	}
	operations := make([]any, sandboxobservability.MaxNetworkAuditProtocolOperations)
	for i := range operations {
		operation := make(map[string]any, len(networkAuditProtocolOperationFields))
		for key := range networkAuditProtocolOperationFields {
			operation[key] = longValue
		}
		operations[i] = operation
	}
	attributes["protocol_operations"] = operations

	sanitized := sanitizeNetworkAuditAttributes(attributes)
	truncated, _ := sanitized["protocol_operations_truncated"].(bool)
	if !truncated {
		t.Fatal("protocol operation field truncation was not recorded")
	}
	boundedOperations, ok := sanitized["protocol_operations"].([]any)
	if !ok || len(boundedOperations) != sandboxobservability.MaxNetworkAuditProtocolOperations {
		t.Fatalf("protocol operations = %#v, want %d entries", sanitized["protocol_operations"], sandboxobservability.MaxNetworkAuditProtocolOperations)
	}
	firstOperation, ok := boundedOperations[0].(map[string]any)
	if !ok {
		t.Fatalf("first protocol operation = %#v", boundedOperations[0])
	}
	for _, key := range []string{"object", "reason"} {
		encoded, err := json.Marshal(firstOperation[key])
		if err != nil {
			t.Fatalf("marshal %s: %v", key, err)
		}
		if contentBytes := len(encoded) - 2; contentBytes > sandboxobservability.MaxNetworkAuditProtocolFieldEncodedBytes {
			t.Fatalf("encoded %s bytes = %d, want <= %d", key, contentBytes, sandboxobservability.MaxNetworkAuditProtocolFieldEncodedBytes)
		}
	}
	encoded, err := json.Marshal(sanitized)
	if err != nil {
		t.Fatalf("marshal sanitized attributes: %v", err)
	}
	if len(encoded) >= 64*1024 {
		t.Fatalf("sanitized attributes = %d bytes, want less than ClickHouse 64 KiB limit", len(encoded))
	}
}

func TestNormalizeNetdAuditReplayKeepsStableReplacingKey(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	now := time.Date(2026, 7, 1, 1, 3, 0, 0, time.UTC)
	handler := NewSandboxObservabilityHandler(&fakeSandboxObservabilityRepo{}, zap.NewNop(), WithAuditIngestPolicy(AuditIngestPolicy{
		RegionID: "region-1", ClusterID: "cluster-1", SigningKey: key,
		Now: func() time.Time { return now },
	}))
	raw := sandboxobservability.Event{
		EventID: "88888888-8888-4888-8888-888888888888", TeamID: "team-1", SandboxID: "sb-1",
		OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 456789000, time.UTC),
		EventType:  sandboxobservability.EventTypeNetworkAudit,
		Phase:      sandboxobservability.EventPhaseAttempt, Outcome: sandboxobservability.OutcomeAccepted,
		Producer:   sandboxobservability.AuditProducer{Sequence: 42},
		Attributes: map[string]any{"action": "use-adapter", "host": "example.com"},
	}
	ctx := internalauth.WithClaims(context.Background(), &internalauth.Claims{Caller: "netd", TeamID: "team-1", SandboxID: "sb-1"})
	first := []sandboxobservability.Event{raw}
	if err := handler.normalizeAuditEvents(ctx, first); err != nil {
		t.Fatalf("first normalizeAuditEvents() error = %v", err)
	}
	now = now.Add(2 * time.Hour)
	second := []sandboxobservability.Event{raw}
	if err := handler.normalizeAuditEvents(ctx, second); err != nil {
		t.Fatalf("second normalizeAuditEvents() error = %v", err)
	}

	if first[0].IngestedAt.Equal(second[0].IngestedAt) {
		t.Fatalf("gateway receipt versions unexpectedly match: %s", first[0].IngestedAt)
	}
	if first[0].Integrity.PayloadHash != second[0].Integrity.PayloadHash {
		t.Fatalf("payload hashes differ: %q != %q", first[0].Integrity.PayloadHash, second[0].Integrity.PayloadHash)
	}
	if first[0].Integrity.Signature != second[0].Integrity.Signature {
		t.Fatalf("signatures differ: %q != %q", first[0].Integrity.Signature, second[0].Integrity.Signature)
	}
	type replacingKey struct {
		TeamID, SandboxID, EventID, PayloadHash string
		OccurredAt                              int64
	}
	keyFor := func(event sandboxobservability.Event) replacingKey {
		return replacingKey{
			TeamID: event.TeamID, SandboxID: event.SandboxID, EventID: event.EventID,
			PayloadHash: event.Integrity.PayloadHash, OccurredAt: event.OccurredAt.UnixNano(),
		}
	}
	if keyFor(first[0]) != keyFor(second[0]) {
		t.Fatalf("ReplacingMergeTree keys differ: %#v != %#v", keyFor(first[0]), keyFor(second[0]))
	}
}

func TestSandboxObservabilityHandlerRejectsUnscopedOrSpoofedAuditIngest(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	handler := NewSandboxObservabilityHandler(&fakeSandboxObservabilityRepo{}, zap.NewNop(), WithAuditIngestPolicy(AuditIngestPolicy{
		RegionID: "region-1", ClusterID: "cluster-1", SigningKey: key,
		Now: func() time.Time { return time.Date(2026, 7, 1, 1, 3, 0, 0, time.UTC) },
	}))
	base := sandboxobservability.Event{
		EventID: "11111111-1111-4111-8111-111111111111", TeamID: "team-1", SandboxID: "sb-1",
		OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC), EventType: sandboxobservability.EventTypeNetworkAudit,
	}

	tests := []struct {
		name   string
		claims *internalauth.Claims
		event  sandboxobservability.Event
	}{
		{name: "system token", claims: &internalauth.Claims{Caller: "netd", IsSystem: true}, event: base},
		{name: "team spoof", claims: &internalauth.Claims{Caller: "netd", TeamID: "team-2", SandboxID: "sb-1"}, event: base},
		{name: "sandbox spoof", claims: &internalauth.Claims{Caller: "netd", TeamID: "team-1", SandboxID: "sb-2"}, event: base},
		{name: "caller spoof", claims: &internalauth.Claims{Caller: "ctld", TeamID: "team-1", SandboxID: "sb-1"}, event: base},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := internalauth.WithClaims(context.Background(), tt.claims)
			if err := handler.normalizeAuditEvents(ctx, []sandboxobservability.Event{tt.event}); err == nil {
				t.Fatal("normalizeAuditEvents() error = nil")
			}
		})
	}
}

func TestSandboxObservabilityHandlerIngestLogs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityIngestRequest(t, "/internal/v1/sandbox-observability/logs", handler.IngestLogs, `{"logs":[{"team_id":"team-1","sandbox_id":"sb-1","occurred_at":"2026-07-01T01:02:03Z","stream":"stdout","message":"hello","cursor":"log:1"}]}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if !repo.ingestLogsCalled || len(repo.ingestLogs) != 1 {
		t.Fatalf("ingest logs called=%v logs=%d", repo.ingestLogsCalled, len(repo.ingestLogs))
	}
}

func TestSandboxObservabilityHandlerIngestRuntimeSamples(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityIngestRequest(t, "/internal/v1/sandbox-observability/runtime-samples", handler.IngestRuntimeSamples, `{"samples":[{"team_id":"team-1","sandbox_id":"sb-1","region_id":"region-1","cluster_id":"cluster-1","runtime_generation":2,"series_epoch":"epoch-1","observed_at":"2026-07-01T01:02:03Z","sample_id":"sample-1","cpu":{"usage":0.5}}]}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if !repo.ingestRuntimeCalled || len(repo.ingestRuntime) != 1 {
		t.Fatalf("ingest runtime called=%v samples=%d", repo.ingestRuntimeCalled, len(repo.ingestRuntime))
	}
}

func TestSandboxObservabilityHandlerIngestDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := NewSandboxObservabilityHandler(nil, zap.NewNop())
	rec := serveSandboxObservabilityIngestRequest(t, "/internal/v1/sandbox-observability/events", handler.IngestEvents, `{"events":[]}`)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func serveSandboxObservabilityRequest(t *testing.T, h gin.HandlerFunc, target string) *httptest.ResponseRecorder {
	t.Helper()

	router := gin.New()
	router.GET("/api/v1/sandboxes/:id/observability/events", withTestAuth(h))
	router.GET("/api/v1/sandboxes/:id/observability/logs", withTestAuth(h))
	router.GET("/api/v1/sandboxes/:id/metrics", withTestAuth(h))
	router.GET("/api/v1/sandboxes/:id/metrics/catalog", withTestAuth(h))

	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func serveSandboxObservabilityIngestRequest(t *testing.T, target string, h gin.HandlerFunc, body string) *httptest.ResponseRecorder {
	t.Helper()

	router := gin.New()
	router.POST(target, withTestAuth(h))

	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func withTestAuth(next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		authCtx := &authn.AuthContext{
			TeamID:      "team-1",
			UserID:      "user-1",
			Permissions: []string{authn.PermSandboxRead},
		}
		c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), authCtx))
		c.Request = c.Request.WithContext(internalauth.WithClaims(c.Request.Context(), &internalauth.Claims{
			Caller: "netd", TeamID: "team-1", SandboxID: "sb-1",
		}))
		next(c)
	}
}
