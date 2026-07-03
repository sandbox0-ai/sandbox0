package handlers

import (
	"context"
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
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

type fakeSandboxObservabilityRepo struct {
	eventsResult        *sandboxobservability.EventListResult
	auditResult         *sandboxobservability.EventListResult
	logsResult          *sandboxobservability.LogListResult
	metricsResult       *sandboxobservability.MetricListResult
	eventsErr           error
	auditErr            error
	logsErr             error
	metricsErr          error
	ingestErr           error
	lastQuery           sandboxobservability.EventQuery
	lastLogQuery        sandboxobservability.LogQuery
	lastMetricQuery     sandboxobservability.MetricQuery
	ingestEvents        []sandboxobservability.Event
	ingestLogs          []sandboxobservability.LogEntry
	ingestMetrics       []sandboxobservability.MetricSample
	eventsCalled        bool
	auditCalled         bool
	logsCalled          bool
	metricsCalled       bool
	ingestCalled        bool
	ingestLogsCalled    bool
	ingestMetricsCalled bool
}

func (f *fakeSandboxObservabilityRepo) ListEvents(_ context.Context, query sandboxobservability.EventQuery) (*sandboxobservability.EventListResult, error) {
	f.eventsCalled = true
	f.lastQuery = query
	return f.eventsResult, f.eventsErr
}

func (f *fakeSandboxObservabilityRepo) ListAuditEvents(_ context.Context, query sandboxobservability.EventQuery) (*sandboxobservability.EventListResult, error) {
	f.auditCalled = true
	f.lastQuery = query
	return f.auditResult, f.auditErr
}

func (f *fakeSandboxObservabilityRepo) ListLogs(_ context.Context, query sandboxobservability.LogQuery) (*sandboxobservability.LogListResult, error) {
	f.logsCalled = true
	f.lastLogQuery = query
	return f.logsResult, f.logsErr
}

func (f *fakeSandboxObservabilityRepo) ListMetricSamples(_ context.Context, query sandboxobservability.MetricQuery) (*sandboxobservability.MetricListResult, error) {
	f.metricsCalled = true
	f.lastMetricQuery = query
	return f.metricsResult, f.metricsErr
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

func (f *fakeSandboxObservabilityRepo) InsertMetricSamples(_ context.Context, samples []sandboxobservability.MetricSample) error {
	f.ingestMetricsCalled = true
	f.ingestMetrics = append(f.ingestMetrics, samples...)
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

func TestSandboxObservabilityHandlerAuditEndpointUsesAuditRepositoryMethod(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{auditResult: &sandboxobservability.EventListResult{}}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListAuditEvents, "/api/v1/sandboxes/sb-1/audit/events?event_type=network_audit")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !repo.auditCalled || repo.eventsCalled {
		t.Fatalf("calls: events=%v audit=%v", repo.eventsCalled, repo.auditCalled)
	}
	if !repo.lastQuery.AuditOnly {
		t.Fatalf("AuditOnly = false, want true")
	}
}

func TestSandboxObservabilityHandlerAuditEndpointDefaultsToNetworkAudit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{auditResult: &sandboxobservability.EventListResult{}}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListAuditEvents, "/api/v1/sandboxes/sb-1/audit/events")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !repo.auditCalled || repo.lastQuery.EventType != sandboxobservability.EventTypeNetworkAudit {
		t.Fatalf("audit query = %+v", repo.lastQuery)
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

func TestSandboxObservabilityHandlerParsesMetricQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{
		metricsResult: &sandboxobservability.MetricListResult{
			Samples: []sandboxobservability.MetricSample{{
				TeamID:     "team-1",
				SandboxID:  "sb-1",
				OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC),
				IngestedAt: time.Date(2026, 7, 1, 1, 2, 4, 0, time.UTC),
				Name:       "cpu.usage",
				Unit:       "cores",
				Value:      0.5,
				Cursor:     "metric:1",
			}},
		},
	}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityRequest(t, handler.ListMetricSamples, "/api/v1/sandboxes/sb-1/observability/metrics?name=cpu.usage&names=memory.bytes,network.rx_bytes&context_id=ctx-1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !repo.metricsCalled {
		t.Fatal("expected ListMetricSamples to be called")
	}
	if repo.lastMetricQuery.TeamID != "team-1" || repo.lastMetricQuery.SandboxID != "sb-1" ||
		repo.lastMetricQuery.ContextID != "ctx-1" {
		t.Fatalf("unexpected metric query identity: %+v", repo.lastMetricQuery)
	}
	wantNames := []string{"cpu.usage", "memory.bytes", "network.rx_bytes"}
	if len(repo.lastMetricQuery.Names) != len(wantNames) {
		t.Fatalf("metric names = %+v, want %+v", repo.lastMetricQuery.Names, wantNames)
	}
	for i, want := range wantNames {
		if repo.lastMetricQuery.Names[i] != want {
			t.Fatalf("metric names = %+v, want %+v", repo.lastMetricQuery.Names, wantNames)
		}
	}
}

func TestSandboxObservabilityHandlerAuditEndpointRejectsNonAuditFilters(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []string{
		"/api/v1/sandboxes/sb-1/audit/events?event_type=lifecycle",
		"/api/v1/sandboxes/sb-1/audit/events?source=metering",
	}
	for _, path := range tests {
		repo := &fakeSandboxObservabilityRepo{auditResult: &sandboxobservability.EventListResult{}}
		handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
		rec := serveSandboxObservabilityRequest(t, handler.ListAuditEvents, path)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d: %s", path, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if repo.auditCalled || repo.eventsCalled {
			t.Fatalf("%s called repository: events=%v audit=%v", path, repo.eventsCalled, repo.auditCalled)
		}
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
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityIngestRequest(t, "/internal/v1/sandbox-observability/events", handler.IngestEvents, `{"events":[{"team_id":"team-1","sandbox_id":"sb-1","occurred_at":"2026-07-01T01:02:03Z","source":"netd","event_type":"network_audit","cursor":"netd:1","watermark":"netd:1"}]}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if !repo.ingestCalled || len(repo.ingestEvents) != 1 {
		t.Fatalf("ingest called=%v events=%d", repo.ingestCalled, len(repo.ingestEvents))
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

func TestSandboxObservabilityHandlerIngestMetricSamples(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeSandboxObservabilityRepo{}
	handler := NewSandboxObservabilityHandler(repo, zap.NewNop())
	rec := serveSandboxObservabilityIngestRequest(t, "/internal/v1/sandbox-observability/metrics", handler.IngestMetricSamples, `{"samples":[{"team_id":"team-1","sandbox_id":"sb-1","occurred_at":"2026-07-01T01:02:03Z","name":"cpu.usage","unit":"cores","value":0.5,"cursor":"metric:1"}]}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if !repo.ingestMetricsCalled || len(repo.ingestMetrics) != 1 {
		t.Fatalf("ingest metrics called=%v samples=%d", repo.ingestMetricsCalled, len(repo.ingestMetrics))
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
	router.GET("/api/v1/sandboxes/:id/observability/metrics", withTestAuth(h))
	router.GET("/api/v1/sandboxes/:id/audit/events", withTestAuth(h))

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
		next(c)
	}
}
