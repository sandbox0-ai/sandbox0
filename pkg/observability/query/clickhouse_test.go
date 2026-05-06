package query

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClickHouseClientListTraceSpansBuildsScopedQuery(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, password, ok := r.BasicAuth(); !ok || user != "reader" || password != "secret" {
			t.Fatalf("missing basic auth")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		query = string(body)
		_, _ = w.Write([]byte(`{"timestamp":"2026-05-07 12:00:00","trace_id":"tr1","span_id":"sp1","name":"HTTP GET /api/v1/sandboxes/{id}","duration_nano":"42","resource_attributes":{"sandbox0.team_id":"team-1"},"attributes":{"http.route":"/api/v1/sandboxes/:id"}}
`))
	}))
	defer server.Close()

	client, err := NewClickHouseClient(ClickHouseConfig{
		HTTPURL:  server.URL,
		Database: "sandbox0_observability",
		Username: "reader",
		Password: "secret",
		Client:   server.Client(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	start := time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC)
	spans, err := client.ListTraceSpans(context.Background(), ListOptions{
		TeamID:    "team-1",
		SandboxID: "sb_1",
		TraceID:   "tr1",
		StartTime: start,
		Limit:     5000,
	})
	if err != nil {
		t.Fatalf("list spans: %v", err)
	}
	for _, want := range []string{
		"FROM `sandbox0_observability`.`otel_traces`",
		"(ResourceAttributes['sandbox0.team_id'] = 'team-1' OR `SpanAttributes`['sandbox0.team_id'] = 'team-1')",
		"(ResourceAttributes['sandbox0.sandbox_id'] = 'sb_1' OR `SpanAttributes`['sandbox0.sandbox_id'] = 'sb_1')",
		"TraceId = 'tr1'",
		"LIMIT 1000",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q:\n%s", want, query)
		}
	}
	if len(spans) != 1 || spans[0].DurationNano != 42 || spans[0].ResourceAttributes["sandbox0.team_id"] != "team-1" {
		t.Fatalf("unexpected spans: %#v", spans)
	}
}

func TestClickHouseClientListLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"timestamp":"2026-05-07 12:00:00","trace_id":"tr1","span_id":"sp1","severity_text":"INFO","severity_number":9,"body":"ready","resource_attributes":{"sandbox0.sandbox_id":"sb_1"}}
`))
	}))
	defer server.Close()

	client, err := NewClickHouseClient(ClickHouseConfig{
		HTTPURL:  server.URL,
		Database: "sandbox0_observability",
		Client:   server.Client(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	logs, err := client.ListLogs(context.Background(), ListOptions{SandboxID: "sb_1"})
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(logs) != 1 || logs[0].Body != "ready" || logs[0].ResourceAttributes["sandbox0.sandbox_id"] != "sb_1" {
		t.Fatalf("unexpected logs: %#v", logs)
	}
}
