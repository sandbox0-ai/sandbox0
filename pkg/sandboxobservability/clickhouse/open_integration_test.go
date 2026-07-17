package clickhouse

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

// TestCanonicalAuditClickHouseIntegration validates the real ClickHouse
// metadata representation, nanosecond round trip, and query-level override of
// an unsafe asynchronous-insert session. Set SANDBOX0_CLICKHOUSE_INTEGRATION_DSN
// to run it against a disposable ClickHouse 24.8+ instance.
func TestCanonicalAuditClickHouseIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("SANDBOX0_CLICKHOUSE_INTEGRATION_DSN"))
	if dsn == "" {
		t.Skip("SANDBOX0_CLICKHOUSE_INTEGRATION_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	database := fmt.Sprintf("sandbox0_audit_integration_%d", time.Now().UnixNano())
	cfg := Config{
		Database:                    database,
		RetentionDays:               2,
		LogsRetentionDays:           1,
		RuntimeSamplesRetentionDays: 1,
	}
	cleanupDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open ClickHouse cleanup connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		defer cleanupDB.Close()
		if _, dropErr := cleanupDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+quoteIdentifier(database)); dropErr != nil {
			t.Logf("drop integration database: %v", dropErr)
		}
	})
	db, _, err := Open(ctx, OpenConfig{
		DSN:                dsn,
		Schema:             cfg,
		Migrate:            true,
		RequireAuditSchema: true,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, statement := range []string{
		"SET async_insert = 1",
		"SET wait_for_async_insert = 0",
		"SET async_insert_use_adaptive_busy_timeout = 0",
		"SET async_insert_busy_timeout_ms = 60000",
	} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatalf("configure unsafe async insert session with %q: %v", statement, err)
		}
	}

	repo, err := NewRepository(conn, cfg)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	occurredAt := time.Date(1960, time.July, 1, 1, 2, 3, 123456789, time.UTC)
	ingestedAt := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Second).Add(987654321 * time.Nanosecond)
	eventID := "11111111-1111-4111-8111-111111111111"
	parentEventID := "22222222-2222-4222-8222-222222222222"
	event := sandboxobservability.Event{
		EventID:       eventID,
		SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
		TeamID:        "team-integration",
		SandboxID:     "sandbox-integration",
		RegionID:      "region-integration",
		ClusterID:     "cluster-integration",
		OccurredAt:    occurredAt,
		IngestedAt:    ingestedAt,
		Source:        sandboxobservability.SourceClusterGateway,
		EventType:     sandboxobservability.EventTypeAPIAccess,
		Phase:         sandboxobservability.EventPhaseResult,
		Outcome:       sandboxobservability.OutcomeSucceeded,
		Actor: sandboxobservability.AuditActor{
			Kind:       sandboxobservability.ActorKindService,
			ID:         "integration-test",
			UserID:     "user-integration",
			APIKeyID:   "key-integration",
			AuthMethod: "internal_token",
		},
		Action:        "sandbox.read",
		Resource:      sandboxobservability.AuditResource{Type: "sandbox", ID: "sandbox-integration", Subresource: "state"},
		OperationID:   "operation-integration",
		ParentEventID: parentEventID,
		Producer:      sandboxobservability.AuditProducer{Service: "cluster-gateway", Instance: "integration-test", Sequence: 1},
		Request: sandboxobservability.AuditRequest{
			RequestID:  "request-integration",
			TraceID:    "trace-integration",
			SourceIP:   "192.0.2.1",
			UserAgent:  "sandbox0-integration-test",
			HTTPMethod: "GET",
			Route:      "/sandboxes/{sandbox_id}",
			StatusCode: 200,
		},
		Attributes: map[string]any{"test": "audit-row-round-trip"},
	}
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	if err := sandboxobservability.SignEvent(&event, key); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	if err := repo.InsertEvents(ctx, []sandboxobservability.Event{event}); err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}

	var gotOccurredAt, gotIngestedAt time.Time
	if err := conn.QueryRowContext(ctx,
		"SELECT occurred_at, ingested_at FROM "+repo.eventsTable+" FINAL WHERE event_id = ?",
		eventID,
	).Scan(&gotOccurredAt, &gotIngestedAt); err != nil {
		t.Fatalf("query inserted audit event: %v", err)
	}
	if gotOccurredAt.UnixNano() != occurredAt.UnixNano() {
		t.Fatalf("occurred_at UnixNano = %d, want %d", gotOccurredAt.UnixNano(), occurredAt.UnixNano())
	}
	if gotIngestedAt.UnixNano() != ingestedAt.UnixNano() {
		t.Fatalf("ingested_at UnixNano = %d, want %d", gotIngestedAt.UnixNano(), ingestedAt.UnixNano())
	}

	startTime := occurredAt.Add(-time.Nanosecond)
	endTime := occurredAt.Add(time.Nanosecond)
	result, err := repo.ListEvents(ctx, sandboxobservability.EventQuery{
		TeamID:    event.TeamID,
		SandboxID: event.SandboxID,
		StartTime: &startTime,
		EndTime:   &endTime,
	})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(result.Events) != 1 {
		t.Fatalf("ListEvents() event count = %d, want 1", len(result.Events))
	}
	got := result.Events[0]
	if got.EventID != eventID || got.Source != event.Source || got.EventType != event.EventType {
		t.Fatalf("ListEvents() event identity = %#v, want event_id/source/event_type from inserted event", got)
	}
	if got.OccurredAt.UnixNano() != occurredAt.UnixNano() || got.IngestedAt.UnixNano() != ingestedAt.UnixNano() {
		t.Fatalf("ListEvents() timestamps = (%s, %s), want (%s, %s)", got.OccurredAt, got.IngestedAt, occurredAt, ingestedAt)
	}
	if result.Watermark != eventID {
		t.Fatalf("ListEvents() watermark = %q, want event ID", result.Watermark)
	}
	if got.Actor != event.Actor || got.Resource != event.Resource || got.OperationID != event.OperationID || got.ParentEventID != parentEventID {
		t.Fatalf("ListEvents() actor/resource correlation did not round trip: %#v", got)
	}
	if got.Producer != event.Producer || got.Request != event.Request {
		t.Fatalf("ListEvents() producer/request did not round trip: %#v", got)
	}
	if value, ok := got.Attributes["test"].(string); !ok || value != "audit-row-round-trip" {
		t.Fatalf("ListEvents() attributes = %#v, want test marker", got.Attributes)
	}
}
