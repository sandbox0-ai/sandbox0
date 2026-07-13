package clickhouse

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type captureDB struct {
	execQuery string
	execArgs  []any
}

func (c *captureDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	c.execQuery = query
	c.execArgs = args
	return nil, nil
}

func (c *captureDB) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}

func mustRepository(t *testing.T) (*Repository, *captureDB) {
	t.Helper()
	db := &captureDB{}
	repo, err := NewRepository(db, Config{})
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	return repo, db
}

func TestInsertEventsBuildsBatchInsertAndSerializesAttributes(t *testing.T) {
	repo, db := mustRepository(t)
	now := time.Date(2026, 7, 1, 1, 2, 4, 987654321, time.UTC)
	repo.now = func() time.Time { return now }
	occurredAt := time.Date(2026, 7, 1, 1, 2, 3, 123456789, time.FixedZone("offset", 8*60*60))

	event := sandboxobservability.Event{
		EventID:       "11111111-1111-4111-8111-111111111111",
		SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
		TeamID:        "team-1",
		SandboxID:     "sb-1",
		RegionID:      "aws-us-east-1",
		ClusterID:     "cluster-a",
		OccurredAt:    occurredAt,
		Source:        sandboxobservability.SourceNetd,
		EventType:     sandboxobservability.EventTypeNetworkAudit,
		Phase:         sandboxobservability.EventPhaseEffect,
		Outcome:       sandboxobservability.OutcomeDenied,
		Actor:         sandboxobservability.AuditActor{Kind: sandboxobservability.ActorKindSandboxWorkload, ID: "sb-1"},
		Action:        "network.deny",
		Resource:      sandboxobservability.AuditResource{Type: "sandbox_network", ID: "sb-1"},
		OperationID:   "operation-1",
		Producer:      sandboxobservability.AuditProducer{Service: "netd", Instance: "node-1", Sequence: 1},
		Attributes:    map[string]any{"destination": "example.com"},
	}
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	if err := sandboxobservability.SignEvent(&event, key); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	err := repo.InsertEvents(context.Background(), []sandboxobservability.Event{event})
	if err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}
	if !strings.HasPrefix(db.execQuery, "INSERT INTO `sandbox0_observability`.`sandbox_audit_events`") {
		t.Fatalf("exec query = %s", db.execQuery)
	}
	if !strings.Contains(db.execQuery, ") SETTINGS async_insert = 0, wait_for_async_insert = 1 VALUES") {
		t.Fatalf("exec query must require a durable ClickHouse acknowledgement: %s", db.execQuery)
	}
	if strings.Count(db.execQuery, dateTime64NanoPlaceholder) != 2 {
		t.Fatalf("exec query must preserve both DateTime64 values at nanosecond precision: %s", db.execQuery)
	}
	if len(db.execArgs) != 38 {
		t.Fatalf("exec args count = %d, want 38", len(db.execArgs))
	}
	if db.execArgs[2] != "team-1" || db.execArgs[3] != "sb-1" {
		t.Fatalf("identity args = %#v", db.execArgs[2:4])
	}
	if got := db.execArgs[6]; got != dateTime64NanoArg(occurredAt) {
		t.Fatalf("occurred_at arg = %#v, want Unix nanoseconds", got)
	}
	if got := db.execArgs[7]; got != dateTime64NanoArg(now) {
		t.Fatalf("ingested_at arg = %#v, want Unix nanoseconds", got)
	}
	if attributes, ok := db.execArgs[33].(string); !ok || !strings.Contains(attributes, `"destination":"example.com"`) {
		t.Fatalf("attributes arg = %#v", db.execArgs[33])
	}
	if strings.Contains(auditEventSelectColumns, "cursor") || strings.Contains(auditEventSelectColumns, "watermark") {
		t.Fatalf("audit event storage columns contain query transport fields: %s", auditEventSelectColumns)
	}
}

func TestDateTime64NanoBindingPreservesPreEpochValues(t *testing.T) {
	value := time.Date(1960, time.July, 1, 1, 2, 3, 123456789, time.UTC)
	if dateTime64NanoPlaceholder != "fromUnixTimestamp64Nano(?, 'UTC')" {
		t.Fatalf("dateTime64NanoPlaceholder = %q", dateTime64NanoPlaceholder)
	}
	if got := dateTime64NanoArg(value); got != value.UnixNano() || got >= 0 {
		t.Fatalf("dateTime64NanoArg(%s) = %d, want negative Unix nanoseconds %d", value, got, value.UnixNano())
	}
}

func TestInsertEventsRejectsMutationOfSignedIdentityWhitespace(t *testing.T) {
	repo, db := mustRepository(t)
	event := sandboxobservability.Event{
		EventID:       "11111111-1111-4111-8111-111111111111",
		SchemaVersion: sandboxobservability.CurrentEventSchemaVersion,
		TeamID:        " team-1",
		SandboxID:     "sb-1",
		OccurredAt:    time.Now().UTC(),
		Source:        sandboxobservability.SourceNetd,
		EventType:     sandboxobservability.EventTypeNetworkAudit,
		Phase:         sandboxobservability.EventPhaseEffect,
		Actor:         sandboxobservability.AuditActor{Kind: sandboxobservability.ActorKindSandboxWorkload},
		Action:        "network.connect",
		Resource:      sandboxobservability.AuditResource{Type: "sandbox_network", ID: "sb-1"},
		Producer:      sandboxobservability.AuditProducer{Service: "netd"},
	}
	if err := repo.InsertEvents(context.Background(), []sandboxobservability.Event{event}); err == nil || !strings.Contains(err.Error(), "team_id must not contain surrounding whitespace") {
		t.Fatalf("InsertEvents() error = %v, want non-canonical signed field rejection", err)
	}
	if db.execQuery != "" {
		t.Fatalf("exec query = %q, want no insert", db.execQuery)
	}
}

func TestBuildListSQLAppliesTypedFiltersAndCursor(t *testing.T) {
	repo, _ := mustRepository(t)
	start := time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 1, 2, 0, 0, 0, time.UTC)
	cursorValue, err := encodePageCursor(sandboxobservability.Event{
		EventID:    "11111111-1111-4111-8111-111111111111",
		OccurredAt: time.Date(2026, 7, 1, 1, 30, 0, 0, time.UTC),
		IngestedAt: time.Date(2026, 7, 1, 1, 30, 1, 0, time.UTC),
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
		Integrity:  sandboxobservability.AuditIntegrity{PayloadHash: strings.Repeat("a", 64)},
	})
	if err != nil {
		t.Fatalf("encodePageCursor() error = %v", err)
	}

	query, limit, cursor, err := normalizeQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		StartTime: &start,
		EndTime:   &end,
		Limit:     10,
		Cursor:    cursorValue,
		Source:    sandboxobservability.SourceNetd,
		EventType: sandboxobservability.EventTypeNetworkAudit,
		Outcome:   sandboxobservability.OutcomeDenied,
	})
	if err != nil {
		t.Fatalf("normalizeQuery() error = %v", err)
	}

	sqlQuery, args := repo.buildListSQL(query, limit+1, cursor)
	for _, want := range []string{
		"FROM `sandbox0_observability`.`sandbox_audit_events` FINAL WHERE team_id = ? AND sandbox_id = ?",
		"source = ?",
		"outcome = ?",
		"event_type = ?",
		"(occurred_at, ingested_at, source, event_type, event_id, payload_hash) > (" + dateTime64NanoPlaceholder + ", " + dateTime64NanoPlaceholder + ", ?, ?, ?, ?)",
		"ORDER BY occurred_at ASC, ingested_at ASC, source ASC, event_type ASC, event_id ASC, payload_hash ASC LIMIT 11",
	} {
		if !strings.Contains(sqlQuery, want) {
			t.Fatalf("query missing %q:\n%s", want, sqlQuery)
		}
	}
	if len(args) != 13 {
		t.Fatalf("args count = %d, want 13: %#v", len(args), args)
	}
	if args[5] != string(sandboxobservability.EventTypeNetworkAudit) {
		t.Fatalf("event_type arg = %#v, want network_audit", args[5])
	}
}

func TestBuildWatchEventsSQLUsesIngestionOrderCursor(t *testing.T) {
	repo, _ := mustRepository(t)
	start := time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC)
	after := time.Date(2026, 7, 1, 1, 30, 1, 0, time.UTC)
	query, limit, cursor, err := normalizeWatchEventQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		StartTime: &start,
		Limit:     10,
		Source:    sandboxobservability.SourceNetd,
		EventType: sandboxobservability.EventTypeNetworkAudit,
	}, sandboxobservability.WatchOptions{
		AfterIngestedAt: &after,
	})
	if err != nil {
		t.Fatalf("normalizeWatchEventQuery() error = %v", err)
	}

	sqlQuery, args := repo.buildWatchEventsSQL(query, limit, cursor)
	for _, want := range []string{
		"occurred_at >= " + dateTime64NanoPlaceholder,
		"source = ?",
		"event_type = ?",
		"(ingested_at, source, event_type, event_id, payload_hash) > (" + dateTime64NanoPlaceholder + ", ?, ?, ?, ?)",
		"ORDER BY ingested_at ASC, source ASC, event_type ASC, event_id ASC, payload_hash ASC LIMIT 10",
	} {
		if !strings.Contains(sqlQuery, want) {
			t.Fatalf("query missing %q:\n%s", want, sqlQuery)
		}
	}
	if strings.Contains(sqlQuery, "ORDER BY occurred_at") {
		t.Fatalf("watch query must not order by occurred_at:\n%s", sqlQuery)
	}
	if got := args[len(args)-5]; got != dateTime64NanoArg(after) {
		t.Fatalf("tail ingested_at arg = %#v, want %s", args[len(args)-5], after)
	}
}

func TestNormalizeQueryCapsLimitAndRejectsInvalidCursor(t *testing.T) {
	query, limit, _, err := normalizeQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		Limit:     5000,
	})
	if err != nil {
		t.Fatalf("normalizeQuery() error = %v", err)
	}
	if query.Limit != 5000 {
		t.Fatalf("query limit should remain caller value for observability, got %d", query.Limit)
	}
	if limit != MaxQueryLimit {
		t.Fatalf("limit = %d, want %d", limit, MaxQueryLimit)
	}

	if _, _, _, err := normalizeQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		Cursor:    "bad",
	}); err == nil {
		t.Fatal("normalizeQuery() error = nil, want invalid cursor error")
	}
}

func TestNormalizeQueryRejectsDateTime64NanoOverflow(t *testing.T) {
	tests := []struct {
		name  string
		query sandboxobservability.EventQuery
		field string
	}{
		{
			name: "start before minimum",
			query: sandboxobservability.EventQuery{
				StartTime: timePointer(time.Date(1899, time.December, 31, 23, 59, 59, 999999999, time.UTC)),
			},
			field: "start_time",
		},
		{
			name: "end after maximum",
			query: sandboxobservability.EventQuery{
				EndTime: timePointer(time.Date(2300, time.January, 1, 0, 0, 0, 0, time.UTC)),
			},
			field: "end_time",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.query.TeamID = "team-1"
			tt.query.SandboxID = "sb-1"
			_, _, _, err := normalizeQuery(tt.query)
			if !errors.Is(err, sandboxobservability.ErrInvalidQuery) || !strings.Contains(err.Error(), tt.field+" is outside the DateTime64(9) range") {
				t.Fatalf("normalizeQuery() error = %v, want ErrInvalidQuery for %s", err, tt.field)
			}
		})
	}
}

func TestNormalizeEventCursorsRejectDateTime64NanoOverflow(t *testing.T) {
	outside := time.Date(1899, time.December, 31, 23, 59, 59, 0, time.UTC)
	page, err := encodePageCursor(sandboxobservability.Event{
		EventID:    "11111111-1111-4111-8111-111111111111",
		OccurredAt: outside,
		IngestedAt: time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
	})
	if err != nil {
		t.Fatalf("encodePageCursor() error = %v", err)
	}
	_, _, _, err = normalizeQuery(sandboxobservability.EventQuery{
		TeamID: "team-1", SandboxID: "sb-1", Cursor: page,
	})
	if !errors.Is(err, sandboxobservability.ErrInvalidCursor) {
		t.Fatalf("normalizeQuery() error = %v, want ErrInvalidCursor", err)
	}

	tail, err := encodeTailCursor(eventTailCursorKind, outside, string(sandboxobservability.SourceNetd), string(sandboxobservability.EventTypeNetworkAudit), "11111111-1111-4111-8111-111111111111", "")
	if err != nil {
		t.Fatalf("encodeTailCursor() error = %v", err)
	}
	_, _, _, err = normalizeWatchEventQuery(sandboxobservability.EventQuery{
		TeamID: "team-1", SandboxID: "sb-1",
	}, sandboxobservability.WatchOptions{Cursor: tail})
	if !errors.Is(err, sandboxobservability.ErrInvalidCursor) {
		t.Fatalf("normalizeWatchEventQuery() error = %v, want ErrInvalidCursor", err)
	}
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func TestNormalizeQueryRejectsEventIDWithOtherFilters(t *testing.T) {
	_, _, _, err := normalizeQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		EventID:   "11111111-1111-4111-8111-111111111111",
		Action:    "sandbox.pause",
	})
	if err == nil || !strings.Contains(err.Error(), "event_id cannot be combined") {
		t.Fatalf("normalizeQuery() error = %v, want incompatible filter error", err)
	}
}

func TestNormalizeQueryBoundsExactEventLookupToTwoPayloadVariants(t *testing.T) {
	_, limit, _, err := normalizeQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		EventID:   "11111111-1111-4111-8111-111111111111",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("normalizeQuery() error = %v", err)
	}
	if limit != 2 {
		t.Fatalf("limit = %d, want 2", limit)
	}
}

func TestNormalizeWatchEventQueryRejectsEventID(t *testing.T) {
	_, _, _, err := normalizeWatchEventQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		EventID:   "11111111-1111-4111-8111-111111111111",
	}, sandboxobservability.WatchOptions{})
	if err == nil || !strings.Contains(err.Error(), "event_id cannot be combined with watch") {
		t.Fatalf("normalizeWatchEventQuery() error = %v, want exact watch error", err)
	}
}
