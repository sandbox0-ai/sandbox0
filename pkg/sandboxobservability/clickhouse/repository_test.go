package clickhouse

import (
	"context"
	"database/sql"
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
	now := time.Date(2026, 7, 1, 1, 2, 4, 0, time.UTC)
	repo.now = func() time.Time { return now }
	occurredAt := time.Date(2026, 7, 1, 1, 2, 3, 0, time.FixedZone("offset", 8*60*60))

	err := repo.InsertEvents(context.Background(), []sandboxobservability.Event{{
		TeamID:     " team-1 ",
		SandboxID:  " sb-1 ",
		RegionID:   "aws-us-east-1",
		ClusterID:  "cluster-a",
		OccurredAt: occurredAt,
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
		Outcome:    sandboxobservability.OutcomeDenied,
		Cursor:     "netd:1",
		Watermark:  "netd:1",
		Attributes: map[string]any{"destination": "example.com"},
	}})
	if err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}
	if !strings.HasPrefix(db.execQuery, "INSERT INTO `sandbox0_observability`.`sandbox_events`") {
		t.Fatalf("exec query = %s", db.execQuery)
	}
	if len(db.execArgs) != 12 {
		t.Fatalf("exec args count = %d, want 12", len(db.execArgs))
	}
	if db.execArgs[0] != "team-1" || db.execArgs[1] != "sb-1" {
		t.Fatalf("identity args = %#v", db.execArgs[:2])
	}
	if got, ok := db.execArgs[4].(time.Time); !ok || got.Location() != time.UTC || !got.Equal(occurredAt) {
		t.Fatalf("occurred_at arg = %#v", db.execArgs[4])
	}
	if got, ok := db.execArgs[5].(time.Time); !ok || !got.Equal(now) {
		t.Fatalf("ingested_at arg = %#v", db.execArgs[5])
	}
	if attributes, ok := db.execArgs[11].(string); !ok || !strings.Contains(attributes, `"destination":"example.com"`) {
		t.Fatalf("attributes arg = %#v", db.execArgs[11])
	}
}

func TestInsertEventsRequiresProducerCursor(t *testing.T) {
	repo, db := mustRepository(t)
	err := repo.InsertEvents(context.Background(), []sandboxobservability.Event{{
		TeamID:     "team-1",
		SandboxID:  "sb-1",
		OccurredAt: time.Now(),
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
	}})
	if err == nil {
		t.Fatal("InsertEvents() error = nil, want missing cursor error")
	}
	if db.execQuery != "" {
		t.Fatalf("exec query = %q, want no insert", db.execQuery)
	}
}

func TestBuildListSQLAppliesTypedFiltersAuditScopeAndCursor(t *testing.T) {
	repo, _ := mustRepository(t)
	start := time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 1, 2, 0, 0, 0, time.UTC)
	cursorValue, err := encodePageCursor(sandboxobservability.Event{
		OccurredAt: time.Date(2026, 7, 1, 1, 30, 0, 0, time.UTC),
		IngestedAt: time.Date(2026, 7, 1, 1, 30, 1, 0, time.UTC),
		Source:     sandboxobservability.SourceNetd,
		EventType:  sandboxobservability.EventTypeNetworkAudit,
		Cursor:     "netd:10",
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
		Outcome:   sandboxobservability.OutcomeDenied,
		AuditOnly: true,
	})
	if err != nil {
		t.Fatalf("normalizeQuery() error = %v", err)
	}

	sqlQuery, args := repo.buildListSQL(query, limit+1, cursor)
	for _, want := range []string{
		"FROM `sandbox0_observability`.`sandbox_events` FINAL WHERE team_id = ? AND sandbox_id = ?",
		"source = ?",
		"outcome = ?",
		"event_type = ?",
		"(occurred_at, ingested_at, source, event_type, cursor) > (?, ?, ?, ?, ?)",
		"ORDER BY occurred_at ASC, ingested_at ASC, source ASC, event_type ASC, cursor ASC LIMIT 11",
	} {
		if !strings.Contains(sqlQuery, want) {
			t.Fatalf("query missing %q:\n%s", want, sqlQuery)
		}
	}
	if len(args) != 12 {
		t.Fatalf("args count = %d, want 12: %#v", len(args), args)
	}
	if args[6] != string(sandboxobservability.EventTypeNetworkAudit) {
		t.Fatalf("audit event_type arg = %#v, want network_audit", args[6])
	}
}

func TestAuditListSQLExcludesRuntimeStats(t *testing.T) {
	repo, _ := mustRepository(t)
	query, limit, cursor, err := normalizeQuery(sandboxobservability.EventQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		EventType: sandboxobservability.EventTypeRuntimeStats,
		AuditOnly: true,
	})
	if err != nil {
		t.Fatalf("normalizeQuery() error = %v", err)
	}

	sqlQuery, _ := repo.buildListSQL(query, limit+1, cursor)
	if !strings.Contains(sqlQuery, "event_type = ?") || !strings.Contains(sqlQuery, "AND 0 = 1") {
		t.Fatalf("audit runtime_stats query should be impossible:\n%s", sqlQuery)
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
		"occurred_at >= ?",
		"source = ?",
		"event_type = ?",
		"(ingested_at, source, event_type, cursor) > (?, ?, ?, ?)",
		"ORDER BY ingested_at ASC, source ASC, event_type ASC, cursor ASC LIMIT 10",
	} {
		if !strings.Contains(sqlQuery, want) {
			t.Fatalf("query missing %q:\n%s", want, sqlQuery)
		}
	}
	if strings.Contains(sqlQuery, "ORDER BY occurred_at") {
		t.Fatalf("watch query must not order by occurred_at:\n%s", sqlQuery)
	}
	if got, ok := args[len(args)-4].(time.Time); !ok || !got.Equal(after) {
		t.Fatalf("tail ingested_at arg = %#v, want %s", args[len(args)-4], after)
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
