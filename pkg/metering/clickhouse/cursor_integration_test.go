package clickhouse

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

// Run against a disposable ClickHouse 24.8+ instance. Every table belongs to
// the uniquely named database created and removed by this test.
func TestWindowCursorCandidatesClickHouseIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("SANDBOX0_CLICKHOUSE_INTEGRATION_DSN"))
	if dsn == "" {
		t.Skip("SANDBOX0_CLICKHOUSE_INTEGRATION_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cfg := Config{Database: fmt.Sprintf("sandbox0_metering_integration_%d", time.Now().UnixNano())}
	db, repo, err := Open(ctx, OpenConfig{DSN: dsn, Schema: cfg, Migrate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		defer db.Close()
		if _, err := db.ExecContext(cleanup, "DROP DATABASE "+quoteIdentifier(cfg.Database)); err != nil {
			t.Errorf("drop owned integration database: %v", err)
		}
	}()
	table := qualified(cfg.Database, DefaultWindowsTable)
	if _, err := db.ExecContext(ctx, "SYSTEM STOP MERGES "+table); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	assertStatus := func(sequence int64, recordedAt time.Time, id string) {
		t.Helper()
		want := ""
		if id != "" {
			var encodeErr error
			want, encodeErr = encodeCursor(recordedAt, "producer", id)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
		}
		status, statusErr := repo.GetStatus(ctx, "region")
		if statusErr != nil || status.LatestWindowSequence != sequence || status.LatestWindowCursor != want ||
			status.LatestEventSequence != 0 || status.LatestEventCursor != "" {
			t.Fatalf("canonical status = %+v, %v; want sequence %d and cursor %q", status, statusErr, sequence, want)
		}
	}
	assertStatus(0, time.Time{}, "")
	latestWindowCursor := func() (string, error) {
		status, statusErr := repo.readLedgerStatus(ctx, table, "window_id")
		if statusErr != nil {
			return "", statusErr
		}
		return status.cursor, nil
	}
	appendWindow := func(id, team, kind string, offset, sequence int64) {
		t.Helper()
		if err := repo.AppendWindow(ctx, &metering.Window{
			WindowID: id, TeamID: team, WindowType: kind, Producer: "producer", RegionID: "region",
			SubjectType: "sandbox", SubjectID: "sandbox", Sequence: sequence, Unit: "bytes",
			WindowStart: base, WindowEnd: base.Add(time.Second), Value: sequence,
			RecordedAt: base.Add(time.Duration(offset)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	appendWindow("changed-team", "team-a", "compute", 1, 1)
	appendWindow("changed-team", "team-b", "storage", 11, 2)
	appendWindow("changed-type", "team-a", "compute", 12, 3)
	appendWindow("changed-type", "team-a", "storage", 13, 4)
	assertStatus(4, base.Add(13), "changed-type")
	appendWindow("stale-replay", "team-a", "compute", 14, 5)
	appendWindow("stale-replay", "team-b", "storage", 2, 99)
	appendWindow("duplicate", "team-a", "compute", 15, 7)
	appendWindow("duplicate", "team-a", "compute", 15, 7)
	appendWindow("same-timestamp-a", "team-a", "compute", 15, 8)
	appendWindow("same-timestamp-b", "team-a", "compute", 15, 9)
	appendWindow("legacy-version", "team-a", "compute", 18, 10)
	// A historical version can win despite an earlier recorded_at. Filtering
	// timestamps before FINAL would resurrect the losing, newer-timestamp row.
	if _, err := db.ExecContext(ctx, "INSERT INTO "+table+" SELECT * REPLACE (toUInt64(10000000000000000000) AS version, fromUnixTimestamp64Nano(?, 'UTC') AS recorded_at) FROM "+table+" WHERE window_id = 'legacy-version'", base.Add(5).UnixNano()); err != nil {
		t.Fatal(err)
	}

	cursor, err := encodeCursor(base.Add(10), "producer", "cursor")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for range 5 {
		page, next, err := repo.ListTeamWindows(ctx, "team-a", "compute", cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		ids = append(ids, page[0].WindowID)
		if page[0].TeamID != "team-a" || page[0].WindowType != "compute" {
			t.Fatalf("filtered version leaked into results: %+v", page[0])
		}
		cursor = next
	}
	want := []string{"stale-replay", "duplicate", "same-timestamp-a", "same-timestamp-b"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("paged canonical IDs = %v, want %v", ids, want)
	}
	latest, err := latestWindowCursor()
	if err != nil || latest != cursor {
		t.Fatalf("latest cursor = %q, %v, want last canonical page %q", latest, err, cursor)
	}
	status, err := repo.GetStatus(ctx, "region")
	if err != nil || status.LatestWindowSequence != 10 {
		t.Fatalf("canonical status = %+v, %v", status, err)
	}
	assertStatus(10, base.Add(15), "same-timestamp-b")
	// Before the Unix epoch, versionFrom clamps both revisions to zero. The
	// candidate filtering must retain FINAL's last-inserted winner.
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
		t.Fatal(err)
	}
	base = time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)
	appendWindow("legacy", "team-a", "compute", 20, 1)
	appendWindow("legacy", "team-a", "compute", 10, 2)
	assertStatus(2, base.Add(10), "legacy")
	latest, err = latestWindowCursor()
	wantLegacy, encodeErr := encodeCursor(base.Add(10), "producer", "legacy")
	if err != nil || encodeErr != nil || latest != wantLegacy {
		t.Fatalf("legacy latest cursor = %q, %v, want %q", latest, err, wantLegacy)
	}
	legacyCursor, err := encodeCursor(base.Add(15), "producer", "cursor")
	if err != nil {
		t.Fatal(err)
	}
	page, next, err := repo.ListWindows(ctx, legacyCursor, 100)
	if err != nil || len(page) != 0 || next != "" {
		t.Fatalf("candidate filter resurrected a legacy version: %+v, %q, %v", page, next, err)
	}
}
