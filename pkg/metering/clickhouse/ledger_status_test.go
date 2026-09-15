package clickhouse

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"
)

func statusBounds(count, sequence int64, stamp time.Time) *captureRows {
	return &captureRows{columns: []string{"count", "sequence", "recorded_at"}, values: [][]driver.Value{{count, sequence, stamp}}}
}

func statusCandidates(values ...[]driver.Value) *captureRows {
	return &captureRows{columns: []string{"sequence", "recorded_at", "region_id", "producer", "id", "version"}, values: values}
}

func statusAggregate(sequence int64, stamp time.Time, producer, id string) *captureRows {
	return &captureRows{columns: []string{"count", "sequence", "recorded_at", "producer", "id"}, values: [][]driver.Value{{int64(2), sequence, stamp, producer, id}}}
}

func TestLedgerStatusResolvesSequenceAndCursorFromDifferentCanonicalRows(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	stamp := time.Date(2026, 9, 1, 0, 0, 0, 123, time.UTC)
	values := [][]driver.Value{
		{int64(100), stamp.Add(-time.Second), "region", "producer-a", "sequence", int64(10)},
		{int64(3), stamp, "region", "producer-a", "z", int64(11)},
		{int64(4), stamp, "region", "producer-b", "a", int64(12)},
		{int64(5), stamp, "region", "producer-b", "b", int64(13)},
	}
	conn.queryRows = []*captureRows{statusBounds(1000000, 100, stamp), statusCandidates(values...), statusCandidates(values...)}
	got, err := repo.readLedgerStatus(context.Background(), "`database`.`windows`", "window_id")
	want, _ := encodeCursor(stamp, "producer-b", "b")
	if err != nil || got.sequence != 100 || got.cursor != want {
		t.Fatalf("status = %+v, %v; want sequence 100 and cursor %s", got, err, want)
	}
	if strings.Contains(conn.queries[0], "FINAL") || !strings.Contains(conn.queries[2], "FINAL\nWHERE (region_id, producer, window_id) IN") {
		t.Fatalf("status did not restrict canonical resolution to candidate identities: %v", conn.queries)
	}
	if strings.Contains(conn.queries[2], "recorded_at >=") || strings.Contains(conn.queries[2], "sequence >=") {
		t.Fatal("canonical resolution filtered out older versions before FINAL")
	}
}

func TestLedgerStatusFallsBackForUnprovenRawExtrema(t *testing.T) {
	stamp := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		canonical []driver.Value
	}{
		{"stale sequence", []driver.Value{int64(9), stamp, "region", "producer", "id", int64(11)}},
		{"legacy timestamp", []driver.Value{int64(99), stamp.Add(-time.Second), "region", "producer", "id", int64(100)}},
		{"unseen replacement", []driver.Value{int64(100), stamp.Add(time.Second), "region", "producer", "id", int64(12)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, conn := newCaptureRepository(t)
			conn.queryRows = []*captureRows{
				statusBounds(5000, 99, stamp),
				statusCandidates([]driver.Value{int64(99), stamp, "region", "producer", "id", int64(10)}),
				statusCandidates(test.canonical),
				statusAggregate(50, stamp.Add(-time.Second), "producer", "winner"),
			}
			got, err := repo.readLedgerStatus(context.Background(), "`database`.`windows`", "window_id")
			want, _ := encodeCursor(stamp.Add(-time.Second), "producer", "winner")
			if err != nil || got.sequence != 50 || got.cursor != want {
				t.Fatalf("fallback status = %+v, %v", got, err)
			}
			if len(conn.queries) != 4 || strings.Contains(conn.queries[3], "WHERE") {
				t.Fatalf("incomplete candidates did not fall back to the complete canonical ledger: %v", conn.queries)
			}
		})
	}
}

func TestLedgerStatusBoundsCandidateWork(t *testing.T) {
	stamp := time.Now().UTC()
	for _, test := range []struct {
		name           string
		count, idBytes int
	}{
		{"too many versions", 1025, 16},
		{"oversized predicate", 100, 512},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, conn := newCaptureRepository(t)
			candidates := statusCandidates()
			for i := range test.count {
				candidates.values = append(candidates.values, []driver.Value{int64(1), stamp, "region", "producer", fmt.Sprintf("%s%d", strings.Repeat("x", test.idBytes), i), int64(1)})
			}
			conn.queryRows = []*captureRows{statusBounds(1000000, 1, stamp), candidates, statusAggregate(1, stamp, "producer", "winner")}
			got, err := repo.readLedgerStatus(context.Background(), "`database`.`windows`", "window_id")
			if err != nil || got.sequence != 1 || len(conn.queries) != 3 {
				t.Fatalf("bounded fallback = %+v, %v; query count = %d", got, err, len(conn.queries))
			}
		})
	}
}

func TestLedgerStatusEmptyDoesNotPublishEpochCursor(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	conn.queryRows = []*captureRows{statusBounds(0, 0, time.Unix(0, 0))}
	got, err := repo.readLedgerStatus(context.Background(), "`database`.`windows`", "window_id")
	if err != nil || got.sequence != 0 || got.cursor != "" || len(conn.queries) != 1 {
		t.Fatalf("empty status = %+v, %v", got, err)
	}
}
