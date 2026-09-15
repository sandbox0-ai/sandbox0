package clickhouse

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWindowCandidatesBoundCardinalityAndSQLSize(t *testing.T) {
	stamp := time.Now().UTC()
	for _, test := range []struct {
		name           string
		count, idBytes int
	}{
		{"too many versions", 1025, 16},
		{"oversized identifiers", 100, 512},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, conn := newCaptureRepository(t)
			rows := &captureRows{columns: []string{"recorded_at", "region_id", "producer", "window_id", "version"}}
			for index := range test.count {
				rows.values = append(rows.values, []driver.Value{stamp, "region", "producer", fmt.Sprintf("%s-%d", strings.Repeat("w", test.idBytes), index), int64(index + 1)})
			}
			conn.queryRows = []*captureRows{rows}
			cursor := &pageCursor{RecordedAt: stamp, Producer: "producer", ID: "cursor"}
			where, args := windowWhere("", "", cursor)
			candidates, err := repo.windowCandidates(context.Background(), where, args, cursor)
			if err != nil || candidates != nil {
				t.Fatalf("oversized candidate set = %+v, %v; want canonical fallback", candidates, err)
			}
		})
	}
}

func TestWindowCandidatesRejectUnseenConcurrentReplacement(t *testing.T) {
	repo, conn := newCaptureRepository(t)
	stamp := time.Now().UTC()
	columns := []string{"recorded_at", "region_id", "producer", "window_id", "version"}
	conn.queryRows = []*captureRows{
		{columns: columns, values: [][]driver.Value{{stamp, "region", "producer", "window", int64(10)}}},
		{columns: columns, values: [][]driver.Value{{stamp.Add(time.Nanosecond), "region", "producer", "window", int64(11)}}},
	}
	cursor, err := encodeCursor(stamp.Add(-time.Second), "producer", "cursor")
	if err != nil {
		t.Fatal(err)
	}
	windows, next, err := repo.ListWindows(context.Background(), cursor, 100)
	if err == nil || !strings.Contains(err.Error(), "changed") || len(windows) != 0 || next != "" {
		t.Fatalf("concurrent replacement advanced page: windows=%+v next=%q error=%v", windows, next, err)
	}
}
