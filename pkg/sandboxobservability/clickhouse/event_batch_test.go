package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type captureEventBatchDB struct {
	query string
	args  []any
	err   error
}

func (d *captureEventBatchDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	d.query = query
	d.args = args
	return nil, d.err
}

func TestSQLEventBatchInserterBuildsOneMultiRowInsert(t *testing.T) {
	db := &captureEventBatchDB{}
	inserter := sqlEventBatchInserter{db: db}

	err := inserter.InsertEventBatch(
		context.Background(),
		"INSERT INTO audit (event_id, action)",
		[][]any{
			{"event-1", "sandbox.create"},
			{"event-2", "process.create"},
		},
	)
	if err != nil {
		t.Fatalf("InsertEventBatch() error = %v", err)
	}
	if db.query != "INSERT INTO audit (event_id, action) VALUES (?, ?), (?, ?)" {
		t.Fatalf("query = %q", db.query)
	}
	if len(db.args) != 4 || db.args[0] != "event-1" || db.args[3] != "process.create" {
		t.Fatalf("args = %#v", db.args)
	}
}

func TestSQLEventBatchInserterRejectsRaggedRows(t *testing.T) {
	db := &captureEventBatchDB{}
	inserter := sqlEventBatchInserter{db: db}

	err := inserter.InsertEventBatch(
		context.Background(),
		"INSERT INTO audit (event_id, action)",
		[][]any{
			{"event-1", "sandbox.create"},
			{"event-2"},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "row 1 has 1 columns, want 2") {
		t.Fatalf("InsertEventBatch() error = %v", err)
	}
	if db.query != "" {
		t.Fatalf("query = %q, want no insert", db.query)
	}
}

func TestSQLEventBatchInserterReturnsAmbiguousInsertFailure(t *testing.T) {
	db := &captureEventBatchDB{err: errors.New("flush outcome unknown")}
	inserter := sqlEventBatchInserter{db: db}

	err := inserter.InsertEventBatch(
		context.Background(),
		"INSERT INTO audit (event_id)",
		[][]any{{"event-1"}},
	)
	if err == nil || !strings.Contains(err.Error(), "execute async event batch: flush outcome unknown") {
		t.Fatalf("InsertEventBatch() error = %v", err)
	}
}
