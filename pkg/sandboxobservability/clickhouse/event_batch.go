package clickhouse

import (
	"context"
	"database/sql"
	"fmt"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

type eventBatchInserter interface {
	InsertEventBatch(context.Context, string, [][]any) error
}

type eventBatchDB interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type sqlEventBatchInserter struct {
	db eventBatchDB
}

func (i sqlEventBatchInserter) InsertEventBatch(ctx context.Context, query string, rows [][]any) error {
	batchContext := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
		"async_insert":          0,
		"wait_for_async_insert": 1,
	}))
	tx, err := i.db.BeginTx(batchContext, nil)
	if err != nil {
		return fmt.Errorf("begin event batch: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.PrepareContext(batchContext, query)
	if err != nil {
		return fmt.Errorf("prepare event batch: %w", err)
	}
	defer statement.Close()
	for index, row := range rows {
		if _, err := statement.ExecContext(batchContext, row...); err != nil {
			return fmt.Errorf("append event batch row %d: %w", index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event batch: %w", err)
	}
	committed = true
	return nil
}
