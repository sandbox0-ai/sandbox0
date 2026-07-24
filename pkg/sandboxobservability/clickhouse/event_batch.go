package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

type eventBatchInserter interface {
	InsertEventBatch(context.Context, string, [][]any) error
}

type eventBatchExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type sqlEventBatchInserter struct {
	db eventBatchExecer
}

func (i sqlEventBatchInserter) InsertEventBatch(ctx context.Context, query string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	columnCount := len(rows[0])
	if columnCount == 0 {
		return fmt.Errorf("event batch has no columns")
	}

	var builder strings.Builder
	builder.WriteString(query)
	builder.WriteString(" VALUES ")
	args := make([]any, 0, len(rows)*columnCount)
	for rowIndex, row := range rows {
		if len(row) != columnCount {
			return fmt.Errorf("event batch row %d has %d columns, want %d", rowIndex, len(row), columnCount)
		}
		if rowIndex > 0 {
			builder.WriteString(", ")
		}
		builder.WriteByte('(')
		for columnIndex := range row {
			if columnIndex > 0 {
				builder.WriteString(", ")
			}
			builder.WriteByte('?')
		}
		builder.WriteByte(')')
		args = append(args, row...)
	}

	// ClickHouse still acknowledges only after the shared async buffer has
	// flushed to storage. The short fixed window combines concurrent audit
	// writers into fewer remote parts without weakening canonical durability.
	batchContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithStdAsync(true),
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"async_insert_use_adaptive_busy_timeout": 0,
			"async_insert_busy_timeout_ms":           auditAsyncInsertBusyTimeoutMillis,
			"async_insert_max_query_number":          auditAsyncInsertMaxQueryNumber,
		}),
	)
	if _, err := i.db.ExecContext(batchContext, builder.String(), args...); err != nil {
		return fmt.Errorf("execute async event batch: %w", err)
	}
	return nil
}
