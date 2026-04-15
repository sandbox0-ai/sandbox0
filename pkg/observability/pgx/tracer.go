package pgx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// observableTracer implements pgx.QueryTracer for tracing, metrics, and logging
type observableTracer struct {
	config  AdapterConfig
	metrics *metrics
}

// TraceQueryStart implements pgx.QueryTracer
func (t *observableTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if t.config.Disabled {
		return ctx
	}

	start := time.Now()
	operation := inferOperation(data.SQL)

	// Track active queries
	if t.metrics != nil {
		t.metrics.activeQueries.WithLabelValues(operation).Inc()
	}

	// Start span
	spanName := fmt.Sprintf("pgx %s", operation)
	ctx, span := t.config.Tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", operation),
			attribute.String("db.statement", truncateSQL(data.SQL)),
		),
	)

	// Store span and start time in context for TraceQueryEnd
	ctx = context.WithValue(ctx, querySpanKey{}, span)
	ctx = context.WithValue(ctx, queryStartKey{}, start)
	ctx = context.WithValue(ctx, queryOperationKey{}, operation)

	if !t.config.DisableLogging && t.config.Logger != nil {
		t.config.Logger.Debug("PostgreSQL query started",
			zap.String("operation", operation),
			zap.String("sql", truncateSQL(data.SQL)),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
		)
	}

	return ctx
}

// TraceQueryEnd implements pgx.QueryTracer
func (t *observableTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if t.config.Disabled {
		return
	}

	// Retrieve span and metadata from context
	span, _ := ctx.Value(querySpanKey{}).(trace.Span)
	start, _ := ctx.Value(queryStartKey{}).(time.Time)
	operation, _ := ctx.Value(queryOperationKey{}).(string)

	if span == nil || start.IsZero() {
		return
	}
	defer span.End()

	duration := time.Since(start)

	// Track active queries
	if t.metrics != nil {
		t.metrics.activeQueries.WithLabelValues(operation).Dec()
	}

	if data.Err != nil {
		// Handle error
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())

		if t.metrics != nil {
			t.metrics.queriesTotal.WithLabelValues(operation, "error").Inc()
			t.metrics.queryDuration.WithLabelValues(operation).Observe(duration.Seconds())
		}

		if !t.config.DisableLogging && t.config.Logger != nil {
			t.config.Logger.Error("PostgreSQL query failed",
				zap.String("operation", operation),
				zap.Duration("duration", duration),
				zap.Error(data.Err),
				zap.String("trace_id", span.SpanContext().TraceID().String()),
			)
		}

		return
	}

	// Success case
	span.SetAttributes(
		attribute.Int64("db.rows_affected", data.CommandTag.RowsAffected()),
	)

	if t.metrics != nil {
		t.metrics.queriesTotal.WithLabelValues(operation, "success").Inc()
		t.metrics.queryDuration.WithLabelValues(operation).Observe(duration.Seconds())
		t.metrics.rowsAffected.WithLabelValues(operation).Add(float64(data.CommandTag.RowsAffected()))
	}

	if !t.config.DisableLogging && t.config.Logger != nil {
		t.config.Logger.Debug("PostgreSQL query completed",
			zap.String("operation", operation),
			zap.Duration("duration", duration),
			zap.Int64("rows_affected", data.CommandTag.RowsAffected()),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
		)
	}
}

// Context keys for storing span metadata
type querySpanKey struct{}
type queryStartKey struct{}
type queryOperationKey struct{}

// inferOperation extracts the SQL operation type from the query
func inferOperation(sql string) string {
	fields := strings.Fields(strings.TrimSpace(sql))
	if len(fields) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(fields[0])
}

// truncateSQL truncates SQL statements for logging/tracing
// to avoid huge span attributes
func truncateSQL(sql string) string {
	const maxLen = 200
	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen] + "..."
}
