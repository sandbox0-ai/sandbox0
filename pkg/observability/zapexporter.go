package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// zapSpanExporter exports spans using zap logger in JSON format
type zapSpanExporter struct {
	logger *zap.Logger
}

// newZapSpanExporter creates a new zap-based span exporter
func newZapSpanExporter(logger *zap.Logger) sdktrace.SpanExporter {
	return &zapSpanExporter{
		logger: logger,
	}
}

// ExportSpans exports spans to zap logger
func (e *zapSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, span := range spans {
		e.logSpan(span)
	}
	return nil
}

// Shutdown shuts down the exporter
func (e *zapSpanExporter) Shutdown(ctx context.Context) error {
	return nil
}

// logSpan logs a single span using zap logger
func (e *zapSpanExporter) logSpan(span sdktrace.ReadOnlySpan) {
	fields := []zap.Field{
		zap.String("trace_id", span.SpanContext().TraceID().String()),
		zap.String("span_id", span.SpanContext().SpanID().String()),
		zap.String("parent_span_id", span.Parent().SpanID().String()),
		zap.String("span_name", span.Name()),
		zap.String("span_kind", span.SpanKind().String()),
		zap.Time("start_time", span.StartTime()),
		zap.Time("end_time", span.EndTime()),
		zap.Duration("duration", span.EndTime().Sub(span.StartTime())),
		zap.String("status_code", span.Status().Code.String()),
	}

	// Add status description if present
	if span.Status().Description != "" {
		fields = append(fields, zap.String("status_description", span.Status().Description))
	}

	// Add resource attributes
	if resource := span.Resource(); resource != nil {
		for _, attr := range resource.Attributes() {
			fields = append(fields, attributeToZapField("resource", attr))
		}
	}

	// Add span attributes
	for _, attr := range span.Attributes() {
		fields = append(fields, attributeToZapField("attr", attr))
	}

	// Add events
	for _, event := range span.Events() {
		eventFields := []zap.Field{
			zap.String("event_name", event.Name),
			zap.Time("event_time", event.Time),
		}
		for _, attr := range event.Attributes {
			eventFields = append(eventFields, attributeToZapField("event", attr))
		}
		fields = append(fields, zap.Any("event", eventFields))
	}

	e.logger.Info("trace_span", fields...)
}

// attributeToZapField converts an OTEL attribute to a zap field
func attributeToZapField(prefix string, attr attribute.KeyValue) zap.Field {
	key := prefix + "." + string(attr.Key)

	switch attr.Value.Type() {
	case attribute.BOOL:
		return zap.Bool(key, attr.Value.AsBool())
	case attribute.INT64:
		return zap.Int64(key, attr.Value.AsInt64())
	case attribute.FLOAT64:
		return zap.Float64(key, attr.Value.AsFloat64())
	case attribute.STRING:
		return zap.String(key, attr.Value.AsString())
	case attribute.BOOLSLICE:
		return zap.Any(key, attr.Value.AsBoolSlice())
	case attribute.INT64SLICE:
		return zap.Any(key, attr.Value.AsInt64Slice())
	case attribute.FLOAT64SLICE:
		return zap.Any(key, attr.Value.AsFloat64Slice())
	case attribute.STRINGSLICE:
		return zap.Strings(key, attr.Value.AsStringSlice())
	default:
		return zap.Any(key, attr.Value.AsInterface())
	}
}
