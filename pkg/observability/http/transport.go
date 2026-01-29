package http

import (
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// observableTransport wraps an http.RoundTripper with tracing, metrics, and logging
type observableTransport struct {
	base    http.RoundTripper
	config  AdapterConfig
	metrics *metrics
}

// RoundTrip implements http.RoundTripper
func (t *observableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Skip if disabled
	if t.config.Disabled {
		return t.base.RoundTrip(req)
	}

	ctx := req.Context()
	start := time.Now()

	method := req.Method
	host := req.URL.Host
	if host == "" {
		host = req.Host
	}

	// Track active requests
	if t.metrics != nil {
		t.metrics.activeRequests.WithLabelValues(method, host).Inc()
		defer t.metrics.activeRequests.WithLabelValues(method, host).Dec()
	}

	// Start span
	spanName := fmt.Sprintf("HTTP %s %s", method, req.URL.Path)
	ctx, span := t.config.Tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.method", method),
			attribute.String("http.url", req.URL.String()),
			attribute.String("http.host", host),
			attribute.String("http.scheme", req.URL.Scheme),
			attribute.String("http.target", req.URL.Path),
		),
	)
	defer span.End()

	// Inject trace context into HTTP headers
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	// Log request start
	t.config.Logger.Debug("HTTP request started",
		zap.String("method", method),
		zap.String("url", req.URL.String()),
		zap.String("host", host),
		zap.String("trace_id", span.SpanContext().TraceID().String()),
		zap.String("span_id", span.SpanContext().SpanID().String()),
	)

	// Record request size
	if t.metrics != nil && req.ContentLength > 0 {
		t.metrics.requestSize.WithLabelValues(method, host).Observe(float64(req.ContentLength))
	}

	// Execute request
	req = req.WithContext(ctx)
	resp, err := t.base.RoundTrip(req)

	duration := time.Since(start)

	if err != nil {
		// Handle error
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		if t.metrics != nil {
			t.metrics.requestsTotal.WithLabelValues(method, host, "error").Inc()
			t.metrics.requestDuration.WithLabelValues(method, host).Observe(duration.Seconds())
		}

		t.config.Logger.Error("HTTP request failed",
			zap.String("method", method),
			zap.String("url", req.URL.String()),
			zap.Duration("duration", duration),
			zap.Error(err),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
		)

		return nil, err
	}

	// Success case
	statusCode := resp.StatusCode
	statusClass := fmt.Sprintf("%dxx", statusCode/100)

	// Record span attributes
	span.SetAttributes(
		attribute.Int("http.status_code", statusCode),
	)
	if resp.ContentLength > 0 {
		span.SetAttributes(attribute.Int64("http.response.body.size", resp.ContentLength))
	}
	if statusCode >= 400 {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
	}

	// Record metrics
	if t.metrics != nil {
		t.metrics.requestsTotal.WithLabelValues(method, host, statusClass).Inc()
		t.metrics.requestDuration.WithLabelValues(method, host).Observe(duration.Seconds())
		if resp.ContentLength > 0 {
			t.metrics.responseSize.WithLabelValues(method, host).Observe(float64(resp.ContentLength))
		}
	}

	// Log response
	logLevel := zap.DebugLevel
	if statusCode >= 500 {
		logLevel = zap.ErrorLevel
	} else if statusCode >= 400 {
		logLevel = zap.WarnLevel
	}

	t.config.Logger.Log(logLevel, "HTTP request completed",
		zap.String("method", method),
		zap.String("url", req.URL.String()),
		zap.Int("status", statusCode),
		zap.Duration("duration", duration),
		zap.Int64("response_size", resp.ContentLength),
		zap.String("trace_id", span.SpanContext().TraceID().String()),
	)

	return resp, nil
}
