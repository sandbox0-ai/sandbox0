package k8s

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// observableTransport wraps an http.RoundTripper for Kubernetes API calls
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

	// Extract K8s-specific info from URL
	verb := req.Method
	resource := extractK8sResource(req.URL.Path)

	// Track active requests
	if t.metrics != nil {
		t.metrics.activeRequests.WithLabelValues(verb, resource).Inc()
		defer t.metrics.activeRequests.WithLabelValues(verb, resource).Dec()
	}

	// Start span
	spanName := fmt.Sprintf("K8s %s %s", verb, resource)
	ctx, span := t.config.Tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("k8s.verb", verb),
			attribute.String("k8s.resource", resource),
			attribute.String("k8s.path", req.URL.Path),
			attribute.String("component", "k8s-client"),
		),
	)
	defer span.End()

	// Inject trace context
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	if !t.config.DisableLogging && t.config.Logger != nil {
		t.config.Logger.Debug("K8s API request started",
			zap.String("verb", verb),
			zap.String("resource", resource),
			zap.String("path", req.URL.Path),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
		)
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
			t.metrics.requestsTotal.WithLabelValues(verb, resource, "error").Inc()
			t.metrics.requestDuration.WithLabelValues(verb, resource).Observe(duration.Seconds())
		}

		if !t.config.DisableLogging && t.config.Logger != nil {
			t.config.Logger.Error("K8s API request failed",
				zap.String("verb", verb),
				zap.String("resource", resource),
				zap.Duration("duration", duration),
				zap.Error(err),
				zap.String("trace_id", span.SpanContext().TraceID().String()),
			)
		}

		return nil, err
	}

	// Success case
	statusCode := resp.StatusCode
	statusClass := fmt.Sprintf("%dxx", statusCode/100)

	// Record span attributes
	span.SetAttributes(
		attribute.Int("http.status_code", statusCode),
	)
	if statusCode >= 400 {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
	}

	// Record metrics
	if t.metrics != nil {
		t.metrics.requestsTotal.WithLabelValues(verb, resource, statusClass).Inc()
		t.metrics.requestDuration.WithLabelValues(verb, resource).Observe(duration.Seconds())
	}

	// Log response
	logLevel := zap.DebugLevel
	if statusCode >= 500 {
		logLevel = zap.ErrorLevel
	} else if statusCode >= 400 {
		logLevel = zap.WarnLevel
	}

	if !t.config.DisableLogging && t.config.Logger != nil {
		t.config.Logger.Log(logLevel, "K8s API request completed",
			zap.String("verb", verb),
			zap.String("resource", resource),
			zap.Int("status", statusCode),
			zap.Duration("duration", duration),
			zap.String("trace_id", span.SpanContext().TraceID().String()),
		)
	}

	return resp, nil
}

// extractK8sResource extracts the resource type from a K8s API path
// e.g., "/api/v1/namespaces/default/pods" -> "pods"
// e.g., "/apis/apps/v1/deployments" -> "deployments"
func extractK8sResource(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")

	// Typical patterns:
	// /api/v1/namespaces/{namespace}/{resource}
	// /apis/{group}/{version}/namespaces/{namespace}/{resource}
	// /api/v1/{resource}

	if len(parts) == 0 {
		return "unknown"
	}

	for i, part := range parts {
		if part == "namespaces" && i+2 < len(parts) {
			return parts[i+2]
		}
	}

	if len(parts) >= 3 && parts[0] == "api" {
		return parts[2]
	}
	if len(parts) >= 4 && parts[0] == "apis" {
		return parts[3]
	}

	return "unknown"
}
