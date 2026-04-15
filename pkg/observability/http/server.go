package http

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/promutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ServerConfig configures HTTP server observability middleware.
type ServerConfig struct {
	ServiceName    string
	Tracer         trace.Tracer
	Logger         *zap.Logger
	Registry       prometheus.Registerer
	DisableMetrics bool
	DisableLogging bool
	Disabled       bool
}

// ServerMiddleware returns net/http middleware with tracing and optional logging.
func ServerMiddleware(cfg ServerConfig) func(http.Handler) http.Handler {
	metrics := newServerMetricsFromConfig(cfg)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.Disabled {
				next.ServeHTTP(w, r)
				return
			}

			tracer := cfg.Tracer
			if tracer == nil {
				tracer = otel.Tracer("http-server")
			}

			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			start := time.Now()
			route := normalizeRoute(r.URL.Path)

			spanName := fmt.Sprintf("HTTP %s %s", r.Method, r.URL.Path)
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("http.method", r.Method),
					attribute.String("http.url", r.URL.String()),
					attribute.String("http.host", r.Host),
					attribute.String("http.scheme", r.URL.Scheme),
					attribute.String("http.target", r.URL.Path),
				),
			)
			defer span.End()

			if metrics != nil {
				metrics.activeRequests.WithLabelValues(r.Method, route).Inc()
				defer metrics.activeRequests.WithLabelValues(r.Method, route).Dec()
				if r.ContentLength > 0 {
					metrics.requestSize.WithLabelValues(r.Method, route).Observe(float64(r.ContentLength))
				}
			}

			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(wrapped, r.WithContext(ctx))

			status := wrapped.statusCode
			span.SetAttributes(attribute.Int("http.status_code", status))
			if status >= 400 {
				span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
			}

			if metrics != nil {
				statusLabel := strconv.Itoa(status)
				metrics.requestsTotal.WithLabelValues(r.Method, route, statusLabel).Inc()
				metrics.requestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
				if wrapped.bytesWritten > 0 {
					metrics.responseSize.WithLabelValues(r.Method, route).Observe(float64(wrapped.bytesWritten))
				}
			}

			if !cfg.DisableLogging && cfg.Logger != nil {
				level := zap.InfoLevel
				if status >= 500 {
					level = zap.ErrorLevel
				} else if status >= 400 {
					level = zap.WarnLevel
				}
				cfg.Logger.Log(level, "HTTP request",
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
					zap.String("route", route),
					zap.Int("status", status),
					zap.Duration("latency", time.Since(start)),
					zap.String("client_ip", r.RemoteAddr),
					zap.String("trace_id", span.SpanContext().TraceID().String()),
					zap.String("span_id", span.SpanContext().SpanID().String()),
				)
			}
		})
	}
}

// GinMiddleware returns gin middleware with tracing and optional logging.
func GinMiddleware(cfg ServerConfig) gin.HandlerFunc {
	metrics := newServerMetricsFromConfig(cfg)
	return func(c *gin.Context) {
		if cfg.Disabled {
			c.Next()
			return
		}

		tracer := cfg.Tracer
		if tracer == nil {
			tracer = otel.Tracer("http-server")
		}

		ctx := otel.GetTextMapPropagator().Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		start := time.Now()
		initialRoute := normalizeRoute(c.Request.URL.Path)

		spanName := fmt.Sprintf("HTTP %s %s", c.Request.Method, c.Request.URL.Path)
		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", c.Request.Method),
				attribute.String("http.url", c.Request.URL.String()),
				attribute.String("http.host", c.Request.Host),
				attribute.String("http.scheme", c.Request.URL.Scheme),
				attribute.String("http.target", c.Request.URL.Path),
			),
		)
		defer span.End()

		c.Request = c.Request.WithContext(ctx)
		if metrics != nil {
			metrics.activeRequests.WithLabelValues(c.Request.Method, initialRoute).Inc()
			defer metrics.activeRequests.WithLabelValues(c.Request.Method, initialRoute).Dec()
			if c.Request.ContentLength > 0 {
				metrics.requestSize.WithLabelValues(c.Request.Method, initialRoute).Observe(float64(c.Request.ContentLength))
			}
		}
		c.Next()

		status := c.Writer.Status()
		route := c.FullPath()
		if route == "" {
			route = initialRoute
		}
		span.SetAttributes(attribute.String("http.route", route))
		span.SetName(fmt.Sprintf("HTTP %s %s", c.Request.Method, route))

		span.SetAttributes(attribute.Int("http.status_code", status))
		if status >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
		}

		if metrics != nil {
			statusLabel := strconv.Itoa(status)
			metrics.requestsTotal.WithLabelValues(c.Request.Method, route, statusLabel).Inc()
			metrics.requestDuration.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
			if size := c.Writer.Size(); size > 0 {
				metrics.responseSize.WithLabelValues(c.Request.Method, route).Observe(float64(size))
			}
		}

		if !cfg.DisableLogging && cfg.Logger != nil {
			level := zap.InfoLevel
			if status >= 500 {
				level = zap.ErrorLevel
			} else if status >= 400 {
				level = zap.WarnLevel
			}
			cfg.Logger.Log(level, "HTTP request",
				zap.String("method", c.Request.Method),
				zap.String("path", c.Request.URL.Path),
				zap.String("route", route),
				zap.Int("status", status),
				zap.Duration("latency", time.Since(start)),
				zap.String("client_ip", c.ClientIP()),
				zap.String("trace_id", span.SpanContext().TraceID().String()),
				zap.String("span_id", span.SpanContext().SpanID().String()),
			)
		}
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(p []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(p)
	rw.bytesWritten += n
	return n, err
}

func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (rw *responseWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rw *responseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := rw.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

type serverMetrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.HistogramVec
	responseSize    *prometheus.HistogramVec
	activeRequests  *prometheus.GaugeVec
}

func newServerMetricsFromConfig(cfg ServerConfig) *serverMetrics {
	if cfg.DisableMetrics || cfg.Registry == nil || strings.TrimSpace(cfg.ServiceName) == "" {
		return nil
	}
	prefix := promutil.MetricPrefix(cfg.ServiceName)
	return &serverMetrics{
		requestsTotal: promutil.RegisterCounterVec(cfg.Registry, prometheus.CounterOpts{
			Name: prefix + "_http_server_requests_total",
			Help: "Total number of HTTP server requests",
		}, []string{"method", "route", "status"}),
		requestDuration: promutil.RegisterHistogramVec(cfg.Registry, prometheus.HistogramOpts{
			Name:    prefix + "_http_server_request_duration_seconds",
			Help:    "HTTP server request duration in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method", "route"}),
		requestSize: promutil.RegisterHistogramVec(cfg.Registry, prometheus.HistogramOpts{
			Name:    prefix + "_http_server_request_size_bytes",
			Help:    "HTTP server request size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		}, []string{"method", "route"}),
		responseSize: promutil.RegisterHistogramVec(cfg.Registry, prometheus.HistogramOpts{
			Name:    prefix + "_http_server_response_size_bytes",
			Help:    "HTTP server response size in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		}, []string{"method", "route"}),
		activeRequests: promutil.RegisterGaugeVec(cfg.Registry, prometheus.GaugeOpts{
			Name: prefix + "_http_server_active_requests",
			Help: "Number of active HTTP server requests",
		}, []string{"method", "route"}),
	}
}

func normalizeRoute(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, ":") || strings.HasPrefix(part, "{") {
			continue
		}
		if isLikelyRouteID(part) {
			parts[i] = "{id}"
		}
	}
	return "/" + strings.Join(parts, "/")
}

func isLikelyRouteID(segment string) bool {
	if segment == "" {
		return false
	}
	if _, err := strconv.Atoi(segment); err == nil {
		return true
	}
	if len(segment) < 8 {
		return false
	}
	hasDigit := false
	for _, r := range segment {
		if r >= '0' && r <= '9' {
			hasDigit = true
			continue
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return hasDigit
}
