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
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
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
				trace.WithAttributes(serverRequestAttributes(r)...),
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
			span.SetAttributes(semconv.HTTPResponseStatusCode(status))
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
			trace.WithAttributes(serverRequestAttributes(c.Request)...),
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
		span.SetAttributes(semconv.HTTPRoute(route))
		span.SetName(fmt.Sprintf("HTTP %s %s", c.Request.Method, route))

		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
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

func serverRequestAttributes(r *http.Request) []attribute.KeyValue {
	scheme := requestScheme(r)
	method := ""
	host := ""
	path := ""
	fullURL := ""
	if r != nil {
		method = r.Method
		host = r.Host
	}
	if r != nil && r.URL != nil {
		if host == "" {
			host = r.URL.Host
		}
		path = r.URL.EscapedPath()
		if path == "" {
			path = "/"
		}
		fullURL = requestFullURL(r, scheme, host)
	}
	address, port := splitHostPort(host)
	attrs := []attribute.KeyValue{
		semconv.HTTPRequestMethodKey.String(method),
		semconv.URLFullKey.String(fullURL),
		semconv.URLSchemeKey.String(scheme),
		semconv.URLPathKey.String(path),
	}
	if address != "" {
		attrs = append(attrs, semconv.ServerAddressKey.String(address))
	}
	if port > 0 {
		attrs = append(attrs, semconv.ServerPort(port))
	}
	return attrs
}

func requestScheme(r *http.Request) string {
	if r == nil {
		return ""
	}
	if r.URL != nil && r.URL.Scheme != "" {
		return r.URL.Scheme
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func requestFullURL(r *http.Request, scheme, host string) string {
	if r == nil || r.URL == nil {
		return ""
	}
	if r.URL.IsAbs() {
		return r.URL.String()
	}
	u := *r.URL
	u.Scheme = scheme
	u.Host = host
	return u.String()
}

func splitHostPort(host string) (string, int) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", 0
	}
	if address, portText, err := net.SplitHostPort(host); err == nil {
		port, _ := strconv.Atoi(portText)
		return address, port
	}
	if strings.Count(host, ":") == 1 {
		if address, portText, ok := strings.Cut(host, ":"); ok {
			port, err := strconv.Atoi(portText)
			if err == nil {
				return address, port
			}
		}
	}
	return host, 0
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
