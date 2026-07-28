// Package httpserver instruments net/http servers without importing a web
// framework.
package httpserver

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/httpattrs"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/internal/promutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Config configures HTTP server observability.
type Config struct {
	ServiceName    string
	Tracer         trace.Tracer
	Logger         *zap.Logger
	Registry       prometheus.Registerer
	DisableMetrics bool
	DisableLogging bool
	Disabled       bool
}

// Result describes the completed request for metrics, tracing, and logging.
type Result struct {
	Status       int
	Route        string
	RouteKnown   bool
	ResponseSize int
	ClientIP     string
}

// Observer starts request observations shared by net/http and framework
// adapters.
type Observer struct {
	cfg     Config
	metrics *serverMetrics
}

// NewObserver creates an HTTP server request observer.
func NewObserver(cfg Config) *Observer {
	return &Observer{cfg: cfg, metrics: newServerMetrics(cfg)}
}

// Observation tracks one HTTP server request.
type Observation struct {
	cfg          Config
	metrics      *serverMetrics
	request      *http.Request
	start        time.Time
	initialRoute string
	span         trace.Span
	closed       bool
	finished     bool
}

// Start begins observing a request and returns the request with tracing context.
func (o *Observer) Start(r *http.Request) (*http.Request, *Observation) {
	if o == nil || o.cfg.Disabled {
		return r, nil
	}
	tracer := o.cfg.Tracer
	if tracer == nil {
		tracer = otel.Tracer("http-server")
	}
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	initialRoute := normalizeRoute(r.URL.Path)
	ctx, span := tracer.Start(ctx, fmt.Sprintf("HTTP %s %s", r.Method, r.URL.Path),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(requestAttributes(r)...),
	)
	if o.metrics != nil {
		o.metrics.activeRequests.WithLabelValues(r.Method, initialRoute).Inc()
		if r.ContentLength > 0 {
			o.metrics.requestSize.WithLabelValues(r.Method, initialRoute).Observe(float64(r.ContentLength))
		}
	}
	request := r.WithContext(ctx)
	return request, &Observation{
		cfg:          o.cfg,
		metrics:      o.metrics,
		request:      request,
		start:        time.Now(),
		initialRoute: initialRoute,
		span:         span,
	}
}

// Finish records the result of the observed request.
func (o *Observation) Finish(result Result) {
	if o == nil || o.finished {
		return
	}
	o.finished = true
	route := strings.TrimSpace(result.Route)
	if route == "" {
		route = o.initialRoute
	}
	if result.RouteKnown {
		o.span.SetAttributes(semconv.HTTPRoute(route))
		o.span.SetName(fmt.Sprintf("HTTP %s %s", o.request.Method, route))
	}
	status := result.Status
	if status == 0 {
		status = http.StatusOK
	}
	o.span.SetAttributes(semconv.HTTPResponseStatusCode(status))
	if status >= 400 {
		o.span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", status))
	}
	if o.metrics != nil {
		statusLabel := strconv.Itoa(status)
		o.metrics.requestsTotal.WithLabelValues(o.request.Method, route, statusLabel).Inc()
		o.metrics.requestDuration.WithLabelValues(o.request.Method, route).Observe(time.Since(o.start).Seconds())
		if result.ResponseSize > 0 {
			o.metrics.responseSize.WithLabelValues(o.request.Method, route).Observe(float64(result.ResponseSize))
		}
	}
	if !o.cfg.DisableLogging && o.cfg.Logger != nil {
		level := zap.InfoLevel
		if status >= 500 {
			level = zap.ErrorLevel
		} else if status >= 400 {
			level = zap.WarnLevel
		}
		o.cfg.Logger.Log(level, "HTTP request",
			zap.String("method", o.request.Method),
			zap.String("path", o.request.URL.Path),
			zap.String("route", route),
			zap.Int("status", status),
			zap.Duration("latency", time.Since(o.start)),
			zap.String("client_ip", result.ClientIP),
			zap.String("trace_id", o.span.SpanContext().TraceID().String()),
			zap.String("span_id", o.span.SpanContext().SpanID().String()),
		)
	}
}

// Close releases active-request accounting and ends the span.
func (o *Observation) Close() {
	if o == nil || o.closed {
		return
	}
	o.closed = true
	if o.metrics != nil {
		o.metrics.activeRequests.WithLabelValues(o.request.Method, o.initialRoute).Dec()
	}
	o.span.End()
}

// Middleware returns net/http middleware with tracing and optional logging.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	observer := NewObserver(cfg)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request, observation := observer.Start(r)
			if observation == nil {
				next.ServeHTTP(w, r)
				return
			}
			defer observation.Close()

			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(wrapped, request)
			observation.Finish(Result{
				Status:       wrapped.statusCode,
				ResponseSize: wrapped.bytesWritten,
				ClientIP:     r.RemoteAddr,
			})
		})
	}
}

func requestAttributes(r *http.Request) []attribute.KeyValue {
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
	address, port := httpattrs.SplitHostPort(host)
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

func newServerMetrics(cfg Config) *serverMetrics {
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
		if part == "" || strings.HasPrefix(part, ":") || strings.HasPrefix(part, "{") {
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
