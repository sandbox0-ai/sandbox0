package http

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Adapter provides observable HTTP clients
type Adapter struct {
	config  AdapterConfig
	metrics *metrics
}

// AdapterConfig configures the HTTP adapter
type AdapterConfig struct {
	ServiceName string
	Tracer      trace.Tracer
	Logger      *zap.Logger
	Registry    prometheus.Registerer
	Disabled    bool
}

// Config holds configuration for creating an observable HTTP client
type Config struct {
	Timeout       time.Duration
	BaseTransport http.RoundTripper

	// Optional: additional custom options
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
}

// NewAdapter creates a new HTTP adapter
func NewAdapter(cfg AdapterConfig) Adapter {
	var m *metrics
	if !cfg.Disabled && cfg.Registry != nil {
		m = newMetrics(cfg.ServiceName, cfg.Registry)
	}

	return Adapter{
		config:  cfg,
		metrics: m,
	}
}

// NewClient creates a fully observable HTTP client
func (a Adapter) NewClient(cfg Config) *http.Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.BaseTransport == nil {
		cfg.BaseTransport = http.DefaultTransport
	}

	// Wrap with observable transport
	transport := &observableTransport{
		base:    cfg.BaseTransport,
		config:  a.config,
		metrics: a.metrics,
	}

	return &http.Client{
		Timeout:   cfg.Timeout,
		Transport: transport,
	}
}

// NewTransport creates an observable HTTP transport that can be used standalone
// or composed with other transports
func (a Adapter) NewTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	return &observableTransport{
		base:    base,
		config:  a.config,
		metrics: a.metrics,
	}
}

// metrics holds Prometheus metrics for HTTP client
type metrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	requestSize     *prometheus.HistogramVec
	responseSize    *prometheus.HistogramVec
	activeRequests  *prometheus.GaugeVec
}

func newMetrics(serviceName string, registry prometheus.Registerer) *metrics {
	factory := promauto.With(registry)

	return &metrics{
		requestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: serviceName + "_http_client_requests_total",
				Help: "Total number of HTTP client requests",
			},
			[]string{"method", "host", "status"},
		),
		requestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    serviceName + "_http_client_request_duration_seconds",
				Help:    "HTTP client request duration in seconds",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"method", "host"},
		),
		requestSize: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    serviceName + "_http_client_request_size_bytes",
				Help:    "HTTP client request size in bytes",
				Buckets: prometheus.ExponentialBuckets(100, 10, 7),
			},
			[]string{"method", "host"},
		),
		responseSize: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    serviceName + "_http_client_response_size_bytes",
				Help:    "HTTP client response size in bytes",
				Buckets: prometheus.ExponentialBuckets(100, 10, 7),
			},
			[]string{"method", "host"},
		),
		activeRequests: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: serviceName + "_http_client_active_requests",
				Help: "Number of active HTTP client requests",
			},
			[]string{"method", "host"},
		),
	}
}
