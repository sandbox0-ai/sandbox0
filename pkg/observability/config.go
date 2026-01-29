package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Config holds configuration for the observability provider
type Config struct {
	// ServiceName is used as the service name in traces and metric prefixes
	ServiceName string

	// Logger for structured logging (required)
	Logger *zap.Logger

	// MetricsRegistry for Prometheus metrics (default: prometheus.DefaultRegisterer)
	MetricsRegistry prometheus.Registerer

	// TraceExporter configures where traces are sent
	// If nil, traces are disabled
	TraceExporter TraceExporterConfig

	// Optional: disable specific observability features globally
	DisableTracing bool
	DisableMetrics bool
	DisableLogging bool

	// Optional: sampling configuration
	TraceSampleRate float64 // 0.0 to 1.0, default: 1.0 (100%)
}

// TraceExporterConfig configures the trace exporter
type TraceExporterConfig struct {
	// Type of exporter: "otlp", "stdout", or "noop" (default: "stdout")
	Type string

	// Endpoint for the exporter
	Endpoint string

	// For OTLP exporters
	Headers map[string]string

	// Timeout for export operations
	Timeout time.Duration
}

// Validate checks if the config is valid
func (c *Config) Validate() error {
	if c.ServiceName == "" {
		return ErrMissingServiceName
	}
	if c.Logger == nil {
		return ErrMissingLogger
	}
	return nil
}

// setDefaults sets default values for optional fields
func (c *Config) setDefaults() {
	if c.MetricsRegistry == nil {
		c.MetricsRegistry = prometheus.DefaultRegisterer
	}
	if c.TraceSampleRate == 0 {
		c.TraceSampleRate = 1.0
	}
	if c.TraceExporter.Type == "" {
		c.TraceExporter.Type = "stdout"
	}
	if c.TraceExporter.Timeout == 0 {
		c.TraceExporter.Timeout = 10 * time.Second
	}
}
