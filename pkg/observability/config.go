package observability

import (
	"net/url"
	"os"
	"strconv"
	"strings"
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

	// ResourceAttributes are attached to emitted OpenTelemetry resources.
	ResourceAttributes map[string]string

	// Optional: disable specific observability features globally
	DisableTracing bool
	DisableMetrics bool
	DisableLogging bool

	// Optional: sampling configuration
	TraceSampleRate float64 // 0.0 to 1.0, default: 1.0 (100%)
}

// TraceExporterConfig configures the trace exporter
type TraceExporterConfig struct {
	// Type of exporter: "otlp", "stdout", or "noop" (default: "noop")
	Type string

	// Endpoint for the exporter
	Endpoint string

	// For OTLP exporters
	Headers map[string]string

	// Timeout for export operations
	Timeout time.Duration

	// Insecure disables transport security for OTLP/gRPC exporters.
	Insecure *bool
}

// ConfigFromEnv builds service observability config from standard OpenTelemetry
// environment variables while preserving the legacy sandbox0 exporter envs.
func ConfigFromEnv(serviceName string, logger *zap.Logger) Config {
	cfg := Config{
		ServiceName: serviceName,
		Logger:      logger,
	}

	if attrs, ok := firstEnv("OTEL_RESOURCE_ATTRIBUTES"); ok {
		cfg.ResourceAttributes = parseKeyValueList(attrs)
	}
	if exporter, ok := firstEnv("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_TYPE"); ok {
		cfg.TraceExporter.Type = normalizeTraceExporter(exporter)
	}
	if endpoint, ok := firstEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_ENDPOINT"); ok {
		cfg.TraceExporter.Endpoint = strings.TrimSpace(endpoint)
	}
	if headers, ok := firstEnv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_EXPORTER_OTLP_HEADERS"); ok {
		cfg.TraceExporter.Headers = parseKeyValueList(headers)
	}
	if timeout, ok := firstEnv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "OTEL_EXPORTER_OTLP_TIMEOUT"); ok {
		if parsed, parseOK := parseOTELTimeout(timeout); parseOK {
			cfg.TraceExporter.Timeout = parsed
		}
	}
	if insecure, ok := firstBoolEnv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "OTEL_EXPORTER_OTLP_INSECURE"); ok {
		cfg.TraceExporter.Insecure = &insecure
	}
	if sampleRate, ok := traceSampleRateFromEnv(); ok {
		if sampleRate <= 0 {
			cfg.DisableTracing = true
		} else {
			cfg.TraceSampleRate = sampleRate
		}
	}

	return cfg
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
		c.TraceExporter.Type = "noop"
	}
	if c.TraceExporter.Timeout == 0 {
		c.TraceExporter.Timeout = 10 * time.Second
	}
	if c.TraceExporter.Type == "otlp" && c.TraceExporter.Insecure == nil {
		insecure := true
		c.TraceExporter.Insecure = &insecure
	}
}

func firstEnv(names ...string) (string, bool) {
	for _, name := range names {
		value, ok := os.LookupEnv(name)
		if ok && strings.TrimSpace(value) != "" {
			return value, true
		}
	}
	return "", false
}

func firstBoolEnv(names ...string) (bool, bool) {
	value, ok := firstEnv(names...)
	if !ok {
		return false, false
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, false
	}
	return parsed, true
}

func normalizeTraceExporter(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none", "noop":
		return "noop"
	case "console":
		return "stdout"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func parseKeyValueList(value string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if decoded, err := url.QueryUnescape(strings.TrimSpace(val)); err == nil {
			val = decoded
		} else {
			val = strings.TrimSpace(val)
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseOTELTimeout(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if duration, err := time.ParseDuration(value); err == nil {
		return duration, true
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil || millis < 0 {
		return 0, false
	}
	return time.Duration(millis) * time.Millisecond, true
}

func traceSampleRateFromEnv() (float64, bool) {
	if value, ok := firstEnv("SANDBOX0_OTEL_TRACE_SAMPLE_RATE"); ok {
		return parseSampleRate(value)
	}

	sampler, ok := firstEnv("OTEL_TRACES_SAMPLER")
	if !ok {
		return 0, false
	}
	switch strings.ToLower(strings.TrimSpace(sampler)) {
	case "always_on":
		return 1, true
	case "always_off":
		return 0, true
	case "traceidratio", "parentbased_traceidratio":
		arg, argOK := firstEnv("OTEL_TRACES_SAMPLER_ARG")
		if !argOK {
			return 0, false
		}
		return parseSampleRate(arg)
	default:
		return 0, false
	}
}

func parseSampleRate(value string) (float64, bool) {
	rate, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || rate < 0 || rate > 1 {
		return 0, false
	}
	return rate, true
}
