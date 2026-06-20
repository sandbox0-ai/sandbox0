package observability

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestConfigDefaultsTraceExporterToNoop(t *testing.T) {
	cfg := Config{
		ServiceName: "test-service",
		Logger:      zap.NewNop(),
	}

	cfg.setDefaults()

	if cfg.TraceExporter.Type != "noop" {
		t.Fatalf("TraceExporter.Type = %q, want noop", cfg.TraceExporter.Type)
	}
}

func TestConfigKeepsExplicitTraceExporter(t *testing.T) {
	cfg := Config{
		ServiceName: "test-service",
		Logger:      zap.NewNop(),
		TraceExporter: TraceExporterConfig{
			Type: "stdout",
		},
	}

	cfg.setDefaults()

	if cfg.TraceExporter.Type != "stdout" {
		t.Fatalf("TraceExporter.Type = %q, want stdout", cfg.TraceExporter.Type)
	}
}

func TestConfigFromEnvParsesStandardOTELTraceConfig(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://collector.sandbox0:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "authorization=Bearer%20token,x-scope=platform")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "2500")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "false")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=dev,sandbox0.region.id=aws-us-east-1")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")

	cfg := ConfigFromEnv("manager", zap.NewNop())

	if cfg.TraceExporter.Type != "otlp" {
		t.Fatalf("TraceExporter.Type = %q, want otlp", cfg.TraceExporter.Type)
	}
	if cfg.TraceExporter.Endpoint != "http://collector.sandbox0:4317" {
		t.Fatalf("TraceExporter.Endpoint = %q", cfg.TraceExporter.Endpoint)
	}
	if cfg.TraceExporter.Headers["authorization"] != "Bearer token" {
		t.Fatalf("authorization header = %q", cfg.TraceExporter.Headers["authorization"])
	}
	if cfg.TraceExporter.Timeout != 2500*time.Millisecond {
		t.Fatalf("TraceExporter.Timeout = %s, want 2.5s", cfg.TraceExporter.Timeout)
	}
	if cfg.TraceExporter.Insecure == nil || *cfg.TraceExporter.Insecure {
		t.Fatalf("TraceExporter.Insecure = %#v, want false", cfg.TraceExporter.Insecure)
	}
	if cfg.ResourceAttributes["deployment.environment"] != "dev" {
		t.Fatalf("deployment.environment = %q", cfg.ResourceAttributes["deployment.environment"])
	}
	if cfg.TraceSampleRate != 0.25 {
		t.Fatalf("TraceSampleRate = %v, want 0.25", cfg.TraceSampleRate)
	}
}

func TestConfigFromEnvPreservesLegacyTraceExporterEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_TYPE", "console")
	t.Setenv("OTEL_EXPORTER_ENDPOINT", "collector:4317")

	cfg := ConfigFromEnv("scheduler", zap.NewNop())

	if cfg.TraceExporter.Type != "stdout" {
		t.Fatalf("TraceExporter.Type = %q, want stdout", cfg.TraceExporter.Type)
	}
	if cfg.TraceExporter.Endpoint != "collector:4317" {
		t.Fatalf("TraceExporter.Endpoint = %q, want collector:4317", cfg.TraceExporter.Endpoint)
	}
}
