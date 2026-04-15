package observability

import (
	"testing"

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
