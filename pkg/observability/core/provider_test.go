package core

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func TestNewNoopProviderAvoidsSDKTracerProvider(t *testing.T) {
	provider, err := New(Config{
		ServiceName:     "procd",
		Logger:          zap.NewNop(),
		MetricsRegistry: prometheus.NewRegistry(),
		TraceExporter:   TraceExporterConfig{Type: "noop"},
		DisableMetrics:  true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if provider.TracerProvider != nil {
		t.Fatal("noop provider initialized an SDK tracer provider")
	}
	if provider.Tracer() == nil {
		t.Fatal("noop provider returned a nil tracer")
	}
	if provider.MetricsRegistryOrNil() != nil {
		t.Fatal("disabled metrics returned a registry")
	}
}

func TestHTTPServerConfigUsesNormalizedCoreConfig(t *testing.T) {
	registry := prometheus.NewRegistry()
	provider, err := New(Config{
		ServiceName:     "procd",
		Logger:          zap.NewNop(),
		MetricsRegistry: registry,
		DisableTracing:  true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cfg := provider.HTTPServerConfig(nil)
	if cfg.ServiceName != "procd" {
		t.Fatalf("service name = %q, want procd", cfg.ServiceName)
	}
	if cfg.Registry != registry {
		t.Fatal("HTTP server config did not preserve the metrics registry")
	}
	if !cfg.DisableLogging {
		t.Fatal("HTTP server config enabled logging without a logger")
	}
}
