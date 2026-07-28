package core

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/observability/httpserver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// Provider owns service-level tracing and metrics configuration without
// importing optional client adapters.
type Provider struct {
	config Config
	logger *zap.Logger
	tracer trace.Tracer

	TracerProvider  *sdktrace.TracerProvider
	MetricsRegistry prometheus.Registerer
}

// New creates a core observability provider.
func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()
	p := &Provider{
		config:          cfg,
		logger:          cfg.Logger,
		MetricsRegistry: cfg.MetricsRegistry,
	}
	if !cfg.DisableTracing && cfg.TraceExporter.Type != "" && cfg.TraceExporter.Type != "noop" {
		tp, err := initTracing(cfg)
		if err != nil {
			return nil, fmt.Errorf("init tracing: %w", err)
		}
		p.TracerProvider = tp
		p.tracer = tp.Tracer(cfg.ServiceName)
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	} else {
		p.tracer = noop.NewTracerProvider().Tracer(cfg.ServiceName)
	}
	cfg.Logger.Info("Observability provider initialized",
		zap.String("service", cfg.ServiceName),
		zap.Bool("tracing", !cfg.DisableTracing && cfg.TraceExporter.Type != "noop"),
		zap.Bool("metrics", !cfg.DisableMetrics),
		zap.String("trace_exporter", cfg.TraceExporter.Type),
	)
	return p, nil
}

// Config returns the normalized provider configuration.
func (p *Provider) Config() Config {
	if p == nil {
		return Config{}
	}
	return p.config
}

// Shutdown gracefully shuts down the provider and flushes pending data.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p != nil && p.TracerProvider != nil {
		if err := p.TracerProvider.Shutdown(ctx); err != nil {
			p.logger.Error("Failed to shutdown tracer provider", zap.Error(err))
			return err
		}
	}
	return nil
}

// Tracer returns the OpenTelemetry tracer for manual instrumentation.
func (p *Provider) Tracer() trace.Tracer {
	if p == nil {
		return noop.NewTracerProvider().Tracer("noop")
	}
	return p.tracer
}

// MetricsRegistryOrNil returns the registry only when metrics are enabled.
func (p *Provider) MetricsRegistryOrNil() prometheus.Registerer {
	if p == nil || p.config.DisableMetrics {
		return nil
	}
	return p.MetricsRegistry
}

// HTTPServerConfig returns framework-neutral HTTP server middleware config.
func (p *Provider) HTTPServerConfig(logger *zap.Logger) httpserver.Config {
	if p == nil {
		return httpserver.Config{Disabled: true}
	}
	return httpserver.Config{
		ServiceName:    p.config.ServiceName,
		Tracer:         p.tracer,
		Logger:         logger,
		Registry:       p.MetricsRegistryOrNil(),
		DisableMetrics: p.config.DisableMetrics,
		DisableLogging: p.config.DisableLogging || logger == nil,
		Disabled:       p.config.DisableTracing && p.config.DisableMetrics && p.config.DisableLogging,
	}
}

func initTracing(cfg Config) (*sdktrace.TracerProvider, error) {
	var exporter sdktrace.SpanExporter
	var err error
	switch cfg.TraceExporter.Type {
	case "otlp":
		exporter, err = otlptracegrpc.New(context.Background(), otlpTraceGRPCOptions(cfg.TraceExporter)...)
	case "stdout":
		exporter = newZapSpanExporter(cfg.Logger)
	default:
		return nil, fmt.Errorf("unknown trace exporter type: %s (supported: otlp, stdout, noop)", cfg.TraceExporter.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(resourceAttributes(cfg)...),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.TraceSampleRate)),
	), nil
}

func otlpTraceGRPCOptions(cfg TraceExporterConfig) []otlptracegrpc.Option {
	opts := make([]otlptracegrpc.Option, 0, 4)
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint != "" {
		if strings.Contains(endpoint, "://") {
			opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
		} else {
			opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
		}
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}
	if cfg.Timeout > 0 {
		opts = append(opts, otlptracegrpc.WithTimeout(cfg.Timeout))
	}
	if cfg.Insecure != nil && *cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return opts
}

func resourceAttributes(cfg Config) []attribute.KeyValue {
	attrs := []attribute.KeyValue{semconv.ServiceNameKey.String(cfg.ServiceName)}
	if len(cfg.ResourceAttributes) == 0 {
		return attrs
	}
	normalized := map[string]string{}
	for key, value := range cfg.ResourceAttributes {
		key = strings.TrimSpace(key)
		if key != "" && key != string(semconv.ServiceNameKey) {
			normalized[key] = value
		}
	}
	keys := make([]string, 0, len(normalized))
	for key := range normalized {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		attrs = append(attrs, attribute.String(key, normalized[key]))
	}
	return attrs
}
