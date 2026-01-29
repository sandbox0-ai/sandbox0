package observability

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	httpobs "github.com/sandbox0-ai/infra/pkg/observability/http"
	k8sobs "github.com/sandbox0-ai/infra/pkg/observability/k8s"
	pgxobs "github.com/sandbox0-ai/infra/pkg/observability/pgx"
)

var (
	ErrMissingServiceName = errors.New("service name is required")
	ErrMissingLogger      = errors.New("logger is required")
)

// Provider is the main entry point for observability features
type Provider struct {
	config Config
	logger *zap.Logger
	tracer trace.Tracer

	// TracerProvider is the OpenTelemetry tracer provider
	// Exposed for advanced use cases
	TracerProvider *sdktrace.TracerProvider

	// MetricsRegistry is the Prometheus metrics registry
	MetricsRegistry prometheus.Registerer

	// Client-specific adapters
	HTTP httpobs.Adapter
	K8s  k8sobs.Adapter
	Pgx  pgxobs.Adapter
	// GRPC grpcobs.Adapter // TODO: implement gRPC adapter
}

// New creates a new observability provider
func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.setDefaults()

	p := &Provider{
		config:          cfg,
		logger:          cfg.Logger,
		MetricsRegistry: cfg.MetricsRegistry,
	}

	// Initialize tracing if not disabled
	if !cfg.DisableTracing && cfg.TraceExporter.Type != "" {
		tp, err := initTracing(cfg)
		if err != nil {
			return nil, fmt.Errorf("init tracing: %w", err)
		}
		p.TracerProvider = tp
		p.tracer = tp.Tracer(cfg.ServiceName)

		// Set global tracer provider
		otel.SetTracerProvider(tp)

		// Set global propagator
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	} else {
		p.tracer = noop.NewTracerProvider().Tracer(cfg.ServiceName)
	}

	// Initialize client adapters
	p.HTTP = httpobs.NewAdapter(httpobs.AdapterConfig{
		ServiceName: cfg.ServiceName,
		Tracer:      p.tracer,
		Logger:      cfg.Logger,
		Registry:    cfg.MetricsRegistry,
		Disabled:    cfg.DisableTracing && cfg.DisableMetrics && cfg.DisableLogging,
	})

	p.K8s = k8sobs.NewAdapter(k8sobs.AdapterConfig{
		ServiceName: cfg.ServiceName,
		Tracer:      p.tracer,
		Logger:      cfg.Logger,
		Registry:    cfg.MetricsRegistry,
		Disabled:    cfg.DisableTracing && cfg.DisableMetrics && cfg.DisableLogging,
	})

	p.Pgx = pgxobs.NewAdapter(pgxobs.AdapterConfig{
		ServiceName: cfg.ServiceName,
		Tracer:      p.tracer,
		Logger:      cfg.Logger,
		Registry:    cfg.MetricsRegistry,
		Disabled:    cfg.DisableTracing && cfg.DisableMetrics && cfg.DisableLogging,
	})

	cfg.Logger.Info("Observability provider initialized",
		zap.String("service", cfg.ServiceName),
		zap.Bool("tracing", !cfg.DisableTracing),
		zap.Bool("metrics", !cfg.DisableMetrics),
		zap.String("trace_exporter", cfg.TraceExporter.Type),
	)

	return p, nil
}

// Shutdown gracefully shuts down the provider and flushes any pending data
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.TracerProvider != nil {
		if err := p.TracerProvider.Shutdown(ctx); err != nil {
			p.logger.Error("Failed to shutdown tracer provider", zap.Error(err))
			return err
		}
	}
	return nil
}

// Tracer returns the OpenTelemetry tracer for manual instrumentation
func (p *Provider) Tracer() trace.Tracer {
	return p.tracer
}

// initTracing initializes OpenTelemetry tracing with the configured exporter
func initTracing(cfg Config) (*sdktrace.TracerProvider, error) {
	var exporter sdktrace.SpanExporter
	var err error

	switch cfg.TraceExporter.Type {
	case "otlp":
		exporter, err = otlptracegrpc.New(
			context.Background(),
			otlptracegrpc.WithEndpoint(cfg.TraceExporter.Endpoint),
			otlptracegrpc.WithInsecure(), // TODO: support TLS
		)
	case "stdout":
		// Use custom zap-based exporter for consistent JSON logging
		exporter = newZapSpanExporter(cfg.Logger)
	case "noop", "":
		return sdktrace.NewTracerProvider(), nil
	default:
		return nil, fmt.Errorf("unknown trace exporter type: %s (supported: otlp, stdout, noop)", cfg.TraceExporter.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.TraceSampleRate)),
	)

	return tp, nil
}
