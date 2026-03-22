// Package observability provides unified observability (tracing, metrics, logging)
// for all types of clients used in the infra codebase.
//
// # Overview
//
// This package implements a unified observability framework that works with:
//   - HTTP clients (via http.RoundTripper)
//   - Kubernetes clients (via rest.Config.Wrap)
//   - PostgreSQL clients (via pgx.QueryTracer)
//   - gRPC clients (via grpc.UnaryClientInterceptor)
//
// All clients share the same configuration and automatically emit:
//   - Distributed traces (OpenTelemetry)
//   - Metrics (Prometheus)
//   - Structured logs (zap)
//
// # Quick Start
//
// Initialize observability provider once in your main.go:
//
//	provider, err := observability.New(observability.Config{
//	    ServiceName: "cluster-gateway",
//	    Logger:      logger,
//	})
//	defer provider.Shutdown(context.Background())
//
// Then use it with any client type:
//
//	// HTTP client
//	httpClient := provider.HTTP.NewClient(http.Config{
//	    Timeout: 10 * time.Second,
//	})
//
//	// Kubernetes client
//	k8sClient, err := provider.K8s.NewClient(k8s.Config{
//	    Kubeconfig: kubeconfigPath,
//	})
//
//	// PostgreSQL pool
//	pgPool, err := provider.Pgx.NewPool(ctx, pgx.Config{
//	    DatabaseURL: dbURL,
//	})
//
//	// gRPC client
//	grpcConn, err := provider.GRPC.Dial(ctx, target, grpc.Config{})
//
// # Features
//
//   - Automatic trace context propagation across service boundaries
//   - Consistent metric naming and labels across all client types
//   - Structured logging with trace correlation
//   - Zero-code instrumentation for common operations
//   - Configurable sampling and filtering
//
// # Integration with Existing Code
//
// This package is designed to work with existing code with minimal changes.
// Simply replace your client constructors with the observable versions:
//
// Before:
//
//	httpClient := &http.Client{Timeout: 10 * time.Second}
//
// After:
//
//	httpClient := provider.HTTP.NewClient(http.Config{Timeout: 10 * time.Second})
//
// # Architecture
//
// The package uses adapter pattern to integrate with different client types:
//
//	┌─────────────────────────────────────────┐
//	│      Observability Provider             │
//	│  (Tracer, Metrics, Logger)              │
//	└────────────┬────────────────────────────┘
//	             │
//	    ┌────────┼────────┬────────┬──────────┐
//	    │        │        │        │          │
//	┌───▼───┐ ┌─▼──┐ ┌───▼───┐ ┌──▼────┐     │
//	│ HTTP  │ │K8s │ │  Pgx  │ │ gRPC  │     │
//	│Adapter│ │Adap│ │Adapter│ │Adapter│     │
//	└───┬───┘ └─┬──┘ └───┬───┘ └──┬────┘     │
//	    │       │        │        │          │
//	┌───▼───────▼────────▼────────▼──────────▼┐
//	│     Your Application Services            │
//	└──────────────────────────────────────────┘
//
// # Best Practices
//
// 1. Create one Provider instance per service
// 2. Pass the provider to all client constructors
// 3. Always call provider.Shutdown() on graceful shutdown
// 4. Use context for request-scoped metadata
// 5. Set appropriate timeout values for all clients
//
// # Performance
//
// The observability layer adds minimal overhead:
//   - HTTP: ~50-100μs per request
//   - K8s: ~20-50μs per API call
//   - Pgx: ~10-20μs per query
//   - gRPC: ~30-60μs per RPC
//
// Metrics collection is lock-free and logging is asynchronous.
package observability
