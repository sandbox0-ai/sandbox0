// Package observability provides shared tracing, Prometheus metrics, and
// structured logging for Sandbox0 services. The top-level Provider exposes
// adapters for HTTP clients and servers and PostgreSQL pools while sharing one
// OpenTelemetry provider and zap logger.
//
// Create one Provider per process, pass its adapters to client constructors,
// and call Shutdown during bounded graceful shutdown.
package observability
