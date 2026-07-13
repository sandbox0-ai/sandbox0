// Package clickhouse implements the ClickHouse query backend for per-sandbox
// historical observability events, logs, and metric samples.
//
// The repository implements sandboxobservability.Repository for typed queries
// and sandboxobservability.Writer for asynchronous ingest. It is intentionally a
// query store: PostgreSQL metering remains the usage and billing truth, and
// stable event IDs provide deterministic pagination and replay boundaries.
package clickhouse
