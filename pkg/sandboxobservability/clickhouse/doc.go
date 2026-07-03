// Package clickhouse implements the ClickHouse query backend
// for per-sandbox historical observability and audit events.
//
// The repository implements sandboxobservability.Repository for typed queries
// and sandboxobservability.Writer for asynchronous ingest. It is intentionally a
// query store: PostgreSQL metering remains the usage and billing truth, and
// producers must provide stable cursors so events can be replayed idempotently.
package clickhouse
