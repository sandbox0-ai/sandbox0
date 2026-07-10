// Package sandboxobservability defines the per-sandbox historical event, log,
// and metric query contract.
//
// The package intentionally contains only domain types and repository/writer
// interfaces. Backends such as ClickHouse implement Repository outside API
// handlers and receive events through the Writer ingest boundary. Metering usage
// windows stay in PostgreSQL metering tables and are not projected into this
// query model.
// Runtime stats history should come from an explicit manager-side sampler
// instead of ad hoc scraping of every procd instance.
//
// File audit is out of scope for this package.
package sandboxobservability
