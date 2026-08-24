package config

// MeteringConfig is the runtime configuration for the optional region usage
// ledger. When disabled, services must not emit usage records.
type MeteringConfig struct {
	// Enabled enables PostgreSQL-buffered, ClickHouse-backed metering.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// ClickHouse configures the metering ClickHouse schema.
	ClickHouse MeteringClickHouseConfig `yaml:"clickhouse" json:"clickHouse"`
}

// MeteringClickHouseConfig names the ClickHouse tables used for long-term metering truth.
type MeteringClickHouseConfig struct {
	// DSN is the ClickHouse database/sql connection string. It may include credentials.
	DSN               string `yaml:"dsn" json:"-"`
	Database          string `yaml:"database" json:"database"`
	EventsTable       string `yaml:"events_table" json:"eventsTable"`
	WindowsTable      string `yaml:"windows_table" json:"windowsTable"`
	WatermarksTable   string `yaml:"watermarks_table" json:"watermarksTable"`
	SandboxStateTable string `yaml:"sandbox_state_table" json:"sandboxStateTable"`
	StorageStateTable string `yaml:"storage_state_table" json:"storageStateTable"`
	// ConnectTimeout bounds startup connection and schema checks.
	ConnectTimeout Duration `yaml:"connect_timeout" json:"connectTimeout"`
	// SkipSchemaMigration disables CREATE/ALTER TABLE at startup.
	SkipSchemaMigration bool `yaml:"skip_schema_migration" json:"skipSchemaMigration"`
}
