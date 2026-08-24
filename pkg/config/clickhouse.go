package config

// ClickHouseConfig is the shared runtime configuration for the region-level
// ClickHouse component.
type ClickHouseConfig struct {
	// DSN is the ClickHouse database/sql connection string. It may include credentials.
	DSN                    string                   `yaml:"dsn" json:"-"`
	NativePort             int32                    `yaml:"native_port" json:"nativePort"`
	HTTPPort               int32                    `yaml:"http_port" json:"httpPort"`
	ConnectTimeout         Duration                 `yaml:"connect_timeout" json:"connectTimeout"`
	SchemaMigrationEnabled bool                     `yaml:"schema_migration_enabled" json:"schemaMigrationEnabled"`
	Databases              ClickHouseDatabaseConfig `yaml:"databases" json:"databases"`
}

// ClickHouseDatabaseConfig names logical Sandbox0 ClickHouse databases.
type ClickHouseDatabaseConfig struct {
	Observability string `yaml:"observability" json:"observability"`
	Metering      string `yaml:"metering" json:"metering"`
}
