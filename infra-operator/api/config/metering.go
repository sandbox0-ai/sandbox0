// +kubebuilder:object:generate=true
package config

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// MeteringConfig is the runtime configuration for the optional region usage
// ledger. When disabled, services must not emit usage records.
type MeteringConfig struct {
	// Enabled enables ClickHouse-backed metering.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `yaml:"enabled" json:"enabled"`
	// ClickHouse configures the metering ClickHouse schema.
	// +optional
	ClickHouse MeteringClickHouseConfig `yaml:"clickhouse" json:"clickHouse"`
}

// MeteringClickHouseConfig names the ClickHouse tables used for metering truth.
type MeteringClickHouseConfig struct {
	// DSN is the ClickHouse database/sql connection string. It may include credentials.
	// +optional
	DSN string `yaml:"dsn" json:"-"`
	// +optional
	// +kubebuilder:default="sandbox0_metering"
	Database string `yaml:"database" json:"database"`
	// +optional
	// +kubebuilder:default="usage_events"
	EventsTable string `yaml:"events_table" json:"eventsTable"`
	// +optional
	// +kubebuilder:default="usage_windows"
	WindowsTable string `yaml:"windows_table" json:"windowsTable"`
	// +optional
	// +kubebuilder:default="producer_watermarks"
	WatermarksTable string `yaml:"watermarks_table" json:"watermarksTable"`
	// +optional
	// +kubebuilder:default="sandbox_projection_state"
	SandboxStateTable string `yaml:"sandbox_state_table" json:"sandboxStateTable"`
	// +optional
	// +kubebuilder:default="storage_projection_state"
	StorageStateTable string `yaml:"storage_state_table" json:"storageStateTable"`
	// ConnectTimeout bounds startup connection and schema checks.
	// +optional
	// +kubebuilder:default="10s"
	ConnectTimeout metav1.Duration `yaml:"connect_timeout" json:"connectTimeout"`
	// SkipSchemaMigration disables CREATE/ALTER TABLE at startup.
	// +optional
	SkipSchemaMigration bool `yaml:"skip_schema_migration" json:"skipSchemaMigration"`
}
