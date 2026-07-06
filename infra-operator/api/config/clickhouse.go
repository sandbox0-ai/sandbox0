// +kubebuilder:object:generate=true
package config

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ClickHouseConfig is the shared runtime configuration for the region-level
// ClickHouse component.
type ClickHouseConfig struct {
	// DSN is the ClickHouse database/sql connection string. It may include credentials.
	// +optional
	DSN string `yaml:"dsn" json:"-"`
	// +optional
	// +kubebuilder:default=9000
	NativePort int32 `yaml:"native_port" json:"nativePort"`
	// +optional
	// +kubebuilder:default=8123
	HTTPPort int32 `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="10s"
	ConnectTimeout metav1.Duration `yaml:"connect_timeout" json:"connectTimeout"`
	// +optional
	// +kubebuilder:default=true
	SchemaMigrationEnabled bool `yaml:"schema_migration_enabled" json:"schemaMigrationEnabled"`
	// +optional
	Databases ClickHouseDatabaseConfig `yaml:"databases" json:"databases"`
}

// ClickHouseDatabaseConfig names logical Sandbox0 ClickHouse databases.
type ClickHouseDatabaseConfig struct {
	// +optional
	// +kubebuilder:default="sandbox0_observability"
	Observability string `yaml:"observability" json:"observability"`
	// +optional
	// +kubebuilder:default="sandbox0_metering"
	Metering string `yaml:"metering" json:"metering"`
}
