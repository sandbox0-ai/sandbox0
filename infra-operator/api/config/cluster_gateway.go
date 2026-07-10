// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	SandboxObservabilityBackendDisabled   = "disabled"
	SandboxObservabilityBackendClickHouse = "clickhouse"
)

// ClusterGatewayConfig holds all configuration for cluster-gateway.
type ClusterGatewayConfig struct {
	// Server configuration
	// +optional
	// +kubebuilder:default=8443
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Upstream services
	ManagerURL      string `yaml:"manager_url" json:"-"`
	StorageProxyURL string `yaml:"storage_proxy_url" json:"-"`

	// Internal authentication (for validating requests from regional-gateway and
	// generating tokens for downstream services)
	// AuthMode controls which authentication modes are accepted on /api/v1.
	// Allowed values: "internal", "public", "both".
	// +optional
	// +kubebuilder:validation:Enum=internal;public;both
	// +kubebuilder:default="internal"
	AuthMode string `yaml:"auth_mode" json:"authMode"`
	// AllowedCallers is the list of services allowed to call cluster-gateway.
	// Default: ["regional-gateway","scheduler"].
	// +optional
	// +kubebuilder:default={"regional-gateway","scheduler"}
	AllowedCallers []string `yaml:"allowed_callers" json:"allowedCallers"`

	// Timeouts
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	// +optional
	// +kubebuilder:default="10s"
	HealthCheckPeriod metav1.Duration `yaml:"health_check_period" json:"healthCheckPeriod"`

	// Proxy configuration
	// +optional
	// +kubebuilder:default="10s"
	ProxyTimeout metav1.Duration `yaml:"proxy_timeout" json:"proxyTimeout"`

	// Public gateway (external auth) configuration
	DatabaseURL string `yaml:"database_url" json:"-"`
	// License file path used to unlock enterprise SSO features.
	// Required when OIDC providers are configured.
	// +optional
	LicenseFile string `yaml:"license_file" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`

	// Shared gateway configuration
	// +optional
	GatewayConfig `yaml:",inline" json:",inline"`

	// SandboxObservability configures the per-sandbox historical observability
	// query backend. It is separate from platform telemetry export.
	// +optional
	SandboxObservability SandboxObservabilityConfig `yaml:"sandbox_observability" json:"sandboxObservability"`

	// Metering configures the optional region usage ledger.
	// +optional
	Metering MeteringConfig `yaml:"metering" json:"metering"`

	// Permissions
	// +optional
	// +kubebuilder:default={"*:*"}
	SchedulerPermissions []string `yaml:"scheduler_permissions" json:"schedulerPermissions"`
}

type SandboxObservabilityConfig struct {
	// Backend selects the historical query backend. Supported values: "disabled", "clickhouse".
	// Empty is treated as "disabled".
	// +optional
	// +kubebuilder:validation:Enum=disabled;clickhouse
	// +kubebuilder:default="disabled"
	Backend string `yaml:"backend" json:"backend"`
	// +optional
	ClickHouse SandboxObservabilityClickHouseConfig `yaml:"clickhouse" json:"clickHouse"`
}

type SandboxObservabilityClickHouseConfig struct {
	// DSN is the ClickHouse database/sql connection string. It may include credentials.
	// +optional
	DSN string `yaml:"dsn" json:"-"`
	// +optional
	// +kubebuilder:default="sandbox0_observability"
	Database string `yaml:"database" json:"database"`
	// +optional
	// +kubebuilder:default="sandbox_events"
	EventsTable string `yaml:"events_table" json:"eventsTable"`
	// +optional
	// +kubebuilder:default="sandbox_logs"
	LogsTable string `yaml:"logs_table" json:"logsTable"`
	// +optional
	// +kubebuilder:default="sandbox_runtime_samples"
	RuntimeSamplesTable string `yaml:"runtime_samples_table" json:"runtimeSamplesTable"`
	// RetentionDays controls ClickHouse TTL for the events table. It is kept as
	// the runtime alias for audit/lifecycle event retention.
	// +optional
	// +kubebuilder:default=90
	RetentionDays int `yaml:"retention_days" json:"retentionDays"`
	// LogsRetentionDays controls ClickHouse TTL for sandbox process logs.
	// +optional
	// +kubebuilder:default=7
	LogsRetentionDays int `yaml:"logs_retention_days" json:"logsRetentionDays"`
	// RuntimeSamplesRetentionDays controls ClickHouse TTL for runtime samples.
	// +optional
	// +kubebuilder:default=30
	RuntimeSamplesRetentionDays int `yaml:"runtime_samples_retention_days" json:"runtimeSamplesRetentionDays"`
	// ConnectTimeout bounds startup connection and schema checks.
	// +optional
	// +kubebuilder:default="10s"
	ConnectTimeout metav1.Duration `yaml:"connect_timeout" json:"connectTimeout"`
	// SkipSchemaMigration disables CREATE/ALTER TABLE at startup.
	// +optional
	SkipSchemaMigration bool `yaml:"skip_schema_migration" json:"skipSchemaMigration"`
}

func (c SandboxObservabilityConfig) BackendType() string {
	backend := strings.TrimSpace(c.Backend)
	if backend == "" {
		return SandboxObservabilityBackendDisabled
	}
	return backend
}

// LoadClusterGatewayConfig returns the cluster-gateway configuration.
func LoadClusterGatewayConfig() *ClusterGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadClusterGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &ClusterGatewayConfig{}
	}
	return cfg
}

func loadClusterGatewayConfig(path string) (*ClusterGatewayConfig, error) {
	cfg := &ClusterGatewayConfig{}
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	data = []byte(os.ExpandEnv(string(data)))

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}
