package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"gopkg.in/yaml.v3"
)

const (
	SandboxObservabilityBackendDisabled   = "disabled"
	SandboxObservabilityBackendClickHouse = "clickhouse"
)

// ClusterGatewayConfig holds all configuration for cluster-gateway.
type ClusterGatewayConfig struct {
	// ClusterID is the trusted data-plane cluster identity attached to audit facts.
	ClusterID string `yaml:"cluster_id" json:"-"`
	// Server configuration
	HTTPPort int    `yaml:"http_port" json:"httpPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Upstream services
	ManagerURL string `yaml:"manager_url" json:"-"`

	// Internal authentication (for validating requests from regional-gateway and
	// generating tokens for downstream services)
	// AuthMode controls which authentication modes are accepted on /api/v1.
	// Allowed values: "internal", "public", "both".
	AuthMode string `yaml:"auth_mode" json:"authMode"`
	// AllowedCallers is the list of services allowed to call cluster-gateway.
	// Default: ["regional-gateway","scheduler"].
	AllowedCallers []string `yaml:"allowed_callers" json:"allowedCallers"`

	// Timeouts
	ShutdownTimeout   Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	HealthCheckPeriod Duration `yaml:"health_check_period" json:"healthCheckPeriod"`

	// Proxy configuration
	ProxyTimeout Duration `yaml:"proxy_timeout" json:"proxyTimeout"`

	// Public gateway (external auth) configuration
	DatabaseURL string `yaml:"database_url" json:"-"`
	// License file path used to unlock cluster-gateway enterprise features.
	// Required when OIDC providers or centralized sandbox audit are enabled.
	LicenseFile      string `yaml:"license_file" json:"-"`
	DatabaseMaxConns int    `yaml:"database_max_conns" json:"databaseMaxConns"`
	DatabaseMinConns int    `yaml:"database_min_conns" json:"databaseMinConns"`

	// Shared gateway configuration
	GatewayConfig `yaml:",inline" json:",inline"`

	// SandboxObservability configures the per-sandbox historical observability
	// query backend. It is separate from platform telemetry export.
	SandboxObservability SandboxObservabilityConfig `yaml:"sandbox_observability" json:"sandboxObservability"`

	// Metering configures the optional region usage ledger.
	Metering MeteringConfig `yaml:"metering" json:"metering"`

	// Permissions
	SchedulerPermissions []string `yaml:"scheduler_permissions" json:"schedulerPermissions"`
}

type SandboxObservabilityConfig struct {
	// Backend selects the historical query backend. Supported values: "disabled", "clickhouse".
	// Empty is treated as "disabled".
	Backend string `yaml:"backend" json:"backend"`
	// AuditEnabled enables licensed centralized per-sandbox audit ingest and query.
	AuditEnabled bool `yaml:"audit_enabled" json:"auditEnabled"`
	// AuditDeliveryMode controls non-mutating API and public exposure admission.
	// Mutations always require canonical ClickHouse acknowledgement.
	AuditDeliveryMode sandboxobservability.AuditDeliveryMode `yaml:"audit_delivery_mode" json:"auditDeliveryMode"`
	// AuditSpoolDir is the fsync-backed local delivery buffer for signed audit
	// events that have not yet been acknowledged by ClickHouse.
	// It is not an audit system of record.
	AuditSpoolDir string                               `yaml:"audit_spool_dir" json:"-"`
	ClickHouse    SandboxObservabilityClickHouseConfig `yaml:"clickhouse" json:"clickHouse"`
}

type SandboxObservabilityClickHouseConfig struct {
	// DSN is the ClickHouse database/sql connection string. It may include credentials.
	DSN                 string `yaml:"dsn" json:"-"`
	Database            string `yaml:"database" json:"database"`
	EventsTable         string `yaml:"events_table" json:"eventsTable"`
	LogsTable           string `yaml:"logs_table" json:"logsTable"`
	RuntimeSamplesTable string `yaml:"runtime_samples_table" json:"runtimeSamplesTable"`
	// RetentionDays controls ClickHouse TTL for the events table. It is kept as
	// the runtime alias for audit/lifecycle event retention.
	RetentionDays int `yaml:"retention_days" json:"retentionDays"`
	// LogsRetentionDays controls ClickHouse TTL for sandbox process logs.
	LogsRetentionDays int `yaml:"logs_retention_days" json:"logsRetentionDays"`
	// RuntimeSamplesRetentionDays controls ClickHouse TTL for runtime samples.
	RuntimeSamplesRetentionDays int `yaml:"runtime_samples_retention_days" json:"runtimeSamplesRetentionDays"`
	// ConnectTimeout bounds startup connection and schema checks.
	ConnectTimeout Duration `yaml:"connect_timeout" json:"connectTimeout"`
	// SkipSchemaMigration disables CREATE/ALTER TABLE at startup.
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
	applyClusterGatewayDefaults(cfg)
	return cfg
}

func loadClusterGatewayConfig(path string) (*ClusterGatewayConfig, error) {
	cfg := &ClusterGatewayConfig{}
	if path == "" {
		applyClusterGatewayDefaults(cfg)
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

	applyClusterGatewayDefaults(cfg)
	return cfg, nil
}

func applyClusterGatewayDefaults(cfg *ClusterGatewayConfig) {
	if cfg == nil {
		return
	}
	cfg.SandboxObservability.AuditDeliveryMode = sandboxobservability.NormalizeAuditDeliveryMode(cfg.SandboxObservability.AuditDeliveryMode)
}
