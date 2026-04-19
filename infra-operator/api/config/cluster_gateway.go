// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// AllowedCallers is the list of services allowed to call cluster-gateway
	// Default: ["regional-gateway"], can include "scheduler" for multi-cluster mode
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

	// Permissions
	// +optional
	// +kubebuilder:default={"*:*"}
	SchedulerPermissions []string `yaml:"scheduler_permissions" json:"schedulerPermissions"`
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
