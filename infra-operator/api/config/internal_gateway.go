// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InternalGatewayConfig holds all configuration for internal-gateway.
type InternalGatewayConfig struct {
	// Server configuration
	// +optional
	// +kubebuilder:default=8443
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Upstream services
	// +optional
	ManagerURL string `yaml:"manager_url" json:"-"`
	// +optional
	StorageProxyURL string `yaml:"storage_proxy_url" json:"-"`

	// Internal authentication (for validating requests from edge-gateway and
	// generating tokens for downstream services)
	// AllowedCallers is the list of services allowed to call internal-gateway
	// Default: ["edge-gateway"], can include "scheduler" for multi-cluster mode
	// +optional
	// +kubebuilder:default={"edge-gateway","scheduler"}
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

	// Permissions
	// +optional
	// +kubebuilder:default={"*:*"}
	SchedulerPermissions []string `yaml:"scheduler_permissions" json:"schedulerPermissions"`
	// +optional
	// +kubebuilder:default={"sandboxvolume:read","sandboxvolume:write"}
	ProcdStoragePermissions []string `yaml:"procd_storage_permissions" json:"procdStoragePermissions"`
}

// LoadInternalGatewayConfig returns the internal-gateway configuration.
func LoadInternalGatewayConfig() *InternalGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadInternalGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &InternalGatewayConfig{}
	}
	return cfg
}

func loadInternalGatewayConfig(path string) (*InternalGatewayConfig, error) {
	cfg := &InternalGatewayConfig{}
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
