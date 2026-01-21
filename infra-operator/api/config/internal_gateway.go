// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"gopkg.in/yaml.v3"
)

// InternalGatewayConfig holds all configuration for internal-gateway.
type InternalGatewayConfig struct {
	// Server configuration
	HTTPPort int    `yaml:"http_port" json:"httpPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Upstream services
	ManagerURL      string `yaml:"manager_url" json:"managerUrl"`
	StorageProxyURL string `yaml:"storage_proxy_url" json:"storageProxyUrl"`

	// Internal authentication (for validating requests from edge-gateway and
	// generating tokens for downstream services)
	// AllowedCallers is the list of services allowed to call internal-gateway
	// Default: ["edge-gateway"], can include "scheduler" for multi-cluster mode
	AllowedCallers []string `yaml:"allowed_callers" json:"allowedCallers"`

	// Timeouts
	ShutdownTimeout   metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	HealthCheckPeriod metav1.Duration `yaml:"health_check_period" json:"healthCheckPeriod"`
}

// DefaultInternalGatewayConfig returns the default configuration.
func DefaultInternalGatewayConfig() *InternalGatewayConfig {
	return &InternalGatewayConfig{
		HTTPPort:          8443,
		LogLevel:          "info",
		ManagerURL:        "http://manager.sandbox0-system:8080",
		StorageProxyURL:   "http://storage-proxy.sandbox0-system:8081",
		AllowedCallers:    []string{"edge-gateway", "scheduler"},
		ShutdownTimeout:   metav1.Duration{Duration: 30 * time.Second},
		HealthCheckPeriod: metav1.Duration{Duration: 10 * time.Second},
	}
}

// LoadInternalGatewayConfig returns the internal-gateway configuration.
func LoadInternalGatewayConfig() *InternalGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadInternalGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		cfg = DefaultInternalGatewayConfig()
	}
	return cfg
}

func loadInternalGatewayConfig(path string) (*InternalGatewayConfig, error) {
	cfg := DefaultInternalGatewayConfig()
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
