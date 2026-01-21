package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// InternalGatewayConfig holds all configuration for internal-gateway.
type InternalGatewayConfig struct {
	// Server configuration
	HTTPPort int    `yaml:"http_port"`
	LogLevel string `yaml:"log_level"`

	// Upstream services
	ManagerURL      string `yaml:"manager_url"`
	StorageProxyURL string `yaml:"storage_proxy_url"`

	// Internal authentication (for validating requests from edge-gateway and
	// generating tokens for downstream services)
	// AllowedCallers is the list of services allowed to call internal-gateway
	// Default: ["edge-gateway"], can include "scheduler" for multi-cluster mode
	AllowedCallers []string `yaml:"allowed_callers"`

	// Timeouts
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	HealthCheckPeriod time.Duration `yaml:"health_check_period"`
}

// DefaultInternalGatewayConfig returns the default configuration.
func DefaultInternalGatewayConfig() *InternalGatewayConfig {
	return &InternalGatewayConfig{
		HTTPPort:          8443,
		LogLevel:          "info",
		ManagerURL:        "http://manager.sandbox0-system:8080",
		StorageProxyURL:   "http://storage-proxy.sandbox0-system:8081",
		AllowedCallers:    []string{"edge-gateway", "scheduler"},
		ShutdownTimeout:   30 * time.Second,
		HealthCheckPeriod: 10 * time.Second,
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
