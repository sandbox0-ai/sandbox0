// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const DefaultFunctionRootDomain = "sandbox0.site"

// FunctionGatewayConfig holds all configuration for function-gateway.
type FunctionGatewayConfig struct {
	// Server configuration.
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration for API key validation and the function registry.
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`

	// DefaultClusterGatewayURL is used to read source sandboxes from the region's
	// data plane. Scheduler-aware cluster routing can replace this later.
	DefaultClusterGatewayURL string `yaml:"default_cluster_gateway_url" json:"-"`

	// FunctionRootDomain is the DNS root for function hosts.
	// +optional
	// +kubebuilder:default="sandbox0.site"
	FunctionRootDomain string `yaml:"function_root_domain" json:"functionRootDomain"`
	// FunctionRegionID is the DNS-safe region label placed under FunctionRootDomain.
	// When empty, PublicRegionID or RegionID is used.
	// +optional
	FunctionRegionID string `yaml:"function_region_id" json:"functionRegionId"`

	// Internal Authentication.
	// +optional
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `yaml:"internal_auth_ttl" json:"internalAuthTTL"`
	// +optional
	// +kubebuilder:default="function-gateway"
	InternalAuthCaller string `yaml:"internal_auth_caller" json:"internalAuthCaller"`

	// Timeouts.
	// +optional
	// +kubebuilder:default="30s"
	ProxyTimeout metav1.Duration `yaml:"proxy_timeout" json:"proxyTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	ServerReadTimeout metav1.Duration `yaml:"server_read_timeout" json:"serverReadTimeout"`
	// +optional
	// +kubebuilder:default="60s"
	ServerWriteTimeout metav1.Duration `yaml:"server_write_timeout" json:"serverWriteTimeout"`
	// +optional
	// +kubebuilder:default="120s"
	ServerIdleTimeout metav1.Duration `yaml:"server_idle_timeout" json:"serverIdleTimeout"`

	// Shared gateway configuration.
	// +optional
	GatewayConfig `yaml:",inline" json:",inline"`
}

// LoadFunctionGatewayConfig returns the function-gateway configuration.
func LoadFunctionGatewayConfig() *FunctionGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadFunctionGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &FunctionGatewayConfig{}
	}
	return cfg
}

func loadFunctionGatewayConfig(path string) (*FunctionGatewayConfig, error) {
	cfg := &FunctionGatewayConfig{}
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	data = []byte(os.ExpandEnv(string(data)))

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return cfg, nil
}
