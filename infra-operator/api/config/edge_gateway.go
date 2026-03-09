// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EdgeGatewayConfig holds all configuration for edge-gateway.
type EdgeGatewayConfig struct {
	// Edition: "saas" or "self-hosted"
	// +optional
	// +kubebuilder:default="self-hosted"
	Edition string `yaml:"edition" json:"edition"`

	// Server configuration
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration (for API key validation)
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`

	// Upstream service
	DefaultInternalGatewayURL string `yaml:"default_internal_gateway_url" json:"-"`

	// Scheduler configuration (optional, for multi-cluster mode)
	// +optional
	SchedulerEnabled bool `yaml:"scheduler_enabled" json:"schedulerEnabled"`
	// +optional
	SchedulerURL string `yaml:"scheduler_url" json:"schedulerUrl"`
	// License file path used to unlock enterprise features.
	// Required when scheduler_enabled is true.
	// +optional
	LicenseFile string `yaml:"license_file" json:"-"`

	// Internal Authentication
	// +optional
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `yaml:"internal_auth_ttl" json:"internalAuthTTL"`
	// +optional
	// +kubebuilder:default="edge-gateway"
	InternalAuthCaller string `yaml:"internal_auth_caller" json:"internalAuthCaller"`

	// Cache configuration
	// +optional
	// +kubebuilder:default="30s"
	ClusterCacheTTL metav1.Duration `yaml:"cluster_cache_ttl" json:"clusterCacheTTL"`

	// Timeouts
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

	// Shared gateway configuration
	// +optional
	GatewayConfig `yaml:",inline" json:",inline"`

	// Registry config for control-plane image push credentials.
	// +optional
	// +kubebuilder:default={}
	Registry RegistryConfig `yaml:"registry" json:"-"`
}

// LoadEdgeGatewayConfig returns the edge-gateway configuration.
func LoadEdgeGatewayConfig() *EdgeGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadEdgeGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &EdgeGatewayConfig{}
	}
	return cfg
}

func loadEdgeGatewayConfig(path string) (*EdgeGatewayConfig, error) {
	cfg := &EdgeGatewayConfig{}
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
	cfg.GatewayConfig.ApplyDefaults()

	return cfg, nil
}

// GetOIDCProvider returns an OIDC provider by ID.
func (c *EdgeGatewayConfig) GetOIDCProvider(id string) *OIDCProviderConfig {
	for i := range c.OIDCProviders {
		if c.OIDCProviders[i].ID == id && c.OIDCProviders[i].Enabled {
			return &c.OIDCProviders[i]
		}
	}
	return nil
}

// GetEnabledOIDCProviders returns all enabled OIDC providers.
func (c *EdgeGatewayConfig) GetEnabledOIDCProviders() []OIDCProviderConfig {
	var providers []OIDCProviderConfig
	for _, p := range c.OIDCProviders {
		if p.Enabled {
			providers = append(providers, p)
		}
	}
	return providers
}
