package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// RegionalGatewayConfig holds all configuration for regional-gateway.
type RegionalGatewayConfig struct {
	// Edition: "saas" or "self-hosted"
	Edition string `yaml:"edition" json:"edition"`

	// AuthMode controls how human-facing authentication is handled.
	// Allowed values: "self_hosted", "federated_global".
	AuthMode string `yaml:"auth_mode" json:"authMode"`

	// Server configuration
	HTTPPort int    `yaml:"http_port" json:"httpPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration (for API key validation)
	DatabaseURL      string `yaml:"database_url" json:"-"`
	DatabaseMaxConns int    `yaml:"database_max_conns" json:"databaseMaxConns"`
	DatabaseMinConns int    `yaml:"database_min_conns" json:"databaseMinConns"`

	// Upstream service
	DefaultClusterGatewayURL string `yaml:"default_cluster_gateway_url" json:"-"`

	// Scheduler configuration (optional, for multi-cluster mode)
	SchedulerEnabled bool   `yaml:"scheduler_enabled" json:"-"`
	SchedulerURL     string `yaml:"scheduler_url" json:"schedulerUrl"`
	// License file path used to unlock enterprise features.
	// Required when scheduler_enabled is true.
	LicenseFile string `yaml:"license_file" json:"-"`

	// Internal Authentication
	InternalAuthTTL    Duration `yaml:"internal_auth_ttl" json:"internalAuthTTL"`
	InternalAuthCaller string   `yaml:"internal_auth_caller" json:"internalAuthCaller"`

	// Cache configuration
	ClusterCacheTTL Duration `yaml:"cluster_cache_ttl" json:"clusterCacheTTL"`

	// Timeouts
	ProxyTimeout       Duration `yaml:"proxy_timeout" json:"proxyTimeout"`
	ShutdownTimeout    Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	ServerReadTimeout  Duration `yaml:"server_read_timeout" json:"serverReadTimeout"`
	ServerWriteTimeout Duration `yaml:"server_write_timeout" json:"serverWriteTimeout"`
	ServerIdleTimeout  Duration `yaml:"server_idle_timeout" json:"serverIdleTimeout"`

	// Shared gateway configuration
	GatewayConfig `yaml:",inline" json:",inline"`

	// Metering configures the optional region usage ledger.
	Metering MeteringConfig `yaml:"metering" json:"metering"`

	// Registry config for control-plane image push credentials.
	Registry RegistryConfig `yaml:"registry" json:"-"`
}

// LoadRegionalGatewayConfig returns the regional-gateway configuration.
func LoadRegionalGatewayConfig() *RegionalGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadRegionalGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &RegionalGatewayConfig{}
	}
	return cfg
}

func loadRegionalGatewayConfig(path string) (*RegionalGatewayConfig, error) {
	cfg := &RegionalGatewayConfig{}
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

// GetOIDCProvider returns an OIDC provider by ID.
func (c *RegionalGatewayConfig) GetOIDCProvider(id string) *OIDCProviderConfig {
	for i := range c.OIDCProviders {
		if c.OIDCProviders[i].ID == id && c.OIDCProviders[i].Enabled {
			return &c.OIDCProviders[i]
		}
	}
	return nil
}

// GetEnabledOIDCProviders returns all enabled OIDC providers.
func (c *RegionalGatewayConfig) GetEnabledOIDCProviders() []OIDCProviderConfig {
	var providers []OIDCProviderConfig
	for _, p := range c.OIDCProviders {
		if p.Enabled {
			providers = append(providers, p)
		}
	}
	return providers
}
