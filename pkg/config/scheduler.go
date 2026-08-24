package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SchedulerConfig holds all configuration for scheduler.
type SchedulerConfig struct {
	// Server configuration
	HTTPPort int    `yaml:"http_port" json:"httpPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration
	DatabaseURL string `yaml:"database_url" json:"-"`
	// License file path used to unlock enterprise features.
	LicenseFile string `yaml:"license_file" json:"-"`

	// ClusterCacheTTL bounds stale cluster routing metadata used for existing
	// sandbox requests. New claims always query live PostgreSQL capacity.
	ClusterCacheTTL Duration `yaml:"cluster_cache_ttl" json:"clusterCacheTtl"`

	// Timeouts
	ShutdownTimeout Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`

	ReadTimeout Duration `yaml:"read_timeout" json:"readTimeout"`

	WriteTimeout Duration `yaml:"write_timeout" json:"writeTimeout"`

	IdleTimeout Duration `yaml:"idle_timeout" json:"idleTimeout"`

	ProxyTimeout Duration `yaml:"proxy_timeout" json:"proxyTimeout"`

	// Database Pool configuration
	DatabasePool DatabasePoolConfig `yaml:"database_pool" json:"databasePool"`

	// RegistryPushRegistry is the registry hostname used for image pushes.
	RegistryPushRegistry string `yaml:"registry_push_registry" json:"-"`

	// RegistryPullRegistry is the registry hostname reachable by sandbox nodes.
	RegistryPullRegistry string `yaml:"registry_pull_registry" json:"-"`

	// RegistryInternalRegistry is the registry service endpoint reserved for server-side access.
	RegistryInternalRegistry string `yaml:"registry_internal_registry" json:"-"`

	// TeamTemplateMemoryPerCPU is derived from manager platform configuration.
	TeamTemplateMemoryPerCPU string `yaml:"team_template_memory_per_cpu" json:"-"`
	// SandboxMaxMemory is derived from manager platform configuration.
	SandboxMaxMemory string `yaml:"sandbox_max_memory" json:"-"`
}

type DatabasePoolConfig struct {
	MaxConns        int32    `yaml:"max_conns" json:"maxConns"`
	MinConns        int32    `yaml:"min_conns" json:"minConns"`
	MaxConnLifetime Duration `yaml:"max_conn_lifetime" json:"maxConnLifetime"`
	MaxConnIdleTime Duration `yaml:"max_conn_idle_time" json:"maxConnIdleTime"`
}

// LoadSchedulerConfig returns the scheduler configuration.
func LoadSchedulerConfig() *SchedulerConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadSchedulerConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &SchedulerConfig{}
	}
	return cfg
}

func loadSchedulerConfig(path string) (*SchedulerConfig, error) {
	cfg := &SchedulerConfig{}
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
