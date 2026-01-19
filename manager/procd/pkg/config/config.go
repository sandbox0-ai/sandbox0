// Package config provides configuration for the Procd service.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for Procd.
type Config struct {
	// Sandbox identity
	SandboxID  string `yaml:"sandbox_id"`
	TemplateID string `yaml:"template_id"`
	NodeName   string `yaml:"node_name"`

	// Server configuration
	HTTPPort int    `yaml:"http_port"`
	LogLevel string `yaml:"log_level"`

	// Storage Proxy configuration
	StorageProxyBaseURL  string `yaml:"storage_proxy_base_url"`
	StorageProxyReplicas int    `yaml:"storage_proxy_replicas"`

	// File manager configuration
	RootPath string `yaml:"root_path"`

	// Cache configuration
	CacheMaxBytes int64         `yaml:"cache_max_bytes"`
	CacheTTL      time.Duration `yaml:"cache_ttl"`
}

// defaultConfig returns the default configuration
func defaultConfig() *Config {
	return &Config{
		HTTPPort:             49983,
		LogLevel:             "info",
		StorageProxyBaseURL:  "storage-proxy.sandbox0-system.svc.cluster.local",
		StorageProxyReplicas: 3,
		RootPath:             "/workspace",
		CacheMaxBytes:        100 * 1024 * 1024,
		CacheTTL:             30 * time.Second,
	}
}

var Cfg *Config

func init() {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	var err error
	Cfg, err = load(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		Cfg = defaultConfig()
	}
}

// LoadConfig returns the global configuration
func LoadConfig() *Config {
	return Cfg
}

// load loads configuration from a YAML file
func load(path string) (*Config, error) {
	// Default configuration
	cfg := defaultConfig()

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

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	// SandboxID and TemplateID can be empty during development
	return nil
}
