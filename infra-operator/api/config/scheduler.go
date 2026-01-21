package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// SchedulerConfig holds all configuration for scheduler.
type SchedulerConfig struct {
	// Server configuration
	HTTPPort int    `yaml:"http_port"`
	LogLevel string `yaml:"log_level"`

	// Database configuration
	DatabaseURL string `yaml:"database_url"`

	// Reconciler configuration
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`

	// Timeouts
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// DefaultSchedulerConfig returns the default configuration.
func DefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		HTTPPort:          8080,
		LogLevel:          "info",
		DatabaseURL:       "",
		ReconcileInterval: 30 * time.Second,
		ShutdownTimeout:   30 * time.Second,
	}
}

// LoadSchedulerConfig returns the scheduler configuration.
func LoadSchedulerConfig() *SchedulerConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadSchedulerConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		cfg = DefaultSchedulerConfig()
	}
	return cfg
}

func loadSchedulerConfig(path string) (*SchedulerConfig, error) {
	cfg := DefaultSchedulerConfig()
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
