package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for scheduler
type Config struct {
	// Server configuration
	HTTPPort int    `yaml:"http_port"`
	LogLevel string `yaml:"log_level"`

	// Database configuration
	DatabaseURL string `yaml:"database_url"`

	// Reconciler configuration
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`

	// Timeouts
	ClusterTimeout  time.Duration `yaml:"cluster_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// defaultConfig returns the default configuration
func defaultConfig() *Config {
	return &Config{
		HTTPPort:          8080,
		LogLevel:          "info",
		DatabaseURL:       "postgres://sandbox0:sandbox0@postgresql:5432/sandbox0?sslmode=disable",
		ReconcileInterval: 30 * time.Second,
		ClusterTimeout:    10 * time.Second,
		ShutdownTimeout:   30 * time.Second,
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
