package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// StorageProxyConfig holds the server configuration.
type StorageProxyConfig struct {
	// gRPC Server
	GRPCAddr string `yaml:"grpc_addr"`
	GRPCPort int    `yaml:"grpc_port"`

	// HTTP Management API
	HTTPAddr string `yaml:"http_addr"`
	HTTPPort int    `yaml:"http_port"`

	// Database
	DatabaseURL string `yaml:"database_url"`

	// JuiceFS defaults
	MetaURL        string `yaml:"meta_url"`
	S3Bucket       string `yaml:"s3_bucket"`
	S3Region       string `yaml:"s3_region"`
	S3Endpoint     string `yaml:"s3_endpoint"`
	S3AccessKey    string `yaml:"s3_access_key"`
	S3SecretKey    string `yaml:"s3_secret_key"`
	S3SessionToken string `yaml:"s3_session_token"`

	DefaultCacheSize string `yaml:"default_cache_size"`
	CacheDir         string `yaml:"cache_dir"`
	DefaultClusterId string `yaml:"default_cluster_id"`

	// Monitoring
	MetricsEnabled bool `yaml:"metrics_enabled"`
	MetricsPort    int  `yaml:"metrics_port"`

	// Logging
	LogLevel  string `yaml:"log_level"`
	AuditLog  bool   `yaml:"audit_log"`
	AuditFile string `yaml:"audit_file"`

	// Rate limiting
	MaxOpsPerSecond   int   `yaml:"max_ops_per_second"`
	MaxBytesPerSecond int64 `yaml:"max_bytes_per_second"`

	// Kubernetes
	KubeconfigPath string `yaml:"kubeconfig_path"` // Path to kubeconfig file (empty for in-cluster config)
}

// DefaultStorageProxyConfig returns the default configuration.
func DefaultStorageProxyConfig() *StorageProxyConfig {
	return &StorageProxyConfig{
		GRPCAddr:          "0.0.0.0",
		GRPCPort:          8080,
		HTTPAddr:          "0.0.0.0",
		HTTPPort:          8081,
		DatabaseURL:       "",
		S3Region:          "us-east-1",
		DefaultCacheSize:  "1G",
		CacheDir:          "/var/lib/storage-proxy/cache",
		DefaultClusterId:  "default",
		MetricsEnabled:    true,
		MetricsPort:       9090,
		LogLevel:          "info",
		AuditLog:          true,
		AuditFile:         "/var/log/storage-proxy/audit.log",
		MaxOpsPerSecond:   10000,
		MaxBytesPerSecond: 1 << 30, // 1GB/s
		KubeconfigPath:    "",
	}
}

// LoadStorageProxyConfig returns the storage-proxy configuration.
func LoadStorageProxyConfig() *StorageProxyConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadStorageProxyConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		cfg = DefaultStorageProxyConfig()
	}
	return cfg
}

func loadStorageProxyConfig(path string) (*StorageProxyConfig, error) {
	cfg := DefaultStorageProxyConfig()
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

// Validate validates the configuration.
func (c *StorageProxyConfig) Validate() error {
	return nil
}

// ConfigError represents a configuration error.
type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return e.Message
}
