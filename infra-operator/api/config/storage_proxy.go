// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// StorageProxyConfig holds the server configuration.
type StorageProxyConfig struct {
	// gRPC Server
	GRPCAddr string `yaml:"grpc_addr" json:"grpcAddr"`
	GRPCPort int    `yaml:"grpc_port" json:"grpcPort"`

	// HTTP Management API
	HTTPAddr string `yaml:"http_addr" json:"httpAddr"`
	HTTPPort int    `yaml:"http_port" json:"httpPort"`

	// Database
	DatabaseURL string `yaml:"database_url" json:"databaseUrl"`

	// JuiceFS defaults
	MetaURL        string `yaml:"meta_url" json:"metaUrl"`
	S3Bucket       string `yaml:"s3_bucket" json:"s3Bucket"`
	S3Region       string `yaml:"s3_region" json:"s3Region"`
	S3Endpoint     string `yaml:"s3_endpoint" json:"s3Endpoint"`
	S3AccessKey    string `yaml:"s3_access_key" json:"s3AccessKey"`
	S3SecretKey    string `yaml:"s3_secret_key" json:"s3SecretKey"`
	S3SessionToken string `yaml:"s3_session_token" json:"s3SessionToken"`

	DefaultCacheSize string `yaml:"default_cache_size" json:"defaultCacheSize"`
	CacheDir         string `yaml:"cache_dir" json:"cacheDir"`
	DefaultClusterId string `yaml:"default_cluster_id" json:"defaultClusterId"`

	// Monitoring
	MetricsEnabled bool `yaml:"metrics_enabled" json:"metricsEnabled"`
	MetricsPort    int  `yaml:"metrics_port" json:"metricsPort"`

	// Logging
	LogLevel  string `yaml:"log_level" json:"logLevel"`
	AuditLog  bool   `yaml:"audit_log" json:"auditLog"`
	AuditFile string `yaml:"audit_file" json:"auditFile"`

	// Rate limiting
	MaxOpsPerSecond   int   `yaml:"max_ops_per_second" json:"maxOpsPerSecond"`
	MaxBytesPerSecond int64 `yaml:"max_bytes_per_second" json:"maxBytesPerSecond"`

	// Kubernetes
	KubeconfigPath string `yaml:"kubeconfig_path" json:"kubeconfigPath"` // Path to kubeconfig file (empty for in-cluster config)
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
