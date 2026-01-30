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
	// +optional
	// +kubebuilder:default="0.0.0.0"
	GRPCAddr string `yaml:"grpc_addr" json:"grpcAddr"`
	// +optional
	// +kubebuilder:default=8080
	GRPCPort int `yaml:"grpc_port" json:"grpcPort"`

	// HTTP Management API
	// +optional
	// +kubebuilder:default="0.0.0.0"
	HTTPAddr string `yaml:"http_addr" json:"httpAddr"`
	// +optional
	// +kubebuilder:default=8081
	HTTPPort int `yaml:"http_port" json:"httpPort"`

	// Database
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=5
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`
	// +optional
	// +kubebuilder:default="sp"
	DatabaseSchema string `yaml:"database_schema" json:"databaseSchema"`

	// JuiceFS defaults
	MetaURL        string `yaml:"meta_url" json:"-"`
	S3Bucket       string `yaml:"s3_bucket" json:"-"`
	S3Region       string `yaml:"s3_region" json:"-"`
	S3Endpoint     string `yaml:"s3_endpoint" json:"-"`
	S3AccessKey    string `yaml:"s3_access_key" json:"-"`
	S3SecretKey    string `yaml:"s3_secret_key" json:"-"`
	S3SessionToken string `yaml:"s3_session_token" json:"-"`

	// +optional
	// +kubebuilder:default="sandbox0"
	JuiceFSName string `yaml:"juicefs_name" json:"juicefsName"`
	// +optional
	// +kubebuilder:default=4096
	JuiceFSBlockSize int `yaml:"juicefs_block_size" json:"juicefsBlockSize"`
	// +optional
	// +kubebuilder:default="lz4"
	JuiceFSCompression string `yaml:"juicefs_compression" json:"juicefsCompression"`
	// +optional
	// +kubebuilder:default=1
	JuiceFSTrashDays int `yaml:"juicefs_trash_days" json:"juicefsTrashDays"`
	// +optional
	// +kubebuilder:default=10
	JuiceFSMetaRetries int `yaml:"juicefs_meta_retries" json:"juicefsMetaRetries"`
	// +optional
	// +kubebuilder:default=20
	JuiceFSMaxUpload int `yaml:"juicefs_max_upload" json:"juicefsMaxUpload"`

	// +optional
	// +kubebuilder:default="1s"
	JuiceFSAttrTimeout string `yaml:"juicefs_attr_timeout" json:"juicefsAttrTimeout"`
	// +optional
	// +kubebuilder:default="1s"
	JuiceFSEntryTimeout string `yaml:"juicefs_entry_timeout" json:"juicefsEntryTimeout"`
	// +optional
	// +kubebuilder:default="1s"
	JuiceFSDirEntryTimeout string `yaml:"juicefs_dir_entry_timeout" json:"juicefsDirEntryTimeout"`

	// Coordination
	// +optional
	// +kubebuilder:default="5s"
	HeartbeatInterval string `yaml:"heartbeat_interval" json:"heartbeatInterval"`
	// +optional
	// +kubebuilder:default=15
	HeartbeatTimeout int `yaml:"heartbeat_timeout" json:"heartbeatTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	FlushTimeout string `yaml:"flush_timeout" json:"flushTimeout"`
	// +optional
	// +kubebuilder:default="60s"
	CleanupInterval string `yaml:"cleanup_interval" json:"cleanupInterval"`

	// +optional
	// +kubebuilder:default="1G"
	DefaultCacheSize string `yaml:"default_cache_size" json:"defaultCacheSize"`
	// +optional
	// +kubebuilder:default="/var/lib/storage-proxy/cache"
	CacheDir         string `yaml:"cache_dir" json:"cacheDir"`
	DefaultClusterId string `yaml:"default_cluster_id" json:"-"`

	// Monitoring
	// +optional
	// +kubebuilder:default=true
	MetricsEnabled bool `yaml:"metrics_enabled" json:"metricsEnabled"`
	// +optional
	// +kubebuilder:default=9090
	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	// Logging
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`
	// +optional
	// +kubebuilder:default=true
	AuditLog bool `yaml:"audit_log" json:"auditLog"`
	// +optional
	// +kubebuilder:default="/var/log/storage-proxy/audit.log"
	AuditFile string `yaml:"audit_file" json:"auditFile"`

	// Timeouts
	// +optional
	// +kubebuilder:default="15s"
	HTTPReadTimeout string `yaml:"http_read_timeout" json:"httpReadTimeout"`
	// +optional
	// +kubebuilder:default="15s"
	HTTPWriteTimeout string `yaml:"http_write_timeout" json:"httpWriteTimeout"`
	// +optional
	// +kubebuilder:default="60s"
	HTTPIdleTimeout string `yaml:"http_idle_timeout" json:"httpIdleTimeout"`

	// Rate limiting
	// +optional
	// +kubebuilder:default=10000
	MaxOpsPerSecond int `yaml:"max_ops_per_second" json:"maxOpsPerSecond"`
	// +optional
	// +kubebuilder:default=1073741824
	MaxBytesPerSecond int64 `yaml:"max_bytes_per_second" json:"maxBytesPerSecond"`

	// Watch events
	// +optional
	// +kubebuilder:default=true
	WatchEventsEnabled bool `yaml:"watch_events_enabled" json:"watchEventsEnabled"`
	// +optional
	// +kubebuilder:default=256
	WatchEventQueueSize int `yaml:"watch_event_queue_size" json:"watchEventQueueSize"`

	// Kubernetes
	// +optional
	KubeconfigPath string `yaml:"kubeconfig_path" json:"kubeconfigPath"` // Path to kubeconfig file (empty for in-cluster config)
}

// LoadStorageProxyConfig returns the storage-proxy configuration.
func LoadStorageProxyConfig() *StorageProxyConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadStorageProxyConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &StorageProxyConfig{}
	}
	return cfg
}

func loadStorageProxyConfig(path string) (*StorageProxyConfig, error) {
	cfg := &StorageProxyConfig{}
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
