// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// StorageProxyConfig holds the storage runtime configuration.
type StorageProxyConfig struct {
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
	// +kubebuilder:default="storage_proxy"
	DatabaseSchema string `yaml:"database_schema" json:"databaseSchema"`

	// S0FS defaults
	ObjectStorageType string `yaml:"object_storage_type" json:"-"`
	MetaURL           string `yaml:"meta_url" json:"-"`
	S3Bucket          string `yaml:"s3_bucket" json:"-"`
	S3Region          string `yaml:"s3_region" json:"-"`
	S3Endpoint        string `yaml:"s3_endpoint" json:"-"`
	S3AccessKey       string `yaml:"s3_access_key" json:"-"`
	S3SecretKey       string `yaml:"s3_secret_key" json:"-"`
	S3SessionToken    string `yaml:"s3_session_token" json:"-"`

	// +optional
	// +kubebuilder:default="sandbox0"
	FilesystemName string `yaml:"filesystem_name" json:"filesystemName"`
	// +optional
	// +kubebuilder:default=4096
	FilesystemBlockSize int `yaml:"filesystem_block_size" json:"filesystemBlockSize"`
	// +optional
	// +kubebuilder:default="lz4"
	FilesystemCompression string `yaml:"filesystem_compression" json:"filesystemCompression"`
	// +optional
	// +kubebuilder:default=1
	FilesystemTrashDays int `yaml:"filesystem_trash_days" json:"filesystemTrashDays"`
	// +optional
	// +kubebuilder:default=10
	FilesystemMetaRetries int `yaml:"filesystem_meta_retries" json:"filesystemMetaRetries"`
	// +optional
	// +kubebuilder:default=20
	FilesystemMaxUpload int `yaml:"filesystem_max_upload" json:"filesystemMaxUpload"`
	// +optional
	// +kubebuilder:default="4Mi"
	S0FSSegmentTargetSize string `yaml:"s0fs_segment_target_size" json:"s0fsSegmentTargetSize"`
	// +optional
	// +kubebuilder:default="1m"
	S0FSCompactionInterval string `yaml:"s0fs_compaction_interval" json:"s0fsCompactionInterval"`
	// +optional
	// +kubebuilder:default="0.5"
	S0FSCompactionMinDeadRatio string `yaml:"s0fs_compaction_min_dead_ratio" json:"s0fsCompactionMinDeadRatio"`
	// +optional
	// +kubebuilder:default="1Mi"
	S0FSCompactionMinReclaimSize string `yaml:"s0fs_compaction_min_reclaim_size" json:"s0fsCompactionMinReclaimSize"`
	// +optional
	// +kubebuilder:default=true
	ObjectEncryptionEnabled bool `yaml:"object_encryption_enabled" json:"objectEncryptionEnabled"`
	// +optional
	ObjectEncryptionKeyPath string `yaml:"object_encryption_key_path" json:"objectEncryptionKeyPath,omitempty"`
	// +optional
	ObjectEncryptionPassphrase string `yaml:"object_encryption_passphrase" json:"objectEncryptionPassphrase,omitempty"`
	// +optional
	// +kubebuilder:default="aes256gcm-rsa"
	ObjectEncryptionAlgo string `yaml:"object_encryption_algo" json:"objectEncryptionAlgo"`

	// +optional
	// +kubebuilder:default="1s"
	FilesystemAttrTimeout string `yaml:"filesystem_attr_timeout" json:"filesystemAttrTimeout"`
	// +optional
	// +kubebuilder:default="1s"
	FilesystemEntryTimeout string `yaml:"filesystem_entry_timeout" json:"filesystemEntryTimeout"`
	// +optional
	// +kubebuilder:default="1s"
	FilesystemDirEntryTimeout string `yaml:"filesystem_dir_entry_timeout" json:"filesystemDirEntryTimeout"`

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
	// +kubebuilder:default="30s"
	DirectVolumeFileIdleTTL string `yaml:"direct_volume_file_idle_ttl" json:"directVolumeFileIdleTTL"`

	// +optional
	// +kubebuilder:default="/var/lib/storage-proxy/cache"
	CacheDir string `yaml:"cache_dir" json:"cacheDir"`
	// +optional
	// +kubebuilder:default="20Gi"
	CacheSizeLimit string `yaml:"cache_size_limit" json:"cacheSizeLimit"`
	// +optional
	// +kubebuilder:default="1Gi"
	LogSizeLimit string `yaml:"log_size_limit" json:"logSizeLimit"`
	// +optional
	// +kubebuilder:default="20Gi"
	VolumePortalCacheSizeLimit string `yaml:"volume_portal_cache_size_limit" json:"volumePortalCacheSizeLimit"`
	// +optional
	// +kubebuilder:default="5Gi"
	VolumePortalRootMinFree string `yaml:"volume_portal_root_min_free" json:"volumePortalRootMinFree"`

	DefaultClusterId string `yaml:"default_cluster_id" json:"-"`
	RegionID         string `yaml:"region_id" json:"-"`

	// Metering configures the optional region usage ledger.
	// +optional
	Metering MeteringConfig `yaml:"metering" json:"metering"`
	// CredentialStore configures secret storage for volume backend credentials.
	// +optional
	CredentialStore CredentialStoreConfig `yaml:"credential_store" json:"-"`

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

	// Watch events
	// +optional
	// +kubebuilder:default=true
	WatchEventsEnabled bool `yaml:"watch_events_enabled" json:"watchEventsEnabled"`
	// +optional
	// +kubebuilder:default=256
	WatchEventQueueSize int `yaml:"watch_event_queue_size" json:"watchEventQueueSize"`

	// Snapshot restore
	// +optional
	// +kubebuilder:default="30s"
	RestoreRemountTimeout string `yaml:"restore_remount_timeout" json:"restoreRemountTimeout"`

	// Kubernetes
	// +optional
	KubeconfigPath string `yaml:"kubeconfig_path" json:"kubeconfigPath"` // Path to kubeconfig file (empty for in-cluster config)
}

// ReadStorageProxyConfig loads storage runtime configuration from an explicit
// path and returns parsing errors to the embedding process.
func ReadStorageProxyConfig(path string) (*StorageProxyConfig, error) {
	return loadStorageProxyConfig(path)
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
	if c.ObjectEncryptionEnabled && c.ObjectEncryptionKeyPath == "" {
		return &ConfigError{Message: "object encryption enabled but object_encryption_key_path is empty"}
	}
	return nil
}

// ConfigError represents a configuration error.
type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return e.Message
}
