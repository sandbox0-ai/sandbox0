package config

import (
	"github.com/sandbox0-ai/infra/pkg/env"
)

// Config holds the server configuration
type Config struct {
	// gRPC Server
	GRPCAddr string
	GRPCPort int

	// HTTP Management API
	HTTPAddr string
	HTTPPort int

	// Database
	DatabaseURL string

	// JuiceFS defaults
	MetaURL        string
	S3Bucket       string
	S3Region       string
	S3Endpoint     string
	S3AccessKey    string
	S3SecretKey    string
	S3SessionToken string

	DefaultCacheSize string
	DefaultCacheDir  string
	DefaultClusterId string

	// Security
	InternalAuthPublicKey string // Ed25519 public key for internal auth (base64 encoded)

	// Cache
	CacheRoot string

	// Monitoring
	MetricsEnabled bool
	MetricsPort    int

	// Logging
	LogLevel  string
	AuditLog  bool
	AuditFile string

	// Rate limiting
	MaxOpsPerSecond   int
	MaxBytesPerSecond int64

	// Kubernetes
	KubeconfigPath string // Path to kubeconfig file (empty for in-cluster config)
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() *Config {
	return &Config{
		GRPCAddr: env.GetEnv("GRPC_ADDR", "0.0.0.0"),
		GRPCPort: env.GetEnvInt("GRPC_PORT", 8080),

		HTTPAddr: env.GetEnv("HTTP_ADDR", "0.0.0.0"),
		HTTPPort: env.GetEnvInt("HTTP_PORT", 8081),

		DatabaseURL: env.GetEnv("DATABASE_URL", ""),

		MetaURL:        env.GetEnv("META_URL", ""),
		S3Bucket:       env.GetEnv("S3_BUCKET", ""),
		S3Region:       env.GetEnv("S3_REGION", "us-east-1"),
		S3Endpoint:     env.GetEnv("S3_ENDPOINT", ""),
		S3AccessKey:    env.GetEnv("S3_ACCESS_KEY", ""),
		S3SecretKey:    env.GetEnv("S3_SECRET_KEY", ""),
		S3SessionToken: env.GetEnv("S3_SESSION_TOKEN", ""),

		DefaultCacheSize: env.GetEnv("DEFAULT_CACHE_SIZE", "1G"),
		DefaultCacheDir:  env.GetEnv("DEFAULT_CACHE_DIR", "/var/lib/storage-proxy/cache"),
		DefaultClusterId: env.GetEnv("DEFAULT_CLUSTER_ID", "default"),

		InternalAuthPublicKey: env.GetEnv("INTERNAL_AUTH_PUBLIC_KEY", ""),

		CacheRoot: env.GetEnv("CACHE_ROOT", "/var/lib/storage-proxy/cache"),

		MetricsEnabled: env.GetEnvBool("METRICS_ENABLED", true),
		MetricsPort:    env.GetEnvInt("METRICS_PORT", 9090),

		LogLevel:  env.GetEnv("LOG_LEVEL", "info"),
		AuditLog:  env.GetEnvBool("AUDIT_LOG", true),
		AuditFile: env.GetEnv("AUDIT_FILE", "/var/log/storage-proxy/audit.log"),

		MaxOpsPerSecond:   env.GetEnvInt("MAX_OPS_PER_SECOND", 10000),
		MaxBytesPerSecond: env.GetEnvInt64("MAX_BYTES_PER_SECOND", 1<<30), // 1GB/s

		KubeconfigPath: env.GetEnv("KUBECONFIG_PATH", ""),
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Require either new internal auth or legacy JWT secret
	if c.InternalAuthPublicKey == "" {
		return ErrMissingAuthConfig
	}
	return nil
}

// Errors
var (
	ErrMissingAuthConfig = &ConfigError{"INTERNAL_AUTH_PUBLIC_KEY is required"}
)

// ConfigError represents a configuration error
type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return e.Message
}
