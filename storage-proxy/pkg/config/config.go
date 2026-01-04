package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the server configuration
type Config struct {
	// gRPC Server
	GRPCAddr string
	GRPCPort int

	// HTTP Management API
	HTTPAddr string
	HTTPPort int

	// JuiceFS defaults
	DefaultMetaURL    string
	DefaultS3Region   string
	DefaultS3Endpoint string
	DefaultCacheSize  string
	DefaultCacheDir   string

	// Security
	JWTSecret string

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
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() *Config {
	return &Config{
		GRPCAddr: getEnv("GRPC_ADDR", "0.0.0.0"),
		GRPCPort: getEnvInt("GRPC_PORT", 8080),

		HTTPAddr: getEnv("HTTP_ADDR", "0.0.0.0"),
		HTTPPort: getEnvInt("HTTP_PORT", 8081),

		DefaultMetaURL:    getEnv("DEFAULT_META_URL", ""),
		DefaultS3Region:   getEnv("DEFAULT_S3_REGION", "us-east-1"),
		DefaultS3Endpoint: getEnv("DEFAULT_S3_ENDPOINT", ""),
		DefaultCacheSize:  getEnv("DEFAULT_CACHE_SIZE", "1G"),
		DefaultCacheDir:   getEnv("DEFAULT_CACHE_DIR", "/var/lib/storage-proxy/cache"),

		JWTSecret: getEnv("JWT_SECRET", ""),

		CacheRoot: getEnv("CACHE_ROOT", "/var/lib/storage-proxy/cache"),

		MetricsEnabled: getEnvBool("METRICS_ENABLED", true),
		MetricsPort:    getEnvInt("METRICS_PORT", 9090),

		LogLevel:  getEnv("LOG_LEVEL", "info"),
		AuditLog:  getEnvBool("AUDIT_LOG", true),
		AuditFile: getEnv("AUDIT_FILE", "/var/log/storage-proxy/audit.log"),

		MaxOpsPerSecond:   getEnvInt("MAX_OPS_PER_SECOND", 10000),
		MaxBytesPerSecond: getEnvInt64("MAX_BYTES_PER_SECOND", 1<<30), // 1GB/s
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.JWTSecret == "" {
		return ErrMissingJWTSecret
	}
	return nil
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

// Errors
var (
	ErrMissingJWTSecret = &ConfigError{"JWT_SECRET is required"}
)

// ConfigError represents a configuration error
type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	return e.Message
}
