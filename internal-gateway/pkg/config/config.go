package config

import (
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
)

// Config holds all configuration for internal-gateway
type Config struct {
	// Server configuration
	HTTPPort int
	LogLevel string

	// Database configuration
	DatabaseURL string

	// Upstream services
	ManagerURL      string
	StorageProxyURL string

	// Authentication
	JWTSecret string

	// Internal authentication (for inter-service communication)
	InternalJWTPrivateKeyPath string

	// Rate limiting
	RateLimitRPS   int
	RateLimitBurst int

	// Timeouts
	ProxyTimeout      time.Duration
	ShutdownTimeout   time.Duration
	HealthCheckPeriod time.Duration

	// Feature flags
	EnableMetrics bool
	EnableAudit   bool
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		// Server
		HTTPPort: env.GetEnvInt("GATEWAY_HTTP_PORT", 8443),
		LogLevel: env.GetEnv("GATEWAY_LOG_LEVEL", "info"),

		// Database
		DatabaseURL: env.GetEnv("DATABASE_URL", "postgres://localhost:5432/sandbox0?sslmode=disable"),

		// Upstream services
		ManagerURL:      env.GetEnv("MANAGER_URL", "http://manager:8080"),
		StorageProxyURL: env.GetEnv("STORAGE_PROXY_URL", "http://storage-proxy:8081"),

		// Authentication
		JWTSecret: env.GetEnv("JWT_SECRET", ""),

		// Internal authentication
		InternalJWTPrivateKeyPath: env.GetEnv("INTERNAL_JWT_PRIVATE_KEY_PATH", "/secrets/internal_jwt_private.key"),

		// Rate limiting (per team)
		RateLimitRPS:   env.GetEnvInt("RATE_LIMIT_RPS", 100),
		RateLimitBurst: env.GetEnvInt("RATE_LIMIT_BURST", 200),

		// Timeouts
		ProxyTimeout:      env.GetEnvDuration("PROXY_TIMEOUT", 30*time.Second),
		ShutdownTimeout:   env.GetEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
		HealthCheckPeriod: env.GetEnvDuration("HEALTH_CHECK_PERIOD", 10*time.Second),

		// Features
		EnableMetrics: env.GetEnvBool("ENABLE_METRICS", true),
		EnableAudit:   env.GetEnvBool("ENABLE_AUDIT", true),
	}
}
