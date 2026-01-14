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

	// Upstream services
	ManagerURL      string
	StorageProxyURL string

	// Internal authentication (for validating requests from edge-gateway and
	// generating tokens for downstream services)
	InternalJWTPrivateKeyPath string

	// Timeouts
	ProxyTimeout      time.Duration
	ShutdownTimeout   time.Duration
	HealthCheckPeriod time.Duration
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		// Server
		HTTPPort: env.GetEnvInt("INTERNAL_GATEWAY_HTTP_PORT", 8443),
		LogLevel: env.GetEnv("INTERNAL_GATEWAY_LOG_LEVEL", "info"),

		// Upstream services
		ManagerURL:      env.GetEnv("MANAGER_URL", "http://manager.sandbox0-system:8080"),
		StorageProxyURL: env.GetEnv("STORAGE_PROXY_URL", "http://storage-proxy.sandbox0-system:8081"),

		// Internal authentication
		InternalJWTPrivateKeyPath: env.GetEnv("INTERNAL_JWT_PRIVATE_KEY_PATH", "/secrets/internal_jwt_private.key"),

		// Timeouts
		ProxyTimeout:      env.GetEnvDuration("PROXY_TIMEOUT", 30*time.Second),
		ShutdownTimeout:   env.GetEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
		HealthCheckPeriod: env.GetEnvDuration("HEALTH_CHECK_PERIOD", 10*time.Second),
	}
}
