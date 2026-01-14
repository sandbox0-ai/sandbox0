package config

import (
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
)

// Config holds all configuration for edge-gateway
type Config struct {
	// Server configuration
	HTTPPort int
	LogLevel string

	// Database configuration (for API key validation)
	DatabaseURL string

	// Upstream service
	InternalGatewayURL string

	// Authentication
	JWTSecret string

	// Internal authentication (for generating tokens to internal-gateway)
	InternalJWTPrivateKeyPath string

	// Rate limiting
	RateLimitRPS   int
	RateLimitBurst int

	// Timeouts
	ProxyTimeout    time.Duration
	ShutdownTimeout time.Duration
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		// Server
		HTTPPort: env.GetEnvInt("EDGE_GATEWAY_HTTP_PORT", 8080),
		LogLevel: env.GetEnv("EDGE_GATEWAY_LOG_LEVEL", "info"),

		// Database
		DatabaseURL: env.GetEnv("DATABASE_URL", "postgres://localhost:5432/sandbox0?sslmode=disable"),

		// Upstream service
		InternalGatewayURL: env.GetEnv("INTERNAL_GATEWAY_URL", "http://internal-gateway.sandbox0-system:8443"),

		// Authentication
		JWTSecret: env.GetEnv("JWT_SECRET", ""),

		// Internal authentication
		InternalJWTPrivateKeyPath: env.GetEnv("INTERNAL_JWT_PRIVATE_KEY_PATH", "/secrets/internal_jwt_private.key"),

		// Rate limiting (per team)
		RateLimitRPS:   env.GetEnvInt("RATE_LIMIT_RPS", 100),
		RateLimitBurst: env.GetEnvInt("RATE_LIMIT_BURST", 200),

		// Timeouts
		ProxyTimeout:    env.GetEnvDuration("PROXY_TIMEOUT", 30*time.Second),
		ShutdownTimeout: env.GetEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}
