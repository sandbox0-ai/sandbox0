package config

import (
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
)

// Config holds the configuration for the manager
type Config struct {
	// HTTP Server
	HTTPPort int

	// Kubernetes
	KubeConfig     string
	Namespace      string
	LeaderElection bool
	ResyncPeriod   time.Duration

	// Database
	DatabaseURL string

	// Cleanup Controller
	CleanupInterval time.Duration

	// Logging
	LogLevel string

	// Metrics
	MetricsPort int

	// Internal Auth
	InternalAuthPublicKeyPath  string
	InternalAuthPrivateKeyPath string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		HTTPPort:                   env.GetEnvInt("HTTP_PORT", 8080),
		KubeConfig:                 env.GetEnv("KUBECONFIG", ""),
		Namespace:                  env.GetEnv("NAMESPACE", "default"),
		LeaderElection:             env.GetEnvBool("LEADER_ELECTION", true),
		ResyncPeriod:               env.GetEnvDuration("RESYNC_PERIOD", 30*time.Second),
		DatabaseURL:                env.GetEnv("DATABASE_URL", ""),
		CleanupInterval:            env.GetEnvDuration("CLEANUP_INTERVAL", 60*time.Second),
		LogLevel:                   env.GetEnv("LOG_LEVEL", "info"),
		MetricsPort:                env.GetEnvInt("METRICS_PORT", 9090),
		InternalAuthPublicKeyPath:  env.GetEnv("INTERNAL_AUTH_PUBLIC_KEY_PATH", "/config/internal_jwt_public.key"),
		InternalAuthPrivateKeyPath: env.GetEnv("INTERNAL_AUTH_PRIVATE_KEY_PATH", "/secrets/internal_jwt_private.key"),
	}
}
