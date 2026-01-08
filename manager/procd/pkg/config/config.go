// Package config provides configuration for the Procd service.
package config

import (
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
)

// Config holds all configuration for Procd.
type Config struct {
	// Sandbox identity
	SandboxID  string
	TemplateID string
	NodeName   string

	// Server configuration
	HTTPPort int
	LogLevel string

	// Context/Process limits
	MaxContexts int

	// Storage Proxy configuration
	StorageProxyBaseURL  string
	StorageProxyReplicas int

	// File manager configuration
	RootPath string

	// Cache configuration
	CacheMaxBytes int64
	CacheTTL      time.Duration

	// Internal auth configuration
	InternalAuthPublicKeyPath string
}

// Note: Network isolation is now handled by the netd service (DaemonSet).
// NetworkConfig has been removed from procd.

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		SandboxID:  env.GetEnv("SANDBOX_ID", ""),
		TemplateID: env.GetEnv("TEMPLATE_ID", ""),
		NodeName:   env.GetEnv("NODE_NAME", ""),

		HTTPPort: env.GetEnvInt("PROCD_HTTP_PORT", 49983),
		LogLevel: env.GetEnv("PROCD_LOG_LEVEL", "info"),

		MaxContexts: env.GetEnvInt("PROCD_MAX_CONTEXTS", 100),

		StorageProxyBaseURL:  env.GetEnv("STORAGE_PROXY_BASE_URL", "storage-proxy.sandbox0-system.svc.cluster.local"),
		StorageProxyReplicas: env.GetEnvInt("STORAGE_PROXY_REPLICAS", 3),

		RootPath: env.GetEnv("PROCD_ROOT_PATH", "/workspace"),

		CacheMaxBytes: int64(env.GetEnvInt("CACHE_MAX_BYTES", 100*1024*1024)),
		CacheTTL:      time.Duration(env.GetEnvInt("CACHE_TTL_SECONDS", 30)) * time.Second,

		InternalAuthPublicKeyPath: env.GetEnv("INTERNAL_AUTH_PUBLIC_KEY_PATH", "/config/internal_jwt_public.key"),
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	// SandboxID and TemplateID can be empty during development
	return nil
}
