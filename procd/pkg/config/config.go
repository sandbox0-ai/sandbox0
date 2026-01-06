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

	// Network configuration
	Network *NetworkConfig

	// File manager configuration
	RootPath string

	// Cache configuration
	CacheMaxBytes int64
	CacheTTL      time.Duration
}

// NetworkConfig holds network isolation configuration.
type NetworkConfig struct {
	// TCP Proxy settings
	TCPProxyPort   int32
	EnableTCPProxy bool

	// DNS servers for independent resolution
	DNSServers []string

	// Default deny CIDRs (private networks)
	DefaultDenyCIDRs []string
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		SandboxID:  env.GetEnv("SANDBOX_ID", ""),
		TemplateID: env.GetEnv("TEMPLATE_ID", ""),
		NodeName:   env.GetEnv("NODE_NAME", ""),

		HTTPPort: env.GetEnvInt("PROCD_HTTP_PORT", 8080),
		LogLevel: env.GetEnv("PROCD_LOG_LEVEL", "info"),

		MaxContexts: env.GetEnvInt("PROCD_MAX_CONTEXTS", 100),

		StorageProxyBaseURL:  env.GetEnv("STORAGE_PROXY_BASE_URL", "storage-proxy.sandbox0-system.svc.cluster.local"),
		StorageProxyReplicas: env.GetEnvInt("STORAGE_PROXY_REPLICAS", 3),

		Network: &NetworkConfig{
			TCPProxyPort:   int32(env.GetEnvInt("NETWORK_TCP_PROXY_PORT", 1080)),
			EnableTCPProxy: env.GetEnvBool("NETWORK_ENABLE_TCP_PROXY", false),
			DNSServers:     []string{"8.8.8.8", "8.8.4.4"},
			DefaultDenyCIDRs: []string{
				"10.0.0.0/8",
				"127.0.0.0/8",
				"169.254.0.0/16",
				"172.16.0.0/12",
				"192.168.0.0/16",
			},
		},

		RootPath: env.GetEnv("PROCD_ROOT_PATH", "/workspace"),

		CacheMaxBytes: int64(env.GetEnvInt("CACHE_MAX_BYTES", 100*1024*1024)),
		CacheTTL:      time.Duration(env.GetEnvInt("CACHE_TTL_SECONDS", 30)) * time.Second,
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	// SandboxID and TemplateID can be empty during development
	return nil
}
