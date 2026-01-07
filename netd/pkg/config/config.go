// Package config provides configuration for netd.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds netd configuration
type Config struct {
	// LogLevel is the logging level (debug, info, warn, error)
	LogLevel string

	// MetricsPort is the port for Prometheus metrics
	MetricsPort int

	// HealthPort is the port for health checks
	HealthPort int

	// NodeName is the name of the node this netd is running on
	NodeName string

	// Namespace is the namespace to watch for sandbox pods
	Namespace string

	// KubeConfig is the path to kubeconfig file (optional, uses in-cluster config if empty)
	KubeConfig string

	// ResyncPeriod is the period for informer resync
	ResyncPeriod time.Duration

	// ProxyListenAddr is the address for the L7 proxy to listen on
	ProxyListenAddr string

	// ProxyHTTPPort is the port for HTTP proxy (redirect from port 80)
	ProxyHTTPPort int

	// ProxyHTTPSPort is the port for HTTPS/TLS proxy (redirect from port 443)
	ProxyHTTPSPort int

	// DNSResolvers are the upstream DNS resolvers for the proxy
	DNSResolvers []string

	// MetricsReportInterval is the interval for reporting metrics
	MetricsReportInterval time.Duration

	// FailClosed if true, blocks all traffic when netd is not ready
	FailClosed bool

	// StorageProxyCIDR is the CIDR for storage-proxy (always allowed)
	StorageProxyCIDR string

	// ClusterDNSCIDR is the CIDR for cluster DNS (always allowed for DNS)
	ClusterDNSCIDR string

	// InternalGatewayCIDR is the CIDR for internal-gateway (allowed for ingress to procd)
	InternalGatewayCIDR string

	// ProcdPort is the port procd listens on
	ProcdPort int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	cfg := &Config{
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		MetricsPort:           getEnvInt("METRICS_PORT", 9090),
		HealthPort:            getEnvInt("HEALTH_PORT", 8080),
		NodeName:              getEnv("NODE_NAME", ""),
		Namespace:             getEnv("NAMESPACE", ""),
		KubeConfig:            getEnv("KUBECONFIG", ""),
		ResyncPeriod:          getEnvDuration("RESYNC_PERIOD", 30*time.Second),
		ProxyListenAddr:       getEnv("PROXY_LISTEN_ADDR", "0.0.0.0"),
		ProxyHTTPPort:         getEnvInt("PROXY_HTTP_PORT", 18080),
		ProxyHTTPSPort:        getEnvInt("PROXY_HTTPS_PORT", 18443),
		DNSResolvers:          []string{getEnv("DNS_RESOLVER", "8.8.8.8:53")},
		MetricsReportInterval: getEnvDuration("METRICS_REPORT_INTERVAL", 10*time.Second),
		FailClosed:            getEnvBool("FAIL_CLOSED", true),
		StorageProxyCIDR:      getEnv("STORAGE_PROXY_CIDR", ""),
		ClusterDNSCIDR:        getEnv("CLUSTER_DNS_CIDR", ""),
		InternalGatewayCIDR:   getEnv("INTERNAL_GATEWAY_CIDR", ""),
		ProcdPort:             getEnvInt("PROCD_PORT", 49983),
	}

	return cfg
}

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
