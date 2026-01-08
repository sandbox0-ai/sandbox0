// Package config provides configuration for netd.
package config

import (
	"time"

	"github.com/sandbox0-ai/infra/pkg/env"
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

	// UseEBPF enables eBPF-based bandwidth control (more efficient than tc htb)
	UseEBPF bool

	// BPFFSPath is the path to bpf filesystem (usually /sys/fs/bpf)
	BPFFSPath string

	// UseEDT enables Earliest Departure Time pacing for eBPF
	UseEDT bool
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	cfg := &Config{
		LogLevel:              env.GetEnv("LOG_LEVEL", "info"),
		MetricsPort:           env.GetEnvInt("METRICS_PORT", 9090),
		HealthPort:            env.GetEnvInt("HEALTH_PORT", 8080),
		NodeName:              env.GetEnv("NODE_NAME", ""),
		Namespace:             env.GetEnv("NAMESPACE", ""),
		KubeConfig:            env.GetEnv("KUBECONFIG", ""),
		ResyncPeriod:          env.GetEnvDuration("RESYNC_PERIOD", 30*time.Second),
		ProxyListenAddr:       env.GetEnv("PROXY_LISTEN_ADDR", "0.0.0.0"),
		ProxyHTTPPort:         env.GetEnvInt("PROXY_HTTP_PORT", 18080),
		ProxyHTTPSPort:        env.GetEnvInt("PROXY_HTTPS_PORT", 18443),
		DNSResolvers:          []string{env.GetEnv("DNS_RESOLVER", "8.8.8.8:53")},
		MetricsReportInterval: env.GetEnvDuration("METRICS_REPORT_INTERVAL", 10*time.Second),
		FailClosed:            env.GetEnvBool("FAIL_CLOSED", true),
		StorageProxyCIDR:      env.GetEnv("STORAGE_PROXY_CIDR", ""),
		ClusterDNSCIDR:        env.GetEnv("CLUSTER_DNS_CIDR", ""),
		InternalGatewayCIDR:   env.GetEnv("INTERNAL_GATEWAY_CIDR", ""),
		ProcdPort:             env.GetEnvInt("PROCD_PORT", 49983),
		UseEBPF:               env.GetEnvBool("USE_EBPF", true), // Enabled by default
		BPFFSPath:             env.GetEnv("BPF_FS_PATH", "/sys/fs/bpf"),
		UseEDT:                env.GetEnvBool("USE_EDT", true), // EDT pacing enabled by default
	}

	return cfg
}
