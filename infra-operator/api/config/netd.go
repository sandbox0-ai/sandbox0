// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"gopkg.in/yaml.v3"
)

// NetdConfig holds netd configuration.
type NetdConfig struct {
	// LogLevel is the logging level (debug, info, warn, error)
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// MetricsPort is the port for Prometheus metrics
	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	// HealthPort is the port for health checks
	HealthPort int `yaml:"health_port" json:"healthPort"`

	// NodeName is the name of the node this netd is running on
	NodeName string `yaml:"node_name" json:"nodeName"`

	// Namespace is the namespace to watch for sandbox pods
	Namespace string `yaml:"namespace" json:"namespace"`

	// KubeConfig is the path to kubeconfig file (optional, uses in-cluster config if empty)
	KubeConfig string `yaml:"kube_config" json:"kubeConfig"`

	// ResyncPeriod is the period for informer resync
	ResyncPeriod metav1.Duration `yaml:"resync_period" json:"resyncPeriod"`

	// ProxyListenAddr is the address for the L7 proxy to listen on
	ProxyListenAddr string `yaml:"proxy_listen_addr" json:"proxyListenAddr"`

	// ProxyHTTPPort is the port for HTTP proxy (redirect from port 80)
	ProxyHTTPPort int `yaml:"proxy_http_port" json:"proxyHTTPPort"`

	// ProxyHTTPSPort is the port for HTTPS/TLS proxy (redirect from port 443)
	ProxyHTTPSPort int `yaml:"proxy_https_port" json:"proxyHTTPSPort"`

	// DNSResolvers are the upstream DNS resolvers for the proxy
	DNSResolvers []string `yaml:"dns_resolvers" json:"dnsResolvers"`

	// MetricsReportInterval is the interval for reporting metrics
	MetricsReportInterval metav1.Duration `yaml:"metrics_report_interval" json:"metricsReportInterval"`

	// FailClosed if true, blocks all traffic when netd is not ready
	FailClosed bool `yaml:"fail_closed" json:"failClosed"`

	// StorageProxyCIDR is the CIDR for storage-proxy (always allowed)
	StorageProxyCIDR string `yaml:"storage_proxy_cidr" json:"storageProxyCIDR"`

	// ClusterDNSCIDR is the CIDR for cluster DNS (always allowed for DNS)
	ClusterDNSCIDR string `yaml:"cluster_dns_cidr" json:"clusterDNSCIDR"`

	// InternalGatewayCIDR is the CIDR for internal-gateway (allowed for ingress to procd)
	InternalGatewayCIDR string `yaml:"internal_gateway_cidr" json:"internalGatewayCIDR"`

	// ProcdPort is the port procd listens on
	ProcdPort int `yaml:"procd_port" json:"procdPort"`

	// UseEBPF enables eBPF-based bandwidth control (more efficient than tc htb)
	UseEBPF bool `yaml:"use_ebpf" json:"useEBPF"`

	// BPFFSPath is the path to bpf filesystem (usually /sys/fs/bpf)
	BPFFSPath string `yaml:"bpf_fs_path" json:"bpfFSPath"`

	// UseEDT enables Earliest Departure Time pacing for eBPF
	UseEDT bool `yaml:"use_edt" json:"useEDT"`
}

// DefaultNetdConfig returns the default configuration.
func DefaultNetdConfig() *NetdConfig {
	return &NetdConfig{
		LogLevel:              "info",
		MetricsPort:           9090,
		HealthPort:            8080,
		NodeName:              "",
		Namespace:             "",
		KubeConfig:            "",
		ResyncPeriod:          metav1.Duration{Duration: 30 * time.Second},
		ProxyListenAddr:       "0.0.0.0",
		ProxyHTTPPort:         18080,
		ProxyHTTPSPort:        18443,
		DNSResolvers:          []string{"8.8.8.8:53"},
		MetricsReportInterval: metav1.Duration{Duration: 10 * time.Second},
		FailClosed:            true,
		StorageProxyCIDR:      "",
		ClusterDNSCIDR:        "",
		InternalGatewayCIDR:   "",
		ProcdPort:             49983,
		UseEBPF:               true,
		BPFFSPath:             "/sys/fs/bpf",
		UseEDT:                true,
	}
}

// LoadNetdConfig returns the netd configuration.
func LoadNetdConfig() *NetdConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadNetdConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		cfg = DefaultNetdConfig()
	}
	return cfg
}

func loadNetdConfig(path string) (*NetdConfig, error) {
	cfg := DefaultNetdConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	data = []byte(os.ExpandEnv(string(data)))

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}
