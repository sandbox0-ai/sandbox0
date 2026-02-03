// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NetdConfig holds configuration for netd.
type NetdConfig struct {
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// +optional
	NodeName string `yaml:"node_name" json:"nodeName"`

	// +optional
	KubeConfig string `yaml:"kube_config" json:"kubeConfig"`

	// +optional
	Namespace string `yaml:"namespace" json:"namespace"`

	// +optional
	// +kubebuilder:default="30s"
	ResyncPeriod metav1.Duration `yaml:"resync_period" json:"resyncPeriod"`

	// +optional
	// +kubebuilder:default=9091
	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	// +optional
	// +kubebuilder:default=8081
	HealthPort int `yaml:"health_port" json:"healthPort"`

	// +optional
	// +kubebuilder:default=true
	FailClosed bool `yaml:"fail_closed" json:"failClosed"`

	// +optional
	// +kubebuilder:default=true
	PreferNFT *bool `yaml:"prefer_nft" json:"preferNft"`

	// +optional
	// +kubebuilder:default="0.125"
	BurstRatio string `yaml:"burst_ratio" json:"burstRatio"`

	// Proxy settings
	// +optional
	// +kubebuilder:default="0.0.0.0"
	ProxyListenAddr string `yaml:"proxy_listen_addr" json:"proxyListenAddr"`
	// +optional
	// +kubebuilder:default=18080
	ProxyHTTPPort int `yaml:"proxy_http_port" json:"proxyHttpPort"`
	// +optional
	// +kubebuilder:default=18443
	ProxyHTTPSPort int `yaml:"proxy_https_port" json:"proxyHttpsPort"`
	// +optional
	ProxyHeaderLimit int64 `yaml:"proxy_header_limit" json:"proxyHeaderLimit"`
	// +optional
	// +kubebuilder:default=1000
	ProxyMaxIdleConns int `yaml:"proxy_max_idle_conns" json:"proxyMaxIdleConns"`
	// +optional
	// +kubebuilder:default=100
	ProxyMaxIdleConnsPerHost int `yaml:"proxy_max_idle_conns_per_host" json:"proxyMaxIdleConnsPerHost"`
	// +optional
	// +kubebuilder:default="30s"
	ProxyUpstreamTimeout metav1.Duration `yaml:"proxy_upstream_timeout" json:"proxyUpstreamTimeout"`
	// +optional
	// +kubebuilder:default="5s"
	ProxyDNSTimeout metav1.Duration `yaml:"proxy_dns_timeout" json:"proxyDnsTimeout"`
	// +optional
	// +kubebuilder:default="10s"
	ProxyResponseHeaderTimeout metav1.Duration `yaml:"proxy_response_header_timeout" json:"proxyResponseHeaderTimeout"`
	// +optional
	// +kubebuilder:default="90s"
	ProxyIdleConnTimeout metav1.Duration `yaml:"proxy_idle_conn_timeout" json:"proxyIdleConnTimeout"`
	// +optional
	DNSResolvers []string `yaml:"dns_resolvers" json:"dnsResolvers"`

	// Ports and CIDRs
	// +optional
	// +kubebuilder:default=49983
	ProcdPort int `yaml:"procd_port" json:"procdPort"`
	// +optional
	// +kubebuilder:default=53
	DNSPort int `yaml:"dns_port" json:"dnsPort"`
	// +optional
	StorageProxyCIDR string `yaml:"storage_proxy_cidr" json:"storageProxyCidr"`
	// +optional
	ClusterDNSCIDR string `yaml:"cluster_dns_cidr" json:"clusterDnsCidr"`
	// +optional
	InternalGatewayCIDR string `yaml:"internal_gateway_cidr" json:"internalGatewayCidr"`

	// eBPF and tc
	// +optional
	UseEBPF bool `yaml:"use_ebpf" json:"useEbpf"`
	// +optional
	BPFFSPath string `yaml:"bpf_fs_path" json:"bpfFsPath"`
	// +optional
	BPFPinPath string `yaml:"bpf_pin_path" json:"bpfPinPath"`
	// +optional
	UseEDT bool `yaml:"use_edt" json:"useEdt"`
	// +optional
	// +kubebuilder:default="200ms"
	EDTHorizon metav1.Duration `yaml:"edt_horizon" json:"edtHorizon"`
	// +optional
	VethPrefix string `yaml:"veth_prefix" json:"vethPrefix"`

	// +optional
	// +kubebuilder:default="10s"
	MetricsReportInterval metav1.Duration `yaml:"metrics_report_interval" json:"metricsReportInterval"`
	// +optional
	AuditLogPath string `yaml:"audit_log_path" json:"auditLogPath"`
	// +optional
	// +kubebuilder:default=104857600
	AuditLogMaxBytes int64 `yaml:"audit_log_max_bytes" json:"auditLogMaxBytes"`
	// +optional
	// +kubebuilder:default=5
	AuditLogMaxBackups int `yaml:"audit_log_max_backups" json:"auditLogMaxBackups"`
	// +optional
	// +kubebuilder:default="2s"
	ShutdownDelay metav1.Duration `yaml:"shutdown_delay" json:"shutdownDelay"`
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
		cfg = &NetdConfig{}
	}

	applyNetdDefaults(cfg)
	return cfg
}

func loadNetdConfig(path string) (*NetdConfig, error) {
	cfg := &NetdConfig{}
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

func applyNetdDefaults(cfg *NetdConfig) {
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = 9091
	}
	if cfg.HealthPort == 0 {
		cfg.HealthPort = 8081
	}
	if cfg.ProxyListenAddr == "" {
		cfg.ProxyListenAddr = "0.0.0.0"
	}
	if cfg.PreferNFT == nil {
		value := true
		cfg.PreferNFT = &value
	}
	if cfg.ProxyHTTPPort == 0 {
		cfg.ProxyHTTPPort = 18080
	}
	if cfg.ProxyHTTPSPort == 0 {
		cfg.ProxyHTTPSPort = 18443
	}
	if cfg.ProxyUpstreamTimeout.Duration == 0 {
		cfg.ProxyUpstreamTimeout = metav1.Duration{Duration: 30 * time.Second}
	}
	if cfg.ProxyDNSTimeout.Duration == 0 {
		cfg.ProxyDNSTimeout = metav1.Duration{Duration: 5 * time.Second}
	}
	if cfg.ProxyResponseHeaderTimeout.Duration == 0 {
		cfg.ProxyResponseHeaderTimeout = metav1.Duration{Duration: 10 * time.Second}
	}
	if cfg.ProxyIdleConnTimeout.Duration == 0 {
		cfg.ProxyIdleConnTimeout = metav1.Duration{Duration: 90 * time.Second}
	}
	if cfg.ProxyHeaderLimit == 0 {
		cfg.ProxyHeaderLimit = 64 * 1024
	}
	if cfg.ProxyMaxIdleConns == 0 {
		cfg.ProxyMaxIdleConns = 1000
	}
	if cfg.ProxyMaxIdleConnsPerHost == 0 {
		cfg.ProxyMaxIdleConnsPerHost = 100
	}
	if cfg.ProcdPort == 0 {
		cfg.ProcdPort = 49983
	}
	if cfg.DNSPort == 0 {
		cfg.DNSPort = 53
	}
	if cfg.ResyncPeriod.Duration == 0 {
		cfg.ResyncPeriod = metav1.Duration{Duration: 30 * time.Second}
	}
	if cfg.EDTHorizon.Duration == 0 {
		cfg.EDTHorizon = metav1.Duration{Duration: 200 * time.Millisecond}
	}
	if cfg.MetricsReportInterval.Duration == 0 {
		cfg.MetricsReportInterval = metav1.Duration{Duration: 10 * time.Second}
	}
	if cfg.ShutdownDelay.Duration == 0 {
		cfg.ShutdownDelay = metav1.Duration{Duration: 2 * time.Second}
	}
	if cfg.AuditLogMaxBytes == 0 {
		cfg.AuditLogMaxBytes = 100 * 1024 * 1024
	}
	if cfg.AuditLogMaxBackups == 0 {
		cfg.AuditLogMaxBackups = 5
	}
	if cfg.BurstRatio == "" {
		cfg.BurstRatio = "0.125"
	}
}
