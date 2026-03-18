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
	RegionID string `yaml:"region_id" json:"-"`

	// +optional
	ClusterID string `yaml:"cluster_id" json:"-"`

	// +optional
	// EgressBrokerURL is the base URL for the runtime egress auth resolver.
	// The legacy field name is kept for compatibility.
	EgressBrokerURL string `yaml:"egress_broker_url" json:"egressBrokerUrl"`

	// +optional
	// +kubebuilder:default=false
	EgressAuthEnabled bool `yaml:"egress_auth_enabled" json:"egressAuthEnabled"`

	// +optional
	// +kubebuilder:default="2s"
	// EgressBrokerTimeout is the timeout for runtime egress auth resolve calls.
	// The legacy field name is kept for compatibility.
	EgressBrokerTimeout metav1.Duration `yaml:"egress_broker_timeout" json:"egressBrokerTimeout"`

	// +optional
	// +kubebuilder:default="fail-closed"
	EgressAuthFailurePolicy string `yaml:"egress_auth_failure_policy" json:"egressAuthFailurePolicy"`

	// +optional
	MITMCACertPath string `yaml:"mitm_ca_cert_path" json:"mitmCaCertPath"`

	// +optional
	MITMCAKeyPath string `yaml:"mitm_ca_key_path" json:"mitmCaKeyPath"`

	// +optional
	// +kubebuilder:default="1h"
	MITMLeafTTL metav1.Duration `yaml:"mitm_leaf_ttl" json:"mitmLeafTtl"`

	// +optional
	DatabaseURL string `yaml:"database_url" json:"-"`

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
	// +kubebuilder:default="30s"
	ProxyUpstreamTimeout metav1.Duration `yaml:"proxy_upstream_timeout" json:"proxyUpstreamTimeout"`

	// Ports and CIDRs
	// +optional
	// +kubebuilder:default=49983
	ProcdPort int `yaml:"procd_port" json:"-"`
	// +optional
	// +kubebuilder:default=53
	DNSPort int `yaml:"dns_port" json:"dnsPort"`
	// +optional
	ClusterDNSCIDR string `yaml:"cluster_dns_cidr" json:"-"`

	// Platform allow/deny lists (override user policy)
	// +optional
	PlatformAllowedCIDRs []string `yaml:"platform_allowed_cidrs" json:"platformAllowedCidrs"`
	// +optional
	PlatformDeniedCIDRs []string `yaml:"platform_denied_cidrs" json:"platformDeniedCidrs"`
	// +optional
	PlatformAllowedDomains []string `yaml:"platform_allowed_domains" json:"platformAllowedDomains"`
	// +optional
	PlatformDeniedDomains []string `yaml:"platform_denied_domains" json:"platformDeniedDomains"`

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
	// +kubebuilder:default="10s"
	MeteringReportInterval metav1.Duration `yaml:"metering_report_interval" json:"meteringReportInterval"`
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
	if cfg.ProxyHeaderLimit == 0 {
		cfg.ProxyHeaderLimit = 64 * 1024
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
	if cfg.EgressBrokerTimeout.Duration == 0 {
		cfg.EgressBrokerTimeout = metav1.Duration{Duration: 2 * time.Second}
	}
	if cfg.EgressAuthFailurePolicy == "" {
		cfg.EgressAuthFailurePolicy = "fail-closed"
	}
	if cfg.MITMLeafTTL.Duration == 0 {
		cfg.MITMLeafTTL = metav1.Duration{Duration: time.Hour}
	}
	if cfg.EDTHorizon.Duration == 0 {
		cfg.EDTHorizon = metav1.Duration{Duration: 200 * time.Millisecond}
	}
	if cfg.MetricsReportInterval.Duration == 0 {
		cfg.MetricsReportInterval = metav1.Duration{Duration: 10 * time.Second}
	}
	if cfg.MeteringReportInterval.Duration == 0 {
		cfg.MeteringReportInterval = metav1.Duration{Duration: 10 * time.Second}
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
