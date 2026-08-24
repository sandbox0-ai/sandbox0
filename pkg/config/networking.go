package config

import (
	"fmt"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"gopkg.in/yaml.v3"
)

// NetworkRuntimeConfig holds configuration for the ctld network runtime.
type NetworkRuntimeConfig struct {
	LogLevel string `yaml:"log_level" json:"logLevel"`

	NodeName string `yaml:"node_name" json:"nodeName"`

	RegionID string `yaml:"region_id" json:"-"`

	ClusterID string `yaml:"cluster_id" json:"-"`

	// EgressAuthResolverURL is the base URL for the runtime egress auth resolver.
	EgressAuthResolverURL string `yaml:"egress_auth_resolver_url" json:"egressAuthResolverUrl"`

	EgressAuthEnabled bool `yaml:"egress_auth_enabled" json:"egressAuthEnabled"`

	// EgressAuthResolverTimeout is the timeout for runtime egress auth resolve calls.
	EgressAuthResolverTimeout Duration `yaml:"egress_auth_resolver_timeout" json:"egressAuthResolverTimeout"`

	EgressAuthFailurePolicy string `yaml:"egress_auth_failure_policy" json:"egressAuthFailurePolicy"`

	MITMCACertPath string `yaml:"mitm_ca_cert_path" json:"mitmCaCertPath"`

	MITMCAKeyPath string `yaml:"mitm_ca_key_path" json:"mitmCaKeyPath"`

	MITMLeafTTL Duration `yaml:"mitm_leaf_ttl" json:"mitmLeafTtl"`

	DatabaseURL string `yaml:"database_url" json:"-"`

	ResyncPeriod Duration `yaml:"resync_period" json:"resyncPeriod"`

	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	HealthPort int `yaml:"health_port" json:"healthPort"`

	FailClosed bool `yaml:"fail_closed" json:"failClosed"`

	PreferNFT *bool `yaml:"prefer_nft" json:"preferNft"`

	BurstRatio string `yaml:"burst_ratio" json:"burstRatio"`

	// Proxy settings
	ProxyListenAddr      string   `yaml:"proxy_listen_addr" json:"proxyListenAddr"`
	ProxyHTTPPort        int      `yaml:"proxy_http_port" json:"proxyHttpPort"`
	ProxyHTTPSPort       int      `yaml:"proxy_https_port" json:"proxyHttpsPort"`
	ProxyHeaderLimit     int64    `yaml:"proxy_header_limit" json:"proxyHeaderLimit"`
	ProxyUpstreamTimeout Duration `yaml:"proxy_upstream_timeout" json:"proxyUpstreamTimeout"`
	// Per-sandbox egress bandwidth limit in bytes per second. Zero disables throttling.
	EgressBandwidthBytesPerSecond int64 `yaml:"egress_bandwidth_bytes_per_second" json:"egressBandwidthBytesPerSecond"`
	// Per-sandbox ingress bandwidth limit in bytes per second. Zero disables throttling.
	IngressBandwidthBytesPerSecond int64 `yaml:"ingress_bandwidth_bytes_per_second" json:"ingressBandwidthBytesPerSecond"`
	// Token bucket burst in bytes for bandwidth limiting. Zero uses one second of the configured rate.
	BandwidthBurstBytes int64 `yaml:"bandwidth_burst_bytes" json:"bandwidthBurstBytes"`
	// Deprecated: bootstrap network_egress_bytes through manager default_team_quotas instead.
	TeamEgressBandwidthBytesPerSecond int64 `yaml:"team_egress_bandwidth_bytes_per_second" json:"teamEgressBandwidthBytesPerSecond"`
	// Deprecated: bootstrap network_ingress_bytes through manager default_team_quotas instead.
	TeamIngressBandwidthBytesPerSecond int64 `yaml:"team_ingress_bandwidth_bytes_per_second" json:"teamIngressBandwidthBytesPerSecond"`
	// Deprecated: use each network quota policy's burst_value.
	TeamBandwidthBurstBytes int64 `yaml:"team_bandwidth_burst_bytes" json:"teamBandwidthBurstBytes"`
	// RedisURL configures the Redis backend used by region-scoped Team Quota bandwidth limiting.
	RedisURL string `yaml:"redis_url" json:"-"`
	// RedisKeyPrefix prefixes Redis keys used by the network runtime.
	RedisKeyPrefix string `yaml:"redis_key_prefix" json:"-"`
	// RedisTimeout bounds each Redis operation.
	RedisTimeout Duration `yaml:"redis_timeout" json:"-"`
	// RedisFailOpen allows traffic when Redis is temporarily unavailable.
	RedisFailOpen bool `yaml:"redis_fail_open" json:"-"`

	// Ports and CIDRs
	ProcdPort        int      `yaml:"procd_port" json:"-"`
	DNSPort          int      `yaml:"dns_port" json:"dnsPort"`
	DNSResolverCIDRs []string `yaml:"dns_resolver_cidrs" json:"-"`

	// Platform allow/deny lists (override user policy)
	PlatformAllowedCIDRs   []string `yaml:"platform_allowed_cidrs" json:"platformAllowedCidrs"`
	PlatformDeniedCIDRs    []string `yaml:"platform_denied_cidrs" json:"platformDeniedCidrs"`
	PlatformAllowedDomains []string `yaml:"platform_allowed_domains" json:"platformAllowedDomains"`
	PlatformDeniedDomains  []string `yaml:"platform_denied_domains" json:"platformDeniedDomains"`

	// eBPF and tc
	UseEBPF    bool     `yaml:"use_ebpf" json:"useEbpf"`
	BPFFSPath  string   `yaml:"bpf_fs_path" json:"bpfFsPath"`
	BPFPinPath string   `yaml:"bpf_pin_path" json:"bpfPinPath"`
	UseEDT     bool     `yaml:"use_edt" json:"useEdt"`
	EDTHorizon Duration `yaml:"edt_horizon" json:"edtHorizon"`
	VethPrefix string   `yaml:"veth_prefix" json:"vethPrefix"`

	MetricsReportInterval  Duration `yaml:"metrics_report_interval" json:"metricsReportInterval"`
	MeteringReportInterval Duration `yaml:"metering_report_interval" json:"meteringReportInterval"`
	// Metering configures the optional region usage ledger.
	Metering                      MeteringConfig `yaml:"metering" json:"metering"`
	AuditLogPath                  string         `yaml:"audit_log_path" json:"auditLogPath"`
	AuditLogMaxBytes              int64          `yaml:"audit_log_max_bytes" json:"auditLogMaxBytes"`
	AuditLogMaxBackups            int            `yaml:"audit_log_max_backups" json:"auditLogMaxBackups"`
	SandboxObservabilityIngestURL string         `yaml:"sandbox_observability_ingest_url" json:"sandboxObservabilityIngestUrl"`
	// SandboxObservabilityAuditSpoolDir is the fsync-backed node-local delivery
	// spool. Records are removed only after cluster-gateway acknowledges them.
	SandboxObservabilityAuditSpoolDir string `yaml:"sandbox_observability_audit_spool_dir" json:"sandboxObservabilityAuditSpoolDir"`
	// SandboxObservabilityAuditDeliveryMode controls whether a durable local
	// enqueue or canonical ClickHouse acknowledgement admits a new flow.
	SandboxObservabilityAuditDeliveryMode    sandboxobservability.AuditDeliveryMode `yaml:"sandbox_observability_audit_delivery_mode" json:"sandboxObservabilityAuditDeliveryMode"`
	SandboxObservabilityIngestQueueSize      int                                    `yaml:"sandbox_observability_ingest_queue_size" json:"sandboxObservabilityIngestQueueSize"`
	SandboxObservabilityIngestBatchSize      int                                    `yaml:"sandbox_observability_ingest_batch_size" json:"sandboxObservabilityIngestBatchSize"`
	SandboxObservabilityIngestFlushInterval  Duration                               `yaml:"sandbox_observability_ingest_flush_interval" json:"sandboxObservabilityIngestFlushInterval"`
	SandboxObservabilityIngestRequestTimeout Duration                               `yaml:"sandbox_observability_ingest_request_timeout" json:"sandboxObservabilityIngestRequestTimeout"`
	SandboxObservabilityIngestMaxRetries     int                                    `yaml:"sandbox_observability_ingest_max_retries" json:"sandboxObservabilityIngestMaxRetries"`
	SandboxObservabilityIngestRetryBackoff   Duration                               `yaml:"sandbox_observability_ingest_retry_backoff" json:"sandboxObservabilityIngestRetryBackoff"`
	ShutdownDelay                            Duration                               `yaml:"shutdown_delay" json:"shutdownDelay"`
}

// DeepCopy returns an independent copy suitable for per-claim policy mutation.
func (c *NetworkRuntimeConfig) DeepCopy() *NetworkRuntimeConfig {
	if c == nil {
		return nil
	}
	out := *c
	if c.PreferNFT != nil {
		value := *c.PreferNFT
		out.PreferNFT = &value
	}
	out.PlatformAllowedCIDRs = append([]string(nil), c.PlatformAllowedCIDRs...)
	out.PlatformDeniedCIDRs = append([]string(nil), c.PlatformDeniedCIDRs...)
	out.PlatformAllowedDomains = append([]string(nil), c.PlatformAllowedDomains...)
	out.PlatformDeniedDomains = append([]string(nil), c.PlatformDeniedDomains...)
	out.DNSResolverCIDRs = append([]string(nil), c.DNSResolverCIDRs...)
	return &out
}

// LoadNetworkRuntimeConfigFromPath loads network runtime configuration from an explicit path.
// The embedding ctld process owns path selection and error handling.
func LoadNetworkRuntimeConfigFromPath(path string) (*NetworkRuntimeConfig, error) {
	cfg, err := loadNetworkRuntimeConfig(path)
	if err != nil {
		return nil, err
	}
	applyNetworkRuntimeDefaults(cfg)
	return cfg, nil
}

func loadNetworkRuntimeConfig(path string) (*NetworkRuntimeConfig, error) {
	cfg := &NetworkRuntimeConfig{}
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

// ValidateListenerPorts rejects collisions between network runtime listeners
// and ports reserved by other ctld subsystems.
func (c *NetworkRuntimeConfig) ValidateListenerPorts(reserved map[int]string) error {
	if c == nil {
		return fmt.Errorf("network runtime config is required")
	}
	listeners := []struct {
		name string
		port int
	}{
		{name: "health", port: c.HealthPort},
		{name: "metrics", port: c.MetricsPort},
		{name: "HTTP proxy", port: c.ProxyHTTPPort},
		{name: "HTTPS proxy", port: c.ProxyHTTPSPort},
	}
	seen := make(map[int]string, len(listeners))
	for _, listener := range listeners {
		if listener.port <= 0 || listener.port > 65535 {
			return fmt.Errorf("network runtime %s port %d is outside 1-65535", listener.name, listener.port)
		}
		if previous := seen[listener.port]; previous != "" {
			return fmt.Errorf("network runtime %s port %d conflicts with %s port", listener.name, listener.port, previous)
		}
		if owner := reserved[listener.port]; owner != "" {
			return fmt.Errorf("network runtime %s port %d conflicts with %s", listener.name, listener.port, owner)
		}
		seen[listener.port] = listener.name
	}
	return nil
}

func applyNetworkRuntimeDefaults(cfg *NetworkRuntimeConfig) {
	if cfg == nil {
		return
	}
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
		cfg.ProxyUpstreamTimeout = Duration{Duration: 30 * time.Second}
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
		cfg.ResyncPeriod = Duration{Duration: 30 * time.Second}
	}
	if cfg.EgressAuthResolverTimeout.Duration == 0 {
		cfg.EgressAuthResolverTimeout = Duration{Duration: 2 * time.Second}
	}
	if cfg.EgressAuthFailurePolicy == "" {
		cfg.EgressAuthFailurePolicy = "fail-closed"
	}
	if cfg.MITMLeafTTL.Duration == 0 {
		cfg.MITMLeafTTL = Duration{Duration: time.Hour}
	}
	if cfg.EDTHorizon.Duration == 0 {
		cfg.EDTHorizon = Duration{Duration: 200 * time.Millisecond}
	}
	if cfg.MetricsReportInterval.Duration == 0 {
		cfg.MetricsReportInterval = Duration{Duration: 10 * time.Second}
	}
	if cfg.MeteringReportInterval.Duration == 0 {
		cfg.MeteringReportInterval = Duration{Duration: 10 * time.Second}
	}
	if cfg.ShutdownDelay.Duration == 0 {
		cfg.ShutdownDelay = Duration{Duration: 2 * time.Second}
	}
	if cfg.AuditLogMaxBytes == 0 {
		cfg.AuditLogMaxBytes = 100 * 1024 * 1024
	}
	if cfg.AuditLogMaxBackups == 0 {
		cfg.AuditLogMaxBackups = 5
	}
	if cfg.SandboxObservabilityIngestQueueSize == 0 {
		cfg.SandboxObservabilityIngestQueueSize = 1024
	}
	if cfg.SandboxObservabilityIngestBatchSize == 0 {
		cfg.SandboxObservabilityIngestBatchSize = 100
	}
	if cfg.SandboxObservabilityIngestFlushInterval.Duration == 0 {
		cfg.SandboxObservabilityIngestFlushInterval = Duration{Duration: time.Second}
	}
	if cfg.SandboxObservabilityIngestRequestTimeout.Duration == 0 {
		cfg.SandboxObservabilityIngestRequestTimeout = Duration{Duration: 2 * time.Second}
	}
	if cfg.SandboxObservabilityIngestMaxRetries == 0 {
		cfg.SandboxObservabilityIngestMaxRetries = 3
	}
	if cfg.SandboxObservabilityIngestRetryBackoff.Duration == 0 {
		cfg.SandboxObservabilityIngestRetryBackoff = Duration{Duration: 100 * time.Millisecond}
	}
	cfg.SandboxObservabilityAuditDeliveryMode = sandboxobservability.NormalizeAuditDeliveryMode(cfg.SandboxObservabilityAuditDeliveryMode)
	if cfg.BurstRatio == "" {
		cfg.BurstRatio = "0.125"
	}
}
