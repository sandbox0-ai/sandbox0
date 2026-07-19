// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultNetdProxyMaxActiveTCPConnections = 4096
	DefaultNetdProxyUDPWorkers              = 32
	DefaultNetdProxyUDPQueueSize            = 1024
	MaxNetdProxyMaxActiveTCPConnections     = 65536
	MaxNetdProxyUDPWorkers                  = 256
	MaxNetdProxyUDPQueueSize                = 8192
)

// NetdConfig holds configuration for the ctld network runtime.
// +kubebuilder:validation:XValidation:rule="!has(self.proxyUdpWorkers) || !has(self.proxyUdpQueueSize) || self.proxyUdpWorkers <= self.proxyUdpQueueSize",message="proxyUdpWorkers must not exceed proxyUdpQueueSize"
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
	// EgressAuthResolverURL is the base URL for the runtime egress auth resolver.
	EgressAuthResolverURL string `yaml:"egress_auth_resolver_url" json:"egressAuthResolverUrl"`

	// +optional
	// +kubebuilder:default=false
	EgressAuthEnabled bool `yaml:"egress_auth_enabled" json:"egressAuthEnabled"`

	// +optional
	// +kubebuilder:default="2s"
	// EgressAuthResolverTimeout is the timeout for runtime egress auth resolve calls.
	EgressAuthResolverTimeout metav1.Duration `yaml:"egress_auth_resolver_timeout" json:"egressAuthResolverTimeout"`

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
	// ProxyMaxActiveTCPConnections bounds accepted TCP connections executing in
	// one ctld network runtime before any tenant lookup or distributed quota
	// call.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65536
	// +kubebuilder:default=4096
	ProxyMaxActiveTCPConnections int `yaml:"proxy_max_active_tcp_connections" json:"proxyMaxActiveTcpConnections"`
	// ProxyUDPWorkers bounds concurrent datagram classification and forwarding.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=256
	// +kubebuilder:default=32
	ProxyUDPWorkers int `yaml:"proxy_udp_workers" json:"proxyUdpWorkers"`
	// ProxyUDPQueueSize bounds all queued and executing datagrams. A full queue
	// drops a datagram before allocating a per-datagram goroutine or audit.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=8192
	// +kubebuilder:default=1024
	ProxyUDPQueueSize int `yaml:"proxy_udp_queue_size" json:"proxyUdpQueueSize"`
	// +optional
	// +kubebuilder:default="30s"
	ProxyUpstreamTimeout metav1.Duration `yaml:"proxy_upstream_timeout" json:"proxyUpstreamTimeout"`
	// +optional
	// Per-sandbox egress bandwidth limit in bytes per second. Zero disables throttling.
	EgressBandwidthBytesPerSecond int64 `yaml:"egress_bandwidth_bytes_per_second" json:"egressBandwidthBytesPerSecond"`
	// +optional
	// Per-sandbox ingress bandwidth limit in bytes per second. Zero disables throttling.
	IngressBandwidthBytesPerSecond int64 `yaml:"ingress_bandwidth_bytes_per_second" json:"ingressBandwidthBytesPerSecond"`
	// +optional
	// Token bucket burst in bytes for bandwidth limiting. Zero uses one second of the configured rate.
	BandwidthBurstBytes int64 `yaml:"bandwidth_burst_bytes" json:"bandwidthBurstBytes"`
	// TeamQuotaDistributedEnforcement configures region-shared network
	// operation and byte rates plus active-connection concurrency admission.
	TeamQuotaDistributedEnforcement TeamQuotaDistributedEnforcementConfig `yaml:"team_quota_distributed_enforcement" json:"teamQuotaDistributedEnforcement"`

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
	// Metering configures the optional region usage ledger.
	// +optional
	Metering MeteringConfig `yaml:"metering" json:"metering"`
	// +optional
	AuditLogPath string `yaml:"audit_log_path" json:"auditLogPath"`
	// +optional
	// +kubebuilder:default=104857600
	AuditLogMaxBytes int64 `yaml:"audit_log_max_bytes" json:"auditLogMaxBytes"`
	// +optional
	// +kubebuilder:default=5
	AuditLogMaxBackups int `yaml:"audit_log_max_backups" json:"auditLogMaxBackups"`
	// +optional
	SandboxObservabilityIngestURL string `yaml:"sandbox_observability_ingest_url" json:"sandboxObservabilityIngestUrl"`
	// SandboxObservabilityAuditSpoolDir is the fsync-backed node-local delivery
	// spool. Records are removed only after cluster-gateway acknowledges them.
	SandboxObservabilityAuditSpoolDir string `yaml:"sandbox_observability_audit_spool_dir" json:"sandboxObservabilityAuditSpoolDir"`
	// SandboxObservabilityAuditSpoolLimits bounds the node-local audit delivery
	// spool globally and per team. These are platform disk-safety guards, not
	// Team Quota or billing policy.
	SandboxObservabilityAuditSpoolLimits AuditSpoolLimitsConfig `yaml:"sandbox_observability_audit_spool_limits" json:"sandboxObservabilityAuditSpoolLimits"`
	// SandboxObservabilityAuditDeliveryMode controls whether a durable local
	// enqueue or canonical ClickHouse acknowledgement admits a new flow.
	// +optional
	// +kubebuilder:validation:Enum=durable_async;canonical_sync
	// +kubebuilder:default="durable_async"
	SandboxObservabilityAuditDeliveryMode sandboxobservability.AuditDeliveryMode `yaml:"sandbox_observability_audit_delivery_mode" json:"sandboxObservabilityAuditDeliveryMode"`
	// +optional
	// +kubebuilder:default=1024
	SandboxObservabilityIngestQueueSize int `yaml:"sandbox_observability_ingest_queue_size" json:"sandboxObservabilityIngestQueueSize"`
	// +optional
	// +kubebuilder:default=100
	SandboxObservabilityIngestBatchSize int `yaml:"sandbox_observability_ingest_batch_size" json:"sandboxObservabilityIngestBatchSize"`
	// +optional
	// +kubebuilder:default="1s"
	SandboxObservabilityIngestFlushInterval metav1.Duration `yaml:"sandbox_observability_ingest_flush_interval" json:"sandboxObservabilityIngestFlushInterval"`
	// +optional
	// +kubebuilder:default="2s"
	SandboxObservabilityIngestRequestTimeout metav1.Duration `yaml:"sandbox_observability_ingest_request_timeout" json:"sandboxObservabilityIngestRequestTimeout"`
	// +optional
	// +kubebuilder:default=3
	SandboxObservabilityIngestMaxRetries int `yaml:"sandbox_observability_ingest_max_retries" json:"sandboxObservabilityIngestMaxRetries"`
	// +optional
	// +kubebuilder:default="100ms"
	SandboxObservabilityIngestRetryBackoff metav1.Duration `yaml:"sandbox_observability_ingest_retry_backoff" json:"sandboxObservabilityIngestRetryBackoff"`
	// +optional
	// +kubebuilder:default="2s"
	ShutdownDelay metav1.Duration `yaml:"shutdown_delay" json:"shutdownDelay"`
}

// LoadNetdConfigFromPath loads network runtime configuration from an explicit path.
// The embedding ctld process owns path selection and error handling.
func LoadNetdConfigFromPath(path string) (*NetdConfig, error) {
	cfg, err := loadNetdConfig(path)
	if err != nil {
		return nil, err
	}
	applyNetdDefaults(cfg)
	if _, _, _, err := cfg.ProxyAdmissionLimits(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ProxyAdmissionLimits returns normalized node-local work bounds and rejects
// configurations that could recreate an unbounded worker surface.
func (c *NetdConfig) ProxyAdmissionLimits() (
	maxTCPConnections int,
	udpWorkers int,
	udpQueueSize int,
	err error,
) {
	if c == nil {
		return 0, 0, 0, fmt.Errorf("network runtime config is required")
	}
	if c.ProxyMaxActiveTCPConnections < 0 ||
		c.ProxyUDPWorkers < 0 ||
		c.ProxyUDPQueueSize < 0 {
		return 0, 0, 0, fmt.Errorf("proxy admission limits must be non-negative")
	}
	maxTCPConnections = c.ProxyMaxActiveTCPConnections
	if maxTCPConnections <= 0 {
		maxTCPConnections = DefaultNetdProxyMaxActiveTCPConnections
	}
	udpWorkers = c.ProxyUDPWorkers
	if udpWorkers <= 0 {
		udpWorkers = DefaultNetdProxyUDPWorkers
	}
	udpQueueSize = c.ProxyUDPQueueSize
	if udpQueueSize <= 0 {
		udpQueueSize = DefaultNetdProxyUDPQueueSize
	}
	switch {
	case maxTCPConnections > MaxNetdProxyMaxActiveTCPConnections:
		err = fmt.Errorf(
			"proxy_max_active_tcp_connections must not exceed %d",
			MaxNetdProxyMaxActiveTCPConnections,
		)
	case udpWorkers > MaxNetdProxyUDPWorkers:
		err = fmt.Errorf(
			"proxy_udp_workers must not exceed %d",
			MaxNetdProxyUDPWorkers,
		)
	case udpQueueSize > MaxNetdProxyUDPQueueSize:
		err = fmt.Errorf(
			"proxy_udp_queue_size must not exceed %d",
			MaxNetdProxyUDPQueueSize,
		)
	case udpWorkers > udpQueueSize:
		err = fmt.Errorf("proxy_udp_workers must not exceed proxy_udp_queue_size")
	}
	return maxTCPConnections, udpWorkers, udpQueueSize, err
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

// ValidateListenerPorts rejects collisions between network runtime listeners
// and ports reserved by other ctld subsystems.
func (c *NetdConfig) ValidateListenerPorts(reserved map[int]string) error {
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

func applyNetdDefaults(cfg *NetdConfig) {
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
		cfg.ProxyUpstreamTimeout = metav1.Duration{Duration: 30 * time.Second}
	}
	if cfg.ProxyHeaderLimit == 0 {
		cfg.ProxyHeaderLimit = 64 * 1024
	}
	if cfg.ProxyMaxActiveTCPConnections == 0 {
		cfg.ProxyMaxActiveTCPConnections = DefaultNetdProxyMaxActiveTCPConnections
	}
	if cfg.ProxyUDPWorkers == 0 {
		cfg.ProxyUDPWorkers = DefaultNetdProxyUDPWorkers
	}
	if cfg.ProxyUDPQueueSize == 0 {
		cfg.ProxyUDPQueueSize = DefaultNetdProxyUDPQueueSize
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
	if cfg.EgressAuthResolverTimeout.Duration == 0 {
		cfg.EgressAuthResolverTimeout = metav1.Duration{Duration: 2 * time.Second}
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
	if cfg.SandboxObservabilityIngestQueueSize == 0 {
		cfg.SandboxObservabilityIngestQueueSize = 1024
	}
	applyAuditSpoolLimitsDefaults(&cfg.SandboxObservabilityAuditSpoolLimits)
	if cfg.SandboxObservabilityIngestBatchSize == 0 {
		cfg.SandboxObservabilityIngestBatchSize = 100
	}
	if cfg.SandboxObservabilityIngestFlushInterval.Duration == 0 {
		cfg.SandboxObservabilityIngestFlushInterval = metav1.Duration{Duration: time.Second}
	}
	if cfg.SandboxObservabilityIngestRequestTimeout.Duration == 0 {
		cfg.SandboxObservabilityIngestRequestTimeout = metav1.Duration{Duration: 2 * time.Second}
	}
	if cfg.SandboxObservabilityIngestMaxRetries == 0 {
		cfg.SandboxObservabilityIngestMaxRetries = 3
	}
	if cfg.SandboxObservabilityIngestRetryBackoff.Duration == 0 {
		cfg.SandboxObservabilityIngestRetryBackoff = metav1.Duration{Duration: 100 * time.Millisecond}
	}
	cfg.SandboxObservabilityAuditDeliveryMode = sandboxobservability.NormalizeAuditDeliveryMode(cfg.SandboxObservabilityAuditDeliveryMode)
	if cfg.BurstRatio == "" {
		cfg.BurstRatio = "0.125"
	}
}
