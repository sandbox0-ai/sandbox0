// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ManagerConfig holds the configuration for the manager.
type ManagerConfig struct {
	// HTTP Server
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `yaml:"http_port" json:"httpPort"`

	// manager docker image, used to copy the procd binary to sandbox pod
	ManagerImage string `yaml:"manager_image" json:"-"`

	// template
	// +optional
	// +kubebuilder:default={}
	DefaultTemplate *DefaultTemplateConfig `yaml:"default_template" json:"defaultTemplate"`
	// +optional
	// +kubebuilder:default="sandbox0"
	TemplateNamespace string `yaml:"template_namespace" json:"templateNamespace"`
	DefaultClusterId  string `yaml:"default_cluster_id" json:"-"`

	// Kubernetes
	// +optional
	KubeConfig string `yaml:"kube_config" json:"kubeConfig"`
	// +optional
	// +kubebuilder:default=true
	LeaderElection bool `yaml:"leader_election" json:"leaderElection"`
	// +optional
	// +kubebuilder:default="30s"
	ResyncPeriod metav1.Duration `yaml:"resync_period" json:"resyncPeriod"`

	// Database
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=10
	DatabaseMaxConns int32 `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=2
	DatabaseMinConns int32 `yaml:"database_min_conns" json:"databaseMinConns"`

	// Cleanup Controller
	// +optional
	// +kubebuilder:default="60s"
	CleanupInterval metav1.Duration `yaml:"cleanup_interval" json:"cleanupInterval"`

	// Logging
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Metrics
	// +optional
	// +kubebuilder:default=9090
	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	// Webhook
	// +optional
	// +kubebuilder:default=9443
	WebhookPort int `yaml:"webhook_port" json:"webhookPort"`
	// +optional
	// +kubebuilder:default="/tmp/k8s-webhook-server/serving-certs/tls.crt"
	WebhookCertPath string `yaml:"webhook_cert_path" json:"webhookCertPath"`
	// +optional
	// +kubebuilder:default="/tmp/k8s-webhook-server/serving-certs/tls.key"
	WebhookKeyPath string `yaml:"webhook_key_path" json:"webhookKeyPath"`

	// Sandbox
	// +optional
	// +kubebuilder:default="5m"
	DefaultSandboxTTL metav1.Duration `yaml:"default_sandbox_ttl" json:"defaultSandboxTTL"`

	// Network
	// +optional
	// +kubebuilder:default=100000000
	DefaultBandwidthRateBps int64 `yaml:"default_bandwidth_rate_bps" json:"defaultBandwidthRateBps"`
	// +optional
	// +kubebuilder:default=12500000
	DefaultBandwidthBurstBytes int64 `yaml:"default_bandwidth_burst_bytes" json:"defaultBandwidthBurstBytes"`
	// +optional
	// +kubebuilder:default=10
	BandwidthAccountingInterval int `yaml:"bandwidth_accounting_interval" json:"bandwidthAccountingInterval"`
	// +optional
	// +kubebuilder:default={}
	Network NetworkProviderConfig `yaml:"network" json:"network"`

	// Pause/Resume
	// +optional
	// +kubebuilder:default="10Mi"
	PauseMinMemoryRequest string `yaml:"pause_min_memory_request" json:"pauseMinMemoryRequest"`
	// +optional
	// +kubebuilder:default="32Mi"
	PauseMinMemoryLimit string `yaml:"pause_min_memory_limit" json:"pauseMinMemoryLimit"`
	// +optional
	// +kubebuilder:default="1.1"
	PauseMemoryBufferRatio string `yaml:"pause_memory_buffer_ratio" json:"pauseMemoryBufferRatio"`
	// +optional
	// +kubebuilder:default="10m"
	PauseMinCPU string `yaml:"pause_min_cpu" json:"pauseMinCPU"`

	// Timeouts
	// +optional
	// +kubebuilder:default="30s"
	ProcdClientTimeout metav1.Duration `yaml:"procd_client_timeout" json:"procdClientTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`

	// Procd config injected into sandbox pods
	// +optional
	// +kubebuilder:default={}
	ProcdConfig ProcdConfig `yaml:"procd_config" json:"procdConfig"`
}

// DefaultTemplateConfig holds defaults for the installed template.
type DefaultTemplateConfig struct {
	// +optional
	// +kubebuilder:default="default"
	Name string `yaml:"name" json:"name"`
	// +optional
	// +kubebuilder:default="sandbox0ai/otemplates:default-v0.1.0"
	Image string `yaml:"image" json:"image"`
	// +optional
	// +kubebuilder:default={}
	Pool DefaultTemplatePoolConfig `yaml:"pool" json:"pool"`
}

// DefaultTemplatePoolConfig holds pool defaults for the installed template.
type DefaultTemplatePoolConfig struct {
	// +optional
	// +kubebuilder:default=1
	MinIdle int32 `yaml:"min_idle" json:"minIdle"`
	// +optional
	// +kubebuilder:default=5
	MaxIdle int32 `yaml:"max_idle" json:"maxIdle"`
	// +optional
	// +kubebuilder:default=true
	AutoScale bool `yaml:"auto_scale" json:"autoScale"`
}

// NetworkProviderConfig holds network provider settings.
type NetworkProviderConfig struct {
	// Provider is the network provider name (noop, cilium).
	// +optional
	// +kubebuilder:default="cilium"
	// +kubebuilder:validation:Enum=noop;cilium
	Provider string `yaml:"provider" json:"provider"`

	// Cilium config is used when provider is set to cilium.
	// +optional
	// +kubebuilder:default={}
	Cilium CiliumConfig `yaml:"cilium" json:"cilium"`
}

// CiliumConfig holds Cilium-specific settings.
type CiliumConfig struct {
	// PolicyNamePrefix is the prefix for generated Cilium policies.
	// +optional
	// +kubebuilder:default="sandbox0"
	PolicyNamePrefix string `yaml:"policy_name_prefix" json:"policyNamePrefix"`

	// BaselinePolicyName is the name for the namespace baseline policy.
	// +optional
	// +kubebuilder:default="sandbox0-baseline"
	BaselinePolicyName string `yaml:"baseline_policy_name" json:"baselinePolicyName"`

	// SandboxSelectorLabelKey selects sandbox pods for baseline policy.
	// +optional
	// +kubebuilder:default="sandbox0.ai/sandbox-id"
	SandboxSelectorLabelKey string `yaml:"sandbox_selector_label_key" json:"sandboxSelectorLabelKey"`

	// CNPGroup is the API group for CiliumNetworkPolicy.
	// +optional
	// +kubebuilder:default="cilium.io"
	CNPGroup string `yaml:"cnp_group" json:"cnpGroup"`

	// CNPVersion is the API version for CiliumNetworkPolicy.
	// +optional
	// +kubebuilder:default="v2"
	CNPVersion string `yaml:"cnp_version" json:"cnpVersion"`

	// CNPKind is the Kind for CiliumNetworkPolicy.
	// +optional
	// +kubebuilder:default="CiliumNetworkPolicy"
	CNPKind string `yaml:"cnp_kind" json:"cnpKind"`

	// FieldManager is the SSA field manager name.
	// +optional
	// +kubebuilder:default="sandbox0-manager"
	FieldManager string `yaml:"field_manager" json:"fieldManager"`

	// EnableBandwidthAnnotations enables pod bandwidth annotations.
	// +optional
	// +kubebuilder:default=true
	EnableBandwidthAnnotations bool `yaml:"enable_bandwidth_annotations" json:"enableBandwidthAnnotations"`

	// EgressBandwidthAnnotation is the egress bandwidth annotation key.
	// +optional
	// +kubebuilder:default="kubernetes.io/egress-bandwidth"
	EgressBandwidthAnnotation string `yaml:"egress_bandwidth_annotation" json:"egressBandwidthAnnotation"`

	// IngressBandwidthAnnotation is the ingress bandwidth annotation key.
	// +optional
	// +kubebuilder:default="kubernetes.io/ingress-bandwidth"
	IngressBandwidthAnnotation string `yaml:"ingress_bandwidth_annotation" json:"ingressBandwidthAnnotation"`
}

// LoadManagerConfig returns the manager configuration.
func LoadManagerConfig() *ManagerConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &ManagerConfig{}
	}
	return cfg
}

func loadManagerConfig(path string) (*ManagerConfig, error) {
	cfg := &ManagerConfig{}
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
