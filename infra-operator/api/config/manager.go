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

	// builtin templates
	// +optional
	// +kubebuilder:default={}
	BuiltinTemplates []BuiltinTemplateConfig `yaml:"builtin_templates" json:"builtinTemplates"`
	DefaultClusterId string                  `yaml:"default_cluster_id" json:"-"`
	// +optional
	// +kubebuilder:default=true
	TemplateStoreEnabled bool `yaml:"template_store_enabled" json:"-"`

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

	// Netd apply wait
	// +optional
	// +kubebuilder:default="30s"
	NetdPolicyApplyTimeout metav1.Duration `yaml:"netd_policy_apply_timeout" json:"netdPolicyApplyTimeout"`
	// +optional
	// +kubebuilder:default="500ms"
	NetdPolicyApplyPollInterval metav1.Duration `yaml:"netd_policy_apply_poll_interval" json:"netdPolicyApplyPollInterval"`

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
	// +kubebuilder:default="6s"
	ProcdInitTimeout metav1.Duration `yaml:"procd_init_timeout" json:"procdInitTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`

	// Procd config injected into sandbox pods
	// +optional
	// +kubebuilder:default={}
	ProcdConfig ProcdConfig `yaml:"procd_config" json:"procdConfig"`
}

// BuiltinTemplateConfig defines a system builtin template.
type BuiltinTemplateConfig struct {
	TemplateID  string                   `yaml:"template_id" json:"templateId"`
	Image       string                   `yaml:"image" json:"image"`
	DisplayName string                   `yaml:"display_name" json:"displayName"`
	Description string                   `yaml:"description" json:"description"`
	Pool        BuiltinTemplatePoolConfig `yaml:"pool" json:"pool"`
}

// BuiltinTemplatePoolConfig holds pool defaults for builtin templates.
type BuiltinTemplatePoolConfig struct {
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
