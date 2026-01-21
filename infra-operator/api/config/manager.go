// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"gopkg.in/yaml.v3"
)

// ManagerConfig holds the configuration for the manager.
type ManagerConfig struct {
	// HTTP Server
	HTTPPort int `yaml:"http_port" json:"httpPort"`

	// manager docker image, used to copy the procd binary to sandbox pod
	ManagerImage string `yaml:"manager_image" json:"managerImage"`

	// template
	DefaultTemplateName      string `yaml:"default_template_name" json:"defaultTemplateName"`
	DefaultTemplateNamespace string `yaml:"default_template_namespace" json:"defaultTemplateNamespace"`
	DefaultTemplateImage     string `yaml:"default_template_image" json:"defaultTemplateImage"`
	DefaultClusterId         string `yaml:"default_cluster_id" json:"defaultClusterId"`

	// Kubernetes
	KubeConfig     string          `yaml:"kube_config" json:"kubeConfig"`
	LeaderElection bool            `yaml:"leader_election" json:"leaderElection"`
	ResyncPeriod   metav1.Duration `yaml:"resync_period" json:"resyncPeriod"`

	// Database
	DatabaseURL string `yaml:"database_url" json:"databaseUrl"`

	// Cleanup Controller
	CleanupInterval metav1.Duration `yaml:"cleanup_interval" json:"cleanupInterval"`

	// Logging
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Metrics
	MetricsPort int `yaml:"metrics_port" json:"metricsPort"`

	// Webhook
	WebhookPort     int    `yaml:"webhook_port" json:"webhookPort"`
	WebhookCertPath string `yaml:"webhook_cert_path" json:"webhookCertPath"`
	WebhookKeyPath  string `yaml:"webhook_key_path" json:"webhookKeyPath"`

	// Sandbox
	DefaultSandboxTTL metav1.Duration `yaml:"default_sandbox_ttl" json:"defaultSandboxTTL"`

	// Procd config injected into sandbox pods
	ProcdConfig ProcdConfig `yaml:"procd_config" json:"procdConfig"`
}

// DefaultManagerConfig returns the default configuration.
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		HTTPPort:                 8080,
		ManagerImage:             "sandbox0ai/infra:latest",
		DefaultTemplateName:      "default",
		DefaultTemplateNamespace: "sandbox0",
		DefaultTemplateImage:     "sandbox0ai/otemplates:default-v0.1.0",
		DefaultClusterId:         "default",
		KubeConfig:               "",
		LeaderElection:           true,
		ResyncPeriod:             metav1.Duration{Duration: 30 * time.Second},
		DatabaseURL:              "",
		CleanupInterval:          metav1.Duration{Duration: 60 * time.Second},
		LogLevel:                 "info",
		MetricsPort:              9090,
		WebhookPort:              9443,
		WebhookCertPath:          "/tmp/k8s-webhook-server/serving-certs/tls.crt",
		WebhookKeyPath:           "/tmp/k8s-webhook-server/serving-certs/tls.key",
		DefaultSandboxTTL:        metav1.Duration{Duration: 5 * time.Minute},
		ProcdConfig:              DefaultProcdConfig(),
	}
}

// LoadManagerConfig returns the manager configuration.
func LoadManagerConfig() *ManagerConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		cfg = DefaultManagerConfig()
	}
	return cfg
}

func loadManagerConfig(path string) (*ManagerConfig, error) {
	cfg := DefaultManagerConfig()
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
