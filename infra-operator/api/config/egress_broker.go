// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EgressBrokerConfig holds deprecated compatibility config for the legacy
// standalone egress auth resolver service. Runtime resolution now runs inside
// manager, and infra-operator maps the compatible fields forward.
type EgressBrokerConfig struct {
	// +optional
	// +kubebuilder:default=8082
	HTTPPort int `yaml:"http_port" json:"httpPort"`

	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// +optional
	// +kubebuilder:default="10s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`

	// +optional
	RegionID string `yaml:"region_id" json:"-"`

	// +optional
	ClusterID string `yaml:"cluster_id" json:"-"`

	// +optional
	DefaultResolveTTL metav1.Duration `yaml:"default_resolve_ttl" json:"defaultResolveTtl"`

	// +optional
	DatabaseURL string `yaml:"database_url" json:"-"`

	// +optional
	// +kubebuilder:default=10
	DatabaseMaxConns int32 `yaml:"database_max_conns" json:"databaseMaxConns"`

	// +optional
	// +kubebuilder:default=2
	DatabaseMinConns int32 `yaml:"database_min_conns" json:"databaseMinConns"`

	// +optional
	StaticAuth []StaticEgressAuthConfig `yaml:"static_auth" json:"staticAuth"`
}

// StaticEgressAuthConfig defines a static auth directive for phase4 HTTP injection.
type StaticEgressAuthConfig struct {
	AuthRef string            `yaml:"auth_ref" json:"authRef"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	TTL     metav1.Duration   `yaml:"ttl" json:"ttl"`
}

// LoadEgressBrokerConfig returns the legacy runtime resolver configuration.
func LoadEgressBrokerConfig() *EgressBrokerConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadEgressBrokerConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &EgressBrokerConfig{}
	}
	applyEgressBrokerDefaults(cfg)
	return cfg
}

func loadEgressBrokerConfig(path string) (*EgressBrokerConfig, error) {
	cfg := &EgressBrokerConfig{}
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	data = []byte(os.ExpandEnv(string(data)))
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}

func applyEgressBrokerDefaults(cfg *EgressBrokerConfig) {
	if cfg == nil {
		return
	}
	if cfg.DefaultResolveTTL.Duration == 0 {
		cfg.DefaultResolveTTL = metav1.Duration{Duration: 5 * time.Minute}
	}
	if cfg.DatabaseMaxConns == 0 {
		cfg.DatabaseMaxConns = 10
	}
	if cfg.DatabaseMinConns == 0 {
		cfg.DatabaseMinConns = 2
	}
	for idx := range cfg.StaticAuth {
		if cfg.StaticAuth[idx].TTL.Duration == 0 {
			cfg.StaticAuth[idx].TTL = cfg.DefaultResolveTTL
		}
	}
}
