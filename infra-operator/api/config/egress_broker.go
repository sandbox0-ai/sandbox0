// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EgressBrokerConfig holds configuration for egress-broker.
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
}

// LoadEgressBrokerConfig returns the egress-broker configuration.
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
