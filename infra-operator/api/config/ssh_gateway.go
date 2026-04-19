// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SSHGatewayConfig holds all configuration for ssh-gateway.
type SSHGatewayConfig struct {
	// Server configuration
	// +optional
	// +kubebuilder:default=2222
	SSHPort int `yaml:"ssh_port" json:"sshPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration for gateway identity data.
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`

	// Upstream regional control-plane endpoint.
	RegionalGatewayURL string `yaml:"regional_gateway_url" json:"-"`

	// Internal authentication caller identity shared across control-plane and
	// data-plane internal requests.
	// +optional
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `yaml:"internal_auth_ttl" json:"internalAuthTTL"`
	// +optional
	// +kubebuilder:default="ssh-gateway"
	InternalAuthCaller string `yaml:"internal_auth_caller" json:"internalAuthCaller"`
	// +optional
	// +kubebuilder:default="/secrets/control_plane_internal_jwt_private.key"
	ControlPlanePrivateKeyPath string `yaml:"control_plane_private_key_path" json:"-"`
	// +optional
	// +kubebuilder:default="/secrets/data_plane_internal_jwt_private.key"
	DataPlanePrivateKeyPath string `yaml:"data_plane_private_key_path" json:"-"`

	// SSH host key used by clients to verify the gateway identity.
	// +optional
	// +kubebuilder:default="/secrets/ssh_host_ed25519_key"
	SSHHostKeyPath string `yaml:"ssh_host_key_path" json:"-"`

	// ResumeTimeout bounds how long ssh-gateway waits for a paused sandbox to
	// become reachable after requesting resume.
	// +optional
	// +kubebuilder:default="30s"
	ResumeTimeout metav1.Duration `yaml:"resume_timeout" json:"resumeTimeout"`
	// +optional
	// +kubebuilder:default="500ms"
	ResumePollInterval metav1.Duration `yaml:"resume_poll_interval" json:"resumePollInterval"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
}

// LoadSSHGatewayConfig returns the ssh-gateway configuration.
func LoadSSHGatewayConfig() *SSHGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadSSHGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &SSHGatewayConfig{}
	}
	return cfg
}

func loadSSHGatewayConfig(path string) (*SSHGatewayConfig, error) {
	cfg := &SSHGatewayConfig{}
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
