package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DefaultSSHGatewayMetricsPort is the internal Prometheus metrics port shared
// by the ssh-gateway process and its service supervisor.
const DefaultSSHGatewayMetricsPort = 9090

// SSHGatewayConfig holds all configuration for ssh-gateway.
type SSHGatewayConfig struct {
	// Server configuration
	SSHPort  int    `yaml:"ssh_port" json:"sshPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration for gateway identity data.
	DatabaseURL      string `yaml:"database_url" json:"-"`
	DatabaseMaxConns int    `yaml:"database_max_conns" json:"databaseMaxConns"`
	DatabaseMinConns int    `yaml:"database_min_conns" json:"databaseMinConns"`

	// Upstream regional control-plane endpoint.
	RegionalGatewayURL string `yaml:"regional_gateway_url" json:"-"`

	// Internal authentication caller identity shared across control-plane and
	// data-plane internal requests.
	InternalAuthTTL            Duration `yaml:"internal_auth_ttl" json:"internalAuthTTL"`
	InternalAuthCaller         string   `yaml:"internal_auth_caller" json:"internalAuthCaller"`
	ControlPlanePrivateKeyPath string   `yaml:"control_plane_private_key_path" json:"-"`
	DataPlanePrivateKeyPath    string   `yaml:"data_plane_private_key_path" json:"-"`

	// SSH host key used by clients to verify the gateway identity.
	SSHHostKeyPath string `yaml:"ssh_host_key_path" json:"-"`

	// ResumeTimeout bounds how long ssh-gateway waits for a paused sandbox to
	// become reachable after requesting resume.
	ResumeTimeout      Duration `yaml:"resume_timeout" json:"resumeTimeout"`
	ResumePollInterval Duration `yaml:"resume_poll_interval" json:"resumePollInterval"`
	ShutdownTimeout    Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
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
