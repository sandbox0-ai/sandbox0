// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InternalGatewayConfig holds all configuration for internal-gateway.
type InternalGatewayConfig struct {
	// Server configuration
	// +optional
	// +kubebuilder:default=8443
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Upstream services
	ManagerURL      string `yaml:"manager_url" json:"-"`
	StorageProxyURL string `yaml:"storage_proxy_url" json:"-"`

	// Internal authentication (for validating requests from edge-gateway and
	// generating tokens for downstream services)
	// AuthMode controls which authentication modes are accepted on /api/v1.
	// Allowed values: "internal", "public", "both".
	// +optional
	// +kubebuilder:default="internal"
	AuthMode string `yaml:"auth_mode" json:"authMode"`
	// AllowedCallers is the list of services allowed to call internal-gateway
	// Default: ["edge-gateway"], can include "scheduler" for multi-cluster mode
	// +optional
	// +kubebuilder:default={"edge-gateway","scheduler"}
	AllowedCallers []string `yaml:"allowed_callers" json:"allowedCallers"`

	// Timeouts
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	// +optional
	// +kubebuilder:default="10s"
	HealthCheckPeriod metav1.Duration `yaml:"health_check_period" json:"healthCheckPeriod"`

	// Proxy configuration
	// +optional
	// +kubebuilder:default="10s"
	ProxyTimeout metav1.Duration `yaml:"proxy_timeout" json:"proxyTimeout"`

	// Public gateway (external auth) configuration
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`

	// Identity and Teams
	// +optional
	// +kubebuilder:default="Personal Team"
	DefaultTeamName string `yaml:"default_team_name" json:"defaultTeamName"`

	// Built-in Authentication
	// +optional
	// +kubebuilder:default={}
	BuiltInAuth BuiltInAuthConfig `yaml:"built_in_auth" json:"builtInAuth"`

	// OIDC Providers
	// +optional
	OIDCProviders []OIDCProviderConfig `yaml:"oidc_providers" json:"oidcProviders"`
	// +optional
	// +kubebuilder:default="10m"
	OIDCStateTTL metav1.Duration `yaml:"oidc_state_ttl" json:"oidcStateTTL"`
	// +optional
	// +kubebuilder:default="5m"
	OIDCStateCleanupInterval metav1.Duration `yaml:"oidc_state_cleanup_interval" json:"oidcStateCleanupInterval"`

	// Base URL for OIDC callbacks
	// +optional
	// +kubebuilder:default="http://localhost:8080"
	BaseURL string `yaml:"base_url" json:"baseUrl"`

	// JWT Configuration
	// +optional
	JWTSecret string `yaml:"jwt_secret" json:"jwtSecret"`
	// +optional
	// +kubebuilder:default="internal-gateway"
	JWTIssuer string `yaml:"jwt_issuer" json:"jwtIssuer"`
	// +optional
	// +kubebuilder:default="15m"
	JWTAccessTokenTTL metav1.Duration `yaml:"jwt_access_token_ttl" json:"jwtAccessTokenTTL"`
	// +optional
	// +kubebuilder:default="168h"
	JWTRefreshTokenTTL metav1.Duration `yaml:"jwt_refresh_token_ttl" json:"jwtRefreshTokenTTL"`

	// Rate limiting
	// +optional
	// +kubebuilder:default=100
	RateLimitRPS int `yaml:"rate_limit_rps" json:"rateLimitRPS"`
	// +optional
	// +kubebuilder:default=200
	RateLimitBurst int `yaml:"rate_limit_burst" json:"rateLimitBurst"`
	// +optional
	// +kubebuilder:default="10m"
	RateLimitCleanupInterval metav1.Duration `yaml:"rate_limit_cleanup_interval" json:"rateLimitCleanupInterval"`

	// Permissions
	// +optional
	// +kubebuilder:default={"*:*"}
	SchedulerPermissions []string `yaml:"scheduler_permissions" json:"schedulerPermissions"`
	// +optional
	// +kubebuilder:default={"sandboxvolume:read","sandboxvolume:write"}
	ProcdStoragePermissions []string `yaml:"procd_storage_permissions" json:"procdStoragePermissions"`
}

// LoadInternalGatewayConfig returns the internal-gateway configuration.
func LoadInternalGatewayConfig() *InternalGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadInternalGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &InternalGatewayConfig{}
	}
	return cfg
}

func loadInternalGatewayConfig(path string) (*InternalGatewayConfig, error) {
	cfg := &InternalGatewayConfig{}
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
