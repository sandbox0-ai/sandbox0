// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EdgeGatewayConfig holds all configuration for edge-gateway.
type EdgeGatewayConfig struct {
	// Edition: "saas" or "self-hosted"
	// +optional
	// +kubebuilder:default="self-hosted"
	Edition string `yaml:"edition" json:"edition"`

	// Server configuration
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration (for API key validation)
	// +optional
	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`

	// Upstream service
	// +optional
	DefaultInternalGatewayURL string `yaml:"default_internal_gateway_url" json:"-"`

	// Scheduler configuration (optional, for multi-cluster mode)
	// +optional
	SchedulerEnabled bool `yaml:"scheduler_enabled" json:"schedulerEnabled"`
	// +optional
	SchedulerURL string `yaml:"scheduler_url" json:"schedulerUrl"`

	// Internal Authentication
	// +optional
	// +kubebuilder:default="30s"
	InternalAuthTTL metav1.Duration `yaml:"internal_auth_ttl" json:"internalAuthTTL"`
	// +optional
	// +kubebuilder:default="edge-gateway"
	InternalAuthCaller string `yaml:"internal_auth_caller" json:"internalAuthCaller"`

	// Cache configuration
	// +optional
	// +kubebuilder:default="30s"
	ClusterCacheTTL metav1.Duration `yaml:"cluster_cache_ttl" json:"clusterCacheTTL"`

	// JWT Configuration
	// +optional
	JWTSecret string `yaml:"jwt_secret" json:"jwtSecret"`
	// +optional
	// +kubebuilder:default="edge-gateway"
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

	// Timeouts
	// +optional
	// +kubebuilder:default="30s"
	ProxyTimeout metav1.Duration `yaml:"proxy_timeout" json:"proxyTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`
	// +optional
	// +kubebuilder:default="30s"
	ServerReadTimeout metav1.Duration `yaml:"server_read_timeout" json:"serverReadTimeout"`
	// +optional
	// +kubebuilder:default="60s"
	ServerWriteTimeout metav1.Duration `yaml:"server_write_timeout" json:"serverWriteTimeout"`
	// +optional
	// +kubebuilder:default="120s"
	ServerIdleTimeout metav1.Duration `yaml:"server_idle_timeout" json:"serverIdleTimeout"`

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
}

// BuiltInAuthConfig configures the built-in authentication.
type BuiltInAuthConfig struct {
	// Enabled enables built-in email/password authentication
	// +optional
	// +kubebuilder:default=true
	Enabled bool `yaml:"enabled" json:"enabled"`

	// AllowRegistration allows new users to register
	// +optional
	AllowRegistration bool `yaml:"allow_registration" json:"allowRegistration"`

	// EmailVerificationRequired requires email verification
	// +optional
	EmailVerificationRequired bool `yaml:"email_verification_required" json:"emailVerificationRequired"`

	// AdminOnly restricts built-in auth to admin accounts only
	// +optional
	AdminOnly bool `yaml:"admin_only" json:"adminOnly"`

	// InitUser is the initial admin user (for self-hosted)
	// +optional
	InitUser *InitUserConfig `yaml:"init_user" json:"-"`
}

// InitUserConfig configures the initial admin user.
type InitUserConfig struct {
	// +optional
	Email string `yaml:"email" json:"email"`
	// +optional
	Password string `yaml:"password" json:"password"`
	// +optional
	Name string `yaml:"name" json:"name"`
}

// OIDCProviderConfig configures an OIDC provider.
type OIDCProviderConfig struct {
	// ID is the unique identifier for the provider (e.g., "github", "google")
	// +optional
	ID string `yaml:"id" json:"id"`

	// Name is the display name
	// +optional
	Name string `yaml:"name" json:"name"`

	// Enabled toggles the provider
	// +optional
	Enabled bool `yaml:"enabled" json:"enabled"`

	// ClientID is the OAuth client ID
	// +optional
	ClientID string `yaml:"client_id" json:"clientId"`

	// ClientSecret is the OAuth client secret
	// +optional
	ClientSecret string `yaml:"client_secret" json:"clientSecret"`

	// DiscoveryURL is the OIDC discovery URL (.well-known/openid-configuration)
	// +optional
	DiscoveryURL string `yaml:"discovery_url" json:"discoveryUrl"`

	// Scopes are the OAuth scopes to request
	// +optional
	// +kubebuilder:default={"openid","email","profile"}
	Scopes []string `yaml:"scopes" json:"scopes"`

	// AutoProvision automatically creates users on first login
	// +optional
	AutoProvision bool `yaml:"auto_provision" json:"autoProvision"`

	// TeamMapping configures automatic team assignment
	// +optional
	TeamMapping *TeamMappingConfig `yaml:"team_mapping" json:"teamMapping"`
}

// TeamMappingConfig configures automatic team mapping for OIDC.
type TeamMappingConfig struct {
	// Domain filters users by email domain
	// +optional
	Domain string `yaml:"domain" json:"domain"`

	// DefaultRole is the role assigned to new users
	// +optional
	DefaultRole string `yaml:"default_role" json:"defaultRole"`

	// DefaultTeamID is the team to add users to
	// +optional
	DefaultTeamID string `yaml:"default_team_id" json:"defaultTeamId"`
}

// LoadEdgeGatewayConfig returns the edge-gateway configuration.
func LoadEdgeGatewayConfig() *EdgeGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadEdgeGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &EdgeGatewayConfig{}
	}
	return cfg
}

func loadEdgeGatewayConfig(path string) (*EdgeGatewayConfig, error) {
	cfg := &EdgeGatewayConfig{}
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

// GetOIDCProvider returns an OIDC provider by ID.
func (c *EdgeGatewayConfig) GetOIDCProvider(id string) *OIDCProviderConfig {
	for i := range c.OIDCProviders {
		if c.OIDCProviders[i].ID == id && c.OIDCProviders[i].Enabled {
			return &c.OIDCProviders[i]
		}
	}
	return nil
}

// GetEnabledOIDCProviders returns all enabled OIDC providers.
func (c *EdgeGatewayConfig) GetEnabledOIDCProviders() []OIDCProviderConfig {
	var providers []OIDCProviderConfig
	for _, p := range c.OIDCProviders {
		if p.Enabled {
			providers = append(providers, p)
		}
	}
	return providers
}
