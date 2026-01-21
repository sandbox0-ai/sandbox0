// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"gopkg.in/yaml.v3"
)

// EdgeGatewayConfig holds all configuration for edge-gateway.
type EdgeGatewayConfig struct {
	// Edition: "saas" or "self-hosted"
	Edition string `yaml:"edition" json:"edition"`

	// Server configuration
	HTTPPort int    `yaml:"http_port" json:"httpPort"`
	LogLevel string `yaml:"log_level" json:"logLevel"`

	// Database configuration (for API key validation)
	DatabaseURL string `yaml:"database_url" json:"databaseUrl"`

	// Upstream service
	DefaultInternalGatewayURL string `yaml:"default_internal_gateway_url" json:"defaultInternalGatewayUrl"`

	// Scheduler configuration (optional, for multi-cluster mode)
	SchedulerEnabled bool   `yaml:"scheduler_enabled" json:"schedulerEnabled"`
	SchedulerURL     string `yaml:"scheduler_url" json:"schedulerUrl"`

	// JWT Configuration
	JWTSecret          string          `yaml:"jwt_secret" json:"jwtSecret"`
	JWTAccessTokenTTL  metav1.Duration `yaml:"jwt_access_token_ttl" json:"jwtAccessTokenTTL"`
	JWTRefreshTokenTTL metav1.Duration `yaml:"jwt_refresh_token_ttl" json:"jwtRefreshTokenTTL"`

	// Rate limiting
	RateLimitRPS   int `yaml:"rate_limit_rps" json:"rateLimitRPS"`
	RateLimitBurst int `yaml:"rate_limit_burst" json:"rateLimitBurst"`

	// Timeouts
	ProxyTimeout    metav1.Duration `yaml:"proxy_timeout" json:"proxyTimeout"`
	ShutdownTimeout metav1.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`

	// Built-in Authentication
	BuiltInAuth BuiltInAuthConfig `yaml:"built_in_auth" json:"builtInAuth"`

	// OIDC Providers
	OIDCProviders []OIDCProviderConfig `yaml:"oidc_providers" json:"oidcProviders"`

	// Base URL for OIDC callbacks
	BaseURL string `yaml:"base_url" json:"baseUrl"`
}

// BuiltInAuthConfig configures the built-in authentication.
type BuiltInAuthConfig struct {
	// Enabled enables built-in email/password authentication
	Enabled bool `yaml:"enabled" json:"enabled"`

	// AllowRegistration allows new users to register
	AllowRegistration bool `yaml:"allow_registration" json:"allowRegistration"`

	// EmailVerificationRequired requires email verification
	EmailVerificationRequired bool `yaml:"email_verification_required" json:"emailVerificationRequired"`

	// AdminOnly restricts built-in auth to admin accounts only
	AdminOnly bool `yaml:"admin_only" json:"adminOnly"`

	// InitUser is the initial admin user (for self-hosted)
	InitUser *InitUserConfig `yaml:"init_user" json:"initUser"`
}

// InitUserConfig configures the initial admin user.
type InitUserConfig struct {
	Email    string `yaml:"email" json:"email"`
	Password string `yaml:"password" json:"password"`
	Name     string `yaml:"name" json:"name"`
}

// OIDCProviderConfig configures an OIDC provider.
type OIDCProviderConfig struct {
	// ID is the unique identifier for the provider (e.g., "github", "google")
	ID string `yaml:"id" json:"id"`

	// Name is the display name
	Name string `yaml:"name" json:"name"`

	// Enabled toggles the provider
	Enabled bool `yaml:"enabled" json:"enabled"`

	// ClientID is the OAuth client ID
	ClientID string `yaml:"client_id" json:"clientId"`

	// ClientSecret is the OAuth client secret
	ClientSecret string `yaml:"client_secret" json:"clientSecret"`

	// DiscoveryURL is the OIDC discovery URL (.well-known/openid-configuration)
	DiscoveryURL string `yaml:"discovery_url" json:"discoveryUrl"`

	// Scopes are the OAuth scopes to request
	Scopes []string `yaml:"scopes" json:"scopes"`

	// AutoProvision automatically creates users on first login
	AutoProvision bool `yaml:"auto_provision" json:"autoProvision"`

	// TeamMapping configures automatic team assignment
	TeamMapping *TeamMappingConfig `yaml:"team_mapping" json:"teamMapping"`
}

// TeamMappingConfig configures automatic team mapping for OIDC.
type TeamMappingConfig struct {
	// Domain filters users by email domain
	Domain string `yaml:"domain" json:"domain"`

	// DefaultRole is the role assigned to new users
	DefaultRole string `yaml:"default_role" json:"defaultRole"`

	// DefaultTeamID is the team to add users to
	DefaultTeamID string `yaml:"default_team_id" json:"defaultTeamId"`
}

// DefaultEdgeGatewayConfig returns the default configuration.
func DefaultEdgeGatewayConfig() *EdgeGatewayConfig {
	return &EdgeGatewayConfig{
		Edition:                   "self-hosted",
		HTTPPort:                  8080,
		LogLevel:                  "info",
		DatabaseURL:               "",
		DefaultInternalGatewayURL: "http://internal-gateway.sandbox0-system:8443",
		JWTSecret:                 "",
		JWTAccessTokenTTL:         metav1.Duration{Duration: 15 * time.Minute},
		JWTRefreshTokenTTL:        metav1.Duration{Duration: 7 * 24 * time.Hour},
		RateLimitRPS:              100,
		RateLimitBurst:            200,
		ProxyTimeout:              metav1.Duration{Duration: 30 * time.Second},
		ShutdownTimeout:           metav1.Duration{Duration: 30 * time.Second},
		BaseURL:                   "http://localhost:8080",
		BuiltInAuth: BuiltInAuthConfig{
			Enabled:           true,
			AllowRegistration: false,
			AdminOnly:         false,
			InitUser: &InitUserConfig{
				Email:    "admin@localhost",
				Password: "admin123",
				Name:     "Admin",
			},
		},
	}
}

// LoadEdgeGatewayConfig returns the edge-gateway configuration.
func LoadEdgeGatewayConfig() *EdgeGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadEdgeGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using defaults\n", path, err)
		cfg = DefaultEdgeGatewayConfig()
	}
	return cfg
}

func loadEdgeGatewayConfig(path string) (*EdgeGatewayConfig, error) {
	cfg := DefaultEdgeGatewayConfig()
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

	// Apply defaults to OIDC providers
	for i := range cfg.OIDCProviders {
		if len(cfg.OIDCProviders[i].Scopes) == 0 {
			cfg.OIDCProviders[i].Scopes = []string{"openid", "email", "profile"}
		}
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
