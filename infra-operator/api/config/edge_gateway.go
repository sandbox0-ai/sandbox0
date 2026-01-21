package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// EdgeGatewayConfig holds all configuration for edge-gateway.
type EdgeGatewayConfig struct {
	// Edition: "saas" or "self-hosted"
	Edition string `yaml:"edition"`

	// Server configuration
	HTTPPort int    `yaml:"http_port"`
	LogLevel string `yaml:"log_level"`

	// Database configuration (for API key validation)
	DatabaseURL string `yaml:"database_url"`

	// Upstream service
	DefaultInternalGatewayURL string `yaml:"default_internal_gateway_url"`

	// Scheduler configuration (optional, for multi-cluster mode)
	SchedulerEnabled bool   `yaml:"scheduler_enabled"`
	SchedulerURL     string `yaml:"scheduler_url"`

	// JWT Configuration
	JWTSecret          string        `yaml:"jwt_secret"`
	JWTAccessTokenTTL  time.Duration `yaml:"jwt_access_token_ttl"`
	JWTRefreshTokenTTL time.Duration `yaml:"jwt_refresh_token_ttl"`

	// Rate limiting
	RateLimitRPS   int `yaml:"rate_limit_rps"`
	RateLimitBurst int `yaml:"rate_limit_burst"`

	// Timeouts
	ProxyTimeout    time.Duration `yaml:"proxy_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`

	// Built-in Authentication
	BuiltInAuth BuiltInAuthConfig `yaml:"built_in_auth"`

	// OIDC Providers
	OIDCProviders []OIDCProviderConfig `yaml:"oidc_providers"`

	// Base URL for OIDC callbacks
	BaseURL string `yaml:"base_url"`
}

// BuiltInAuthConfig configures the built-in authentication.
type BuiltInAuthConfig struct {
	// Enabled enables built-in email/password authentication
	Enabled bool `yaml:"enabled"`

	// AllowRegistration allows new users to register
	AllowRegistration bool `yaml:"allow_registration"`

	// EmailVerificationRequired requires email verification
	EmailVerificationRequired bool `yaml:"email_verification_required"`

	// AdminOnly restricts built-in auth to admin accounts only
	AdminOnly bool `yaml:"admin_only"`

	// InitUser is the initial admin user (for self-hosted)
	InitUser *InitUserConfig `yaml:"init_user"`
}

// InitUserConfig configures the initial admin user.
type InitUserConfig struct {
	Email    string `yaml:"email"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
}

// OIDCProviderConfig configures an OIDC provider.
type OIDCProviderConfig struct {
	// ID is the unique identifier for the provider (e.g., "github", "google")
	ID string `yaml:"id"`

	// Name is the display name
	Name string `yaml:"name"`

	// Enabled toggles the provider
	Enabled bool `yaml:"enabled"`

	// ClientID is the OAuth client ID
	ClientID string `yaml:"client_id"`

	// ClientSecret is the OAuth client secret
	ClientSecret string `yaml:"client_secret"`

	// DiscoveryURL is the OIDC discovery URL (.well-known/openid-configuration)
	DiscoveryURL string `yaml:"discovery_url"`

	// Scopes are the OAuth scopes to request
	Scopes []string `yaml:"scopes"`

	// AutoProvision automatically creates users on first login
	AutoProvision bool `yaml:"auto_provision"`

	// TeamMapping configures automatic team assignment
	TeamMapping *TeamMappingConfig `yaml:"team_mapping"`
}

// TeamMappingConfig configures automatic team mapping for OIDC.
type TeamMappingConfig struct {
	// Domain filters users by email domain
	Domain string `yaml:"domain"`

	// DefaultRole is the role assigned to new users
	DefaultRole string `yaml:"default_role"`

	// DefaultTeamID is the team to add users to
	DefaultTeamID string `yaml:"default_team_id"`
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
		JWTAccessTokenTTL:         15 * time.Minute,
		JWTRefreshTokenTTL:        7 * 24 * time.Hour,
		RateLimitRPS:              100,
		RateLimitBurst:            200,
		ProxyTimeout:              30 * time.Second,
		ShutdownTimeout:           30 * time.Second,
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
