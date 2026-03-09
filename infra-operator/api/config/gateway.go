// +kubebuilder:object:generate=true
package config

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GatewayConfig holds shared gateway configuration used by edge/internal gateway.
type GatewayConfig struct {
	// JWT Configuration
	// +optional
	JWTSecret string `yaml:"jwt_secret" json:"-"`
	// +optional
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

	// Public exposure host routing configuration.
	// Host format is fixed as: <exposureLabel>.<region>.<rootDomain>
	// These fields are injected by infra-operator from Sandbox0Infra.Spec.PublicExposure
	// +optional
	// +kubebuilder:default=true
	PublicExposureEnabled bool `yaml:"public_exposure_enabled" json:"-"`
	// +optional
	// +kubebuilder:default="sandbox0.app"
	PublicRootDomain string `yaml:"public_root_domain" json:"-"`
	// +optional
	// +kubebuilder:default="aws-us-east-1"
	PublicRegionID string `yaml:"public_region_id" json:"-"`

	// Agent skill distribution configuration.
	// +optional
	// +kubebuilder:default={}
	AgentSkill AgentSkillConfig `yaml:"agent_skill" json:"agentSkill"`
}

// AgentSkillConfig configures the deployment-matched coding-agent skill
// artifact exposed by gateway APIs.
type AgentSkillConfig struct {
	// +optional
	// +kubebuilder:default=true
	Enabled bool `yaml:"enabled" json:"enabled"`
	// +optional
	// +kubebuilder:default="sandbox0"
	Name string `yaml:"name" json:"name"`
	// +optional
	ReleaseVersion string `yaml:"release_version" json:"releaseVersion"`
	// +optional
	// +kubebuilder:default="https://github.com/sandbox0-ai/sandbox0/releases/download"
	ArtifactBaseURL string `yaml:"artifact_base_url" json:"artifactBaseURL"`
	// +optional
	// +kubebuilder:default="sandbox0-agent-skill"
	ArtifactPrefix string `yaml:"artifact_prefix" json:"artifactPrefix"`
}

func (c *GatewayConfig) ApplyDefaults() {
	if strings.TrimSpace(c.AgentSkill.Name) == "" {
		c.AgentSkill.Name = "sandbox0"
	}
	if strings.TrimSpace(c.AgentSkill.ArtifactBaseURL) == "" {
		c.AgentSkill.ArtifactBaseURL = "https://github.com/sandbox0-ai/sandbox0/releases/download"
	}
	if strings.TrimSpace(c.AgentSkill.ArtifactPrefix) == "" {
		c.AgentSkill.ArtifactPrefix = "sandbox0-agent-skill"
	}
	if strings.TrimSpace(c.AgentSkill.ReleaseVersion) != "" {
		c.AgentSkill.Enabled = true
	}
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

// HasEnabledOIDCProviders returns true when at least one OIDC provider is enabled.
func HasEnabledOIDCProviders(providers []OIDCProviderConfig) bool {
	for _, p := range providers {
		if p.Enabled {
			return true
		}
	}
	return false
}
