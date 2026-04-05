// +kubebuilder:object:generate=true
package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GatewayConfig holds shared gateway configuration used by edge/cluster gateway.
type GatewayConfig struct {
	// JWT Configuration
	// +optional
	JWTSecret string `yaml:"jwt_secret" json:"-"`
	// +optional
	JWTPrivateKeyPEM string `yaml:"jwt_private_key_pem" json:"-"`
	// +optional
	JWTPublicKeyPEM string `yaml:"jwt_public_key_pem" json:"-"`
	// +optional
	JWTPrivateKeyFile string `yaml:"jwt_private_key_file" json:"-"`
	// +optional
	JWTPublicKeyFile string `yaml:"jwt_public_key_file" json:"-"`
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

	// RegionID is the region identifier used by tenancy and routing contracts,
	// for example "aws-us-east-1".
	// +optional
	RegionID string `yaml:"region_id" json:"regionId"`

	// Public exposure host routing configuration.
	// Host format is fixed as: <exposureLabel>.<regionLabel>.<rootDomain>
	// These fields are injected by infra-operator from Sandbox0Infra.Spec.PublicExposure
	// +optional
	// +kubebuilder:default=true
	PublicExposureEnabled bool `yaml:"public_exposure_enabled" json:"-"`
	// +optional
	// +kubebuilder:default="sandbox0.app"
	PublicRootDomain string `yaml:"public_root_domain" json:"-"`
	// PublicRegionID is the region label used in exposure hosts,
	// for example "aws-us-east-1". It should match RegionID.
	// +optional
	// +kubebuilder:default="aws-us-east-1"
	PublicRegionID string `yaml:"public_region_id" json:"-"`
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
	// +optional
	HomeRegionID string `yaml:"home_region_id" json:"homeRegionId"`
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

	// TokenEndpointAuthMethod controls how the client authenticates to the token endpoint.
	// Supported values are "client_secret_basic" and "client_secret_post".
	// When omitted, the oauth2 library auto-detects the method.
	// +optional
	TokenEndpointAuthMethod string `yaml:"token_endpoint_auth_method" json:"tokenEndpointAuthMethod"`

	// Scopes are the OAuth scopes to request
	// +optional
	// +kubebuilder:default={"openid","email","profile"}
	Scopes []string `yaml:"scopes" json:"scopes"`

	// DeviceAuthorizationEnabled enables device authorization flow for CLI login.
	// +optional
	DeviceAuthorizationEnabled bool `yaml:"device_authorization_enabled" json:"deviceAuthorizationEnabled"`

	// DeviceAuthorizationEndpoint overrides the discovery metadata endpoint when set.
	// +optional
	DeviceAuthorizationEndpoint string `yaml:"device_authorization_endpoint" json:"deviceAuthorizationEndpoint"`

	// DeviceClientID overrides the OAuth client ID used for device authorization.
	// Falls back to ClientID when omitted.
	// +optional
	DeviceClientID string `yaml:"device_client_id" json:"deviceClientId"`

	// DeviceClientSecret overrides the OAuth client secret used for device authorization.
	// Falls back to ClientSecret when omitted.
	// +optional
	DeviceClientSecret string `yaml:"device_client_secret" json:"-"`

	// AutoProvision automatically creates users on first login
	// +optional
	AutoProvision bool `yaml:"auto_provision" json:"autoProvision"`

	// TeamMapping configures automatic team assignment
	// +optional
	TeamMapping *TeamMappingConfig `yaml:"team_mapping" json:"teamMapping"`

	// ExternalAuthPortalURL, when set, redirects unauthenticated browser users to this URL
	// instead of initiating the OIDC flow directly. Use for deployments that host their own
	// login portal that handles OIDC initiation externally.
	// +optional
	ExternalAuthPortalURL string `yaml:"external_auth_portal_url" json:"externalAuthPortalUrl"`
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
