package config

// GatewayConfig holds shared gateway configuration used by edge/cluster gateway.
type GatewayConfig struct {
	// JWT Configuration
	JWTSecret          string   `yaml:"jwt_secret" json:"-"`
	JWTPrivateKeyPEM   string   `yaml:"jwt_private_key_pem" json:"-"`
	JWTPublicKeyPEM    string   `yaml:"jwt_public_key_pem" json:"-"`
	JWTPrivateKeyFile  string   `yaml:"jwt_private_key_file" json:"-"`
	JWTPublicKeyFile   string   `yaml:"jwt_public_key_file" json:"-"`
	JWTIssuer          string   `yaml:"jwt_issuer" json:"jwtIssuer"`
	JWTAccessTokenTTL  Duration `yaml:"jwt_access_token_ttl" json:"jwtAccessTokenTTL"`
	JWTRefreshTokenTTL Duration `yaml:"jwt_refresh_token_ttl" json:"jwtRefreshTokenTTL"`

	// Shared Redis cache backend.
	RedisURL       string   `yaml:"redis_url" json:"-"`
	RedisKeyPrefix string   `yaml:"redis_key_prefix" json:"-"`
	RedisTimeout   Duration `yaml:"redis_timeout" json:"-"`

	// Rate limiting
	RateLimitRPS             int      `yaml:"rate_limit_rps" json:"rateLimitRPS"`
	RateLimitBurst           int      `yaml:"rate_limit_burst" json:"rateLimitBurst"`
	RateLimitCleanupInterval Duration `yaml:"rate_limit_cleanup_interval" json:"rateLimitCleanupInterval"`
	// RateLimitBackend selects the rate limit backend. Supported values: "memory", "redis".
	RateLimitBackend string `yaml:"rate_limit_backend" json:"-"`
	// RateLimitRedisURL configures the Redis backend when RateLimitBackend is "redis".
	RateLimitRedisURL string `yaml:"rate_limit_redis_url" json:"-"`
	// RateLimitRedisKeyPrefix prefixes Redis keys used by the rate limiter.
	RateLimitRedisKeyPrefix string `yaml:"rate_limit_redis_key_prefix" json:"-"`
	// RateLimitRedisTimeout bounds each Redis rate limit operation.
	RateLimitRedisTimeout Duration `yaml:"rate_limit_redis_timeout" json:"-"`
	// RateLimitFailOpen allows traffic when the configured backend is temporarily unavailable.
	RateLimitFailOpen bool `yaml:"rate_limit_fail_open" json:"-"`

	// AdmissionRequireState rejects usage-starting requests until a team
	// admission record has been projected into the region database.
	AdmissionRequireState bool `yaml:"admission_require_state" json:"admissionRequireState"`

	// Identity and Teams
	DefaultTeamName string `yaml:"default_team_name" json:"defaultTeamName"`

	// Built-in Authentication
	BuiltInAuth BuiltInAuthConfig `yaml:"built_in_auth" json:"builtInAuth"`

	// OIDC Providers
	OIDCProviders            []OIDCProviderConfig `yaml:"oidc_providers" json:"oidcProviders"`
	OIDCStateTTL             Duration             `yaml:"oidc_state_ttl" json:"oidcStateTTL"`
	OIDCStateCleanupInterval Duration             `yaml:"oidc_state_cleanup_interval" json:"oidcStateCleanupInterval"`

	// Base URL for OIDC callbacks
	BaseURL string `yaml:"base_url" json:"baseUrl"`

	// RegionID is the region identifier used by tenancy and routing contracts,
	// for example "aws-us-east-1".
	RegionID string `yaml:"region_id" json:"regionId"`

	// Public exposure host routing configuration.
	// Host format is fixed as: <exposureLabel>.<regionLabel>.<rootDomain>
	// These fields define the externally reachable gateway endpoints.
	PublicExposureEnabled bool   `yaml:"public_exposure_enabled" json:"-"`
	PublicRootDomain      string `yaml:"public_root_domain" json:"-"`
	// PublicRegionID is the region label used in exposure hosts,
	// for example "aws-us-east-1". It should match RegionID.
	PublicRegionID string `yaml:"public_region_id" json:"-"`

	// SSH endpoint advertised to users for sandbox detail responses.
	SSHEndpointHost string `yaml:"ssh_endpoint_host" json:"-"`
	SSHEndpointPort int    `yaml:"ssh_endpoint_port" json:"-"`
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
	InitUser *InitUserConfig `yaml:"init_user" json:"-"`
}

// InitUserConfig configures the initial admin user.
type InitUserConfig struct {
	Email        string `yaml:"email" json:"email"`
	Password     string `yaml:"password" json:"password"`
	Name         string `yaml:"name" json:"name"`
	HomeRegionID string `yaml:"home_region_id" json:"homeRegionId"`
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

	// TokenEndpointAuthMethod controls how the client authenticates to the token endpoint.
	// Supported values are "client_secret_basic" and "client_secret_post".
	// When omitted, the oauth2 library auto-detects the method.
	TokenEndpointAuthMethod string `yaml:"token_endpoint_auth_method" json:"tokenEndpointAuthMethod"`

	// Scopes are the OAuth scopes to request
	Scopes []string `yaml:"scopes" json:"scopes"`

	// DeviceAuthorizationEnabled enables device authorization flow for CLI login.
	DeviceAuthorizationEnabled bool `yaml:"device_authorization_enabled" json:"deviceAuthorizationEnabled"`

	// DeviceAuthorizationEndpoint overrides the discovery metadata endpoint when set.
	DeviceAuthorizationEndpoint string `yaml:"device_authorization_endpoint" json:"deviceAuthorizationEndpoint"`

	// DeviceClientID overrides the OAuth client ID used for device authorization.
	// Falls back to ClientID when omitted.
	DeviceClientID string `yaml:"device_client_id" json:"deviceClientId"`

	// DeviceClientSecret overrides the OAuth client secret used for device authorization.
	// Falls back to ClientSecret when omitted.
	DeviceClientSecret string `yaml:"device_client_secret" json:"-"`

	// AutoProvision automatically creates users on first login
	AutoProvision bool `yaml:"auto_provision" json:"autoProvision"`

	// TeamMapping configures automatic team assignment
	TeamMapping *TeamMappingConfig `yaml:"team_mapping" json:"teamMapping"`

	// ExternalAuthPortalURL, when set, redirects unauthenticated browser users to this URL
	// instead of initiating the OIDC flow directly. Use for deployments that host their own
	// login portal that handles OIDC initiation externally.
	ExternalAuthPortalURL string `yaml:"external_auth_portal_url" json:"externalAuthPortalUrl"`
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

// HasEnabledOIDCProviders returns true when at least one OIDC provider is enabled.
func HasEnabledOIDCProviders(providers []OIDCProviderConfig) bool {
	for _, p := range providers {
		if p.Enabled {
			return true
		}
	}
	return false
}
