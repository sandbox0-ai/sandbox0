// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GlobalGatewayConfig holds all configuration for global-gateway.
type GlobalGatewayConfig struct {
	// +optional
	// +kubebuilder:default=8080
	HTTPPort int `yaml:"http_port" json:"httpPort"`
	// +optional
	// +kubebuilder:default="info"
	LogLevel string `yaml:"log_level" json:"logLevel"`

	DatabaseURL string `yaml:"database_url" json:"-"`
	// +optional
	// +kubebuilder:default=30
	DatabaseMaxConns int `yaml:"database_max_conns" json:"databaseMaxConns"`
	// +optional
	// +kubebuilder:default=8
	DatabaseMinConns int `yaml:"database_min_conns" json:"databaseMinConns"`
	// +optional
	// +kubebuilder:default="global_gateway"
	DatabaseSchema string `yaml:"database_schema" json:"databaseSchema"`

	// License file path used to unlock enterprise SSO features.
	// +optional
	LicenseFile string `yaml:"license_file" json:"-"`

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

	// +optional
	GatewayConfig `yaml:",inline" json:",inline"`
}

type globalGatewayConfigYAML struct {
	HTTPPort           int               `yaml:"http_port"`
	LogLevel           string            `yaml:"log_level"`
	DatabaseURL        string            `yaml:"database_url"`
	DatabaseMaxConns   int               `yaml:"database_max_conns"`
	DatabaseMinConns   int               `yaml:"database_min_conns"`
	DatabaseSchema     string            `yaml:"database_schema"`
	LicenseFile        string            `yaml:"license_file"`
	ShutdownTimeout    durationYAMLValue `yaml:"shutdown_timeout"`
	ServerReadTimeout  durationYAMLValue `yaml:"server_read_timeout"`
	ServerWriteTimeout durationYAMLValue `yaml:"server_write_timeout"`
	ServerIdleTimeout  durationYAMLValue `yaml:"server_idle_timeout"`

	JWTSecret             string                    `yaml:"jwt_secret"`
	JWTPrivateKeyPEM      string                    `yaml:"jwt_private_key_pem"`
	JWTPublicKeyPEM       string                    `yaml:"jwt_public_key_pem"`
	JWTPrivateKeyFile     string                    `yaml:"jwt_private_key_file"`
	JWTPublicKeyFile      string                    `yaml:"jwt_public_key_file"`
	JWTIssuer             string                    `yaml:"jwt_issuer"`
	JWTAccessTokenTTL     durationYAMLValue         `yaml:"jwt_access_token_ttl"`
	JWTRefreshTokenTTL    durationYAMLValue         `yaml:"jwt_refresh_token_ttl"`
	RedisURL              string                    `yaml:"redis_url"`
	RedisKeyPrefix        string                    `yaml:"redis_key_prefix"`
	RedisTimeout          durationYAMLValue         `yaml:"redis_timeout"`
	OverloadGuard         overloadGuardYAML         `yaml:"overload_guard"`
	IdentityResourceGuard identityResourceGuardYAML `yaml:"identity_resource_guard"`
	DefaultTeamName       string                    `yaml:"default_team_name"`
	BuiltInAuth           BuiltInAuthConfig         `yaml:"built_in_auth"`
	OIDCProviders         []OIDCProviderConfig      `yaml:"oidc_providers"`
	OIDCStateTTL          durationYAMLValue         `yaml:"oidc_state_ttl"`
	BaseURL               string                    `yaml:"base_url"`
	RegionID              string                    `yaml:"region_id"`
	PublicExposureEnabled bool                      `yaml:"public_exposure_enabled"`
	PublicRootDomain      string                    `yaml:"public_root_domain"`
	PublicRegionID        string                    `yaml:"public_region_id"`
}

type overloadGuardYAML struct {
	RequestsPerSecond      int               `yaml:"requests_per_second"`
	Burst                  int               `yaml:"burst"`
	LocalRequestsPerSecond int               `yaml:"local_requests_per_second"`
	LocalBurst             int               `yaml:"local_burst"`
	MaxInFlight            int               `yaml:"max_in_flight"`
	CleanupInterval        durationYAMLValue `yaml:"cleanup_interval"`
	RedisURL               string            `yaml:"redis_url"`
	RedisKeyPrefix         string            `yaml:"redis_key_prefix"`
	RedisTimeout           durationYAMLValue `yaml:"redis_timeout"`
}

type identityResourceGuardYAML struct {
	MaxTeamsOwnedPerUser          int               `yaml:"max_teams_owned_per_user"`
	MaxMembersPerTeam             int               `yaml:"max_members_per_team"`
	MaxTeamMembershipsPerUser     int               `yaml:"max_team_memberships_per_user"`
	MaxLinkedIdentitiesPerUser    int               `yaml:"max_linked_identities_per_user"`
	MaxActiveRefreshTokensPerUser int               `yaml:"max_active_refresh_tokens_per_user"`
	MaxActiveWebLoginCodesPerUser int               `yaml:"max_active_web_login_codes_per_user"`
	MaxActiveDeviceAuthSessions   int               `yaml:"max_active_device_auth_sessions"`
	MaxPendingOIDCStates          int               `yaml:"max_pending_oidc_states"`
	SessionCleanupInterval        durationYAMLValue `yaml:"session_cleanup_interval"`
	SessionCleanupBatchSize       int               `yaml:"session_cleanup_batch_size"`
}

type durationYAMLValue string

func (d *durationYAMLValue) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		*d = ""
		return nil
	}

	switch node.Kind {
	case yaml.ScalarNode:
		var raw string
		if err := node.Decode(&raw); err != nil {
			return err
		}
		*d = durationYAMLValue(raw)
		return nil
	case yaml.MappingNode:
		var wrapped struct {
			Duration string `yaml:"duration"`
		}
		if err := node.Decode(&wrapped); err != nil {
			return err
		}
		*d = durationYAMLValue(wrapped.Duration)
		return nil
	default:
		return fmt.Errorf("expected duration string or object with duration field")
	}
}

// LoadGlobalGatewayConfig returns the global-gateway configuration.
func LoadGlobalGatewayConfig() *GlobalGatewayConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadGlobalGatewayConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &GlobalGatewayConfig{}
	}
	return cfg
}

func loadGlobalGatewayConfig(path string) (*GlobalGatewayConfig, error) {
	cfg := &GlobalGatewayConfig{}
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	data = []byte(os.ExpandEnv(string(data)))

	var raw globalGatewayConfigYAML
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := applyGlobalGatewayYAML(cfg, raw); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyGlobalGatewayYAML(cfg *GlobalGatewayConfig, raw globalGatewayConfigYAML) error {
	if cfg == nil {
		return nil
	}

	cfg.HTTPPort = raw.HTTPPort
	cfg.LogLevel = raw.LogLevel
	cfg.DatabaseURL = raw.DatabaseURL
	cfg.DatabaseMaxConns = raw.DatabaseMaxConns
	cfg.DatabaseMinConns = raw.DatabaseMinConns
	cfg.DatabaseSchema = raw.DatabaseSchema
	cfg.LicenseFile = raw.LicenseFile
	cfg.JWTSecret = raw.JWTSecret
	cfg.JWTPrivateKeyPEM = raw.JWTPrivateKeyPEM
	cfg.JWTPublicKeyPEM = raw.JWTPublicKeyPEM
	cfg.JWTPrivateKeyFile = raw.JWTPrivateKeyFile
	cfg.JWTPublicKeyFile = raw.JWTPublicKeyFile
	cfg.JWTIssuer = raw.JWTIssuer
	cfg.RedisURL = raw.RedisURL
	cfg.RedisKeyPrefix = raw.RedisKeyPrefix
	cfg.OverloadGuard.RequestsPerSecond = raw.OverloadGuard.RequestsPerSecond
	cfg.OverloadGuard.Burst = raw.OverloadGuard.Burst
	cfg.OverloadGuard.LocalRequestsPerSecond = raw.OverloadGuard.LocalRequestsPerSecond
	cfg.OverloadGuard.LocalBurst = raw.OverloadGuard.LocalBurst
	cfg.OverloadGuard.MaxInFlight = raw.OverloadGuard.MaxInFlight
	cfg.OverloadGuard.RedisURL = raw.OverloadGuard.RedisURL
	cfg.OverloadGuard.RedisKeyPrefix = raw.OverloadGuard.RedisKeyPrefix
	cfg.IdentityResourceGuard.MaxTeamsOwnedPerUser = raw.IdentityResourceGuard.MaxTeamsOwnedPerUser
	cfg.IdentityResourceGuard.MaxMembersPerTeam = raw.IdentityResourceGuard.MaxMembersPerTeam
	cfg.IdentityResourceGuard.MaxTeamMembershipsPerUser = raw.IdentityResourceGuard.MaxTeamMembershipsPerUser
	cfg.IdentityResourceGuard.MaxLinkedIdentitiesPerUser = raw.IdentityResourceGuard.MaxLinkedIdentitiesPerUser
	cfg.IdentityResourceGuard.MaxActiveRefreshTokensPerUser = raw.IdentityResourceGuard.MaxActiveRefreshTokensPerUser
	cfg.IdentityResourceGuard.MaxActiveWebLoginCodesPerUser = raw.IdentityResourceGuard.MaxActiveWebLoginCodesPerUser
	cfg.IdentityResourceGuard.MaxActiveDeviceAuthSessions = raw.IdentityResourceGuard.MaxActiveDeviceAuthSessions
	cfg.IdentityResourceGuard.MaxPendingOIDCStates = raw.IdentityResourceGuard.MaxPendingOIDCStates
	cfg.IdentityResourceGuard.SessionCleanupBatchSize = raw.IdentityResourceGuard.SessionCleanupBatchSize
	cfg.DefaultTeamName = raw.DefaultTeamName
	cfg.BuiltInAuth = raw.BuiltInAuth
	cfg.OIDCProviders = raw.OIDCProviders
	cfg.BaseURL = raw.BaseURL
	cfg.RegionID = raw.RegionID
	cfg.PublicExposureEnabled = raw.PublicExposureEnabled
	cfg.PublicRootDomain = raw.PublicRootDomain
	cfg.PublicRegionID = raw.PublicRegionID

	if err := applyOptionalDuration(&cfg.ShutdownTimeout, raw.ShutdownTimeout, "shutdown_timeout"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.ServerReadTimeout, raw.ServerReadTimeout, "server_read_timeout"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.ServerWriteTimeout, raw.ServerWriteTimeout, "server_write_timeout"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.ServerIdleTimeout, raw.ServerIdleTimeout, "server_idle_timeout"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.JWTAccessTokenTTL, raw.JWTAccessTokenTTL, "jwt_access_token_ttl"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.JWTRefreshTokenTTL, raw.JWTRefreshTokenTTL, "jwt_refresh_token_ttl"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.RedisTimeout, raw.RedisTimeout, "redis_timeout"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.OverloadGuard.CleanupInterval, raw.OverloadGuard.CleanupInterval, "overload_guard.cleanup_interval"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.OverloadGuard.RedisTimeout, raw.OverloadGuard.RedisTimeout, "overload_guard.redis_timeout"); err != nil {
		return err
	}
	if err := applyOptionalDuration(
		&cfg.IdentityResourceGuard.SessionCleanupInterval,
		raw.IdentityResourceGuard.SessionCleanupInterval,
		"identity_resource_guard.session_cleanup_interval",
	); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.OIDCStateTTL, raw.OIDCStateTTL, "oidc_state_ttl"); err != nil {
		return err
	}

	return nil
}

func applyOptionalDuration(dst *metav1.Duration, raw durationYAMLValue, field string) error {
	if dst == nil || raw == "" {
		return nil
	}

	parsed, err := time.ParseDuration(string(raw))
	if err != nil {
		return fmt.Errorf("parse %s: %w", field, err)
	}
	*dst = metav1.Duration{Duration: parsed}
	return nil
}
