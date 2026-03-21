// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GlobalDirectoryConfig holds all configuration for global-directory.
type GlobalDirectoryConfig struct {
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
	// +kubebuilder:default="gd"
	DatabaseSchema string `yaml:"database_schema" json:"databaseSchema"`

	// +optional
	// +kubebuilder:default="5m"
	RegionTokenTTL metav1.Duration `yaml:"region_token_ttl" json:"regionTokenTTL"`

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

type globalDirectoryConfigYAML struct {
	HTTPPort           int    `yaml:"http_port"`
	LogLevel           string `yaml:"log_level"`
	DatabaseURL        string `yaml:"database_url"`
	DatabaseMaxConns   int    `yaml:"database_max_conns"`
	DatabaseMinConns   int    `yaml:"database_min_conns"`
	DatabaseSchema     string `yaml:"database_schema"`
	RegionTokenTTL     string `yaml:"region_token_ttl"`
	LicenseFile        string `yaml:"license_file"`
	ShutdownTimeout    string `yaml:"shutdown_timeout"`
	ServerReadTimeout  string `yaml:"server_read_timeout"`
	ServerWriteTimeout string `yaml:"server_write_timeout"`
	ServerIdleTimeout  string `yaml:"server_idle_timeout"`

	JWTSecret                string               `yaml:"jwt_secret"`
	JWTIssuer                string               `yaml:"jwt_issuer"`
	JWTAccessTokenTTL        string               `yaml:"jwt_access_token_ttl"`
	JWTRefreshTokenTTL       string               `yaml:"jwt_refresh_token_ttl"`
	RateLimitRPS             int                  `yaml:"rate_limit_rps"`
	RateLimitBurst           int                  `yaml:"rate_limit_burst"`
	RateLimitCleanupInterval string               `yaml:"rate_limit_cleanup_interval"`
	DefaultTeamName          string               `yaml:"default_team_name"`
	BuiltInAuth              BuiltInAuthConfig    `yaml:"built_in_auth"`
	OIDCProviders            []OIDCProviderConfig `yaml:"oidc_providers"`
	OIDCStateTTL             string               `yaml:"oidc_state_ttl"`
	OIDCStateCleanupInterval string               `yaml:"oidc_state_cleanup_interval"`
	BaseURL                  string               `yaml:"base_url"`
	RegionID                 string               `yaml:"region_id"`
	PublicExposureEnabled    bool                 `yaml:"public_exposure_enabled"`
	PublicRootDomain         string               `yaml:"public_root_domain"`
	PublicRegionID           string               `yaml:"public_region_id"`
}

// LoadGlobalDirectoryConfig returns the global-directory configuration.
func LoadGlobalDirectoryConfig() *GlobalDirectoryConfig {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/config/config.yaml"
	}

	cfg, err := loadGlobalDirectoryConfig(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config from %s: %v, using empty config\n", path, err)
		cfg = &GlobalDirectoryConfig{}
	}
	return cfg
}

func loadGlobalDirectoryConfig(path string) (*GlobalDirectoryConfig, error) {
	cfg := &GlobalDirectoryConfig{}
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	data = []byte(os.ExpandEnv(string(data)))

	var raw globalDirectoryConfigYAML
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := applyGlobalDirectoryYAML(cfg, raw); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyGlobalDirectoryYAML(cfg *GlobalDirectoryConfig, raw globalDirectoryConfigYAML) error {
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
	cfg.JWTIssuer = raw.JWTIssuer
	cfg.RateLimitRPS = raw.RateLimitRPS
	cfg.RateLimitBurst = raw.RateLimitBurst
	cfg.DefaultTeamName = raw.DefaultTeamName
	cfg.BuiltInAuth = raw.BuiltInAuth
	cfg.OIDCProviders = raw.OIDCProviders
	cfg.BaseURL = raw.BaseURL
	cfg.RegionID = raw.RegionID
	cfg.PublicExposureEnabled = raw.PublicExposureEnabled
	cfg.PublicRootDomain = raw.PublicRootDomain
	cfg.PublicRegionID = raw.PublicRegionID

	if err := applyOptionalDuration(&cfg.RegionTokenTTL, raw.RegionTokenTTL, "region_token_ttl"); err != nil {
		return err
	}
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
	if err := applyOptionalDuration(&cfg.RateLimitCleanupInterval, raw.RateLimitCleanupInterval, "rate_limit_cleanup_interval"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.OIDCStateTTL, raw.OIDCStateTTL, "oidc_state_ttl"); err != nil {
		return err
	}
	if err := applyOptionalDuration(&cfg.OIDCStateCleanupInterval, raw.OIDCStateCleanupInterval, "oidc_state_cleanup_interval"); err != nil {
		return err
	}

	return nil
}

func applyOptionalDuration(dst *metav1.Duration, raw string, field string) error {
	if dst == nil || raw == "" {
		return nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", field, err)
	}
	*dst = metav1.Duration{Duration: parsed}
	return nil
}
