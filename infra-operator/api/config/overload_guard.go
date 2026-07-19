// +kubebuilder:object:generate=true
package config

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultOverloadGuardRequestsPerSecond      = 100
	DefaultOverloadGuardBurst                  = 200
	DefaultOverloadGuardLocalRequestsPerSecond = 500
	DefaultOverloadGuardLocalBurst             = 1000
	DefaultOverloadGuardMaxInFlight            = 512
	DefaultOverloadGuardCleanupInterval        = 10 * time.Minute
	MaxOverloadGuardRequestsPerSecond          = 1_000_000
	MaxOverloadGuardBurst                      = 10_000_000
	MaxOverloadGuardMaxInFlight                = 1_000_000
)

// OverloadGuardConfig configures a platform safety valve. Unlike Team Quota,
// this policy is not a tenant entitlement and has no per-team override.
type OverloadGuardConfig struct {
	RequestsPerSecond      int             `yaml:"requests_per_second" json:"requestsPerSecond"`
	Burst                  int             `yaml:"burst" json:"burst"`
	LocalRequestsPerSecond int             `yaml:"local_requests_per_second" json:"localRequestsPerSecond"`
	LocalBurst             int             `yaml:"local_burst" json:"localBurst"`
	MaxInFlight            int             `yaml:"max_in_flight" json:"maxInFlight"`
	CleanupInterval        metav1.Duration `yaml:"cleanup_interval" json:"cleanupInterval"`
	RedisURL               string          `yaml:"redis_url" json:"-"`
	RedisKeyPrefix         string          `yaml:"redis_key_prefix" json:"-"`
	RedisTimeout           metav1.Duration `yaml:"redis_timeout" json:"-"`
}

// NormalizeOverloadGuardConfig applies safe defaults while retaining the
// operator-injected backend configuration.
func NormalizeOverloadGuardConfig(cfg OverloadGuardConfig) OverloadGuardConfig {
	if cfg.RequestsPerSecond == 0 {
		cfg.RequestsPerSecond = DefaultOverloadGuardRequestsPerSecond
	}
	if cfg.Burst == 0 {
		cfg.Burst = DefaultOverloadGuardBurst
	}
	if cfg.LocalRequestsPerSecond == 0 {
		cfg.LocalRequestsPerSecond = DefaultOverloadGuardLocalRequestsPerSecond
	}
	if cfg.LocalBurst == 0 {
		cfg.LocalBurst = DefaultOverloadGuardLocalBurst
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = DefaultOverloadGuardMaxInFlight
	}
	if cfg.CleanupInterval.Duration <= 0 {
		cfg.CleanupInterval = metav1.Duration{Duration: DefaultOverloadGuardCleanupInterval}
	}
	return cfg
}

// ValidateOverloadGuardConfig rejects unsafe direct-file configuration. The
// operator CRD enforces the same bounds before reconciliation.
func ValidateOverloadGuardConfig(cfg OverloadGuardConfig) error {
	switch {
	case cfg.RequestsPerSecond < 0 ||
		cfg.Burst < 0 ||
		cfg.LocalRequestsPerSecond < 0 ||
		cfg.LocalBurst < 0 ||
		cfg.MaxInFlight < 0:
		return fmt.Errorf("overload guard limits must be non-negative")
	case cfg.RequestsPerSecond > MaxOverloadGuardRequestsPerSecond:
		return fmt.Errorf(
			"overload guard requests_per_second must not exceed %d",
			MaxOverloadGuardRequestsPerSecond,
		)
	case cfg.LocalRequestsPerSecond > MaxOverloadGuardRequestsPerSecond:
		return fmt.Errorf(
			"overload guard local_requests_per_second must not exceed %d",
			MaxOverloadGuardRequestsPerSecond,
		)
	case cfg.Burst > MaxOverloadGuardBurst:
		return fmt.Errorf(
			"overload guard burst must not exceed %d",
			MaxOverloadGuardBurst,
		)
	case cfg.LocalBurst > MaxOverloadGuardBurst:
		return fmt.Errorf(
			"overload guard local_burst must not exceed %d",
			MaxOverloadGuardBurst,
		)
	case cfg.MaxInFlight > MaxOverloadGuardMaxInFlight:
		return fmt.Errorf(
			"overload guard max_in_flight must not exceed %d",
			MaxOverloadGuardMaxInFlight,
		)
	default:
		return nil
	}
}
