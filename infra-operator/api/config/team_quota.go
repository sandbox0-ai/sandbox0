// +kubebuilder:object:generate=true
package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TeamQuotaConfig is the region-entrypoint runtime configuration for team
// quota defaults and enforcement. Defaults must not be copied into data-plane
// consumer configs.
type TeamQuotaConfig struct {
	// PolicyOwner marks the region entrypoint that reconciles the complete
	// default policy set. It is runtime-only, explicit, and defaults to false.
	PolicyOwner            bool                                  `yaml:"policy_owner" json:"-"`
	DefaultsOwnerEpoch     string                                `yaml:"defaults_owner_epoch,omitempty" json:"-"`
	DefaultsGeneration     int64                                 `yaml:"defaults_generation,omitempty" json:"-"`
	Defaults               []TeamQuotaPolicyConfig               `yaml:"defaults,omitempty" json:"defaults,omitempty"`
	DistributedEnforcement TeamQuotaDistributedEnforcementConfig `yaml:"distributed_enforcement" json:"distributedEnforcement"`
}

// TeamQuotaPolicyConfig defines one default policy reconciled by the region
// entrypoint.
type TeamQuotaPolicyConfig struct {
	Key      string           `yaml:"key" json:"key"`
	Kind     string           `yaml:"kind" json:"kind"`
	Limit    *int64           `yaml:"limit,omitempty" json:"limit,omitempty"`
	Tokens   *int64           `yaml:"tokens,omitempty" json:"tokens,omitempty"`
	Interval *metav1.Duration `yaml:"interval,omitempty" json:"interval,omitempty"`
	Burst    *int64           `yaml:"burst,omitempty" json:"burst,omitempty"`
}

// TeamQuotaDistributedEnforcementConfig contains runtime-only settings shared
// by rate and concurrency enforcers. Redis settings are injected from the
// region's spec.redis.
type TeamQuotaDistributedEnforcementConfig struct {
	// StateID is the trusted, immutable region Team Quota state-plane UUID.
	// Infra-operator resolves it from owner status or consumer spec before
	// writing runtime configuration.
	StateID        string          `yaml:"state_id" json:"-"`
	PolicyCacheTTL metav1.Duration `yaml:"policy_cache_ttl" json:"policyCacheTtl"`
	LeaseTTL       metav1.Duration `yaml:"lease_ttl" json:"leaseTtl"`
	RenewInterval  metav1.Duration `yaml:"renew_interval" json:"renewInterval"`
	RedisURL       string          `yaml:"redis_url" json:"-"`
	RedisKeyPrefix string          `yaml:"redis_key_prefix" json:"-"`
	RedisTimeout   metav1.Duration `yaml:"redis_timeout" json:"-"`
}
