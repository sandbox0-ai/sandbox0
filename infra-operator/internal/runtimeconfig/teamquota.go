package runtimeconfig

import (
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultTeamQuotaPolicyCacheTTL     = 5 * time.Second
	defaultTeamQuotaLeaseTTL           = 15 * time.Second
	defaultTeamQuotaLeaseRenewInterval = 5 * time.Second
)

// ResolveTeamQuotaSpec returns an isolated runtime copy and fills an omitted
// owner bootstrap value from the create-once status identity. Consumer-only
// resources always provide StateID in spec; owner validation guarantees that
// an explicit recovery value matches status after initialization.
func ResolveTeamQuotaSpec(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.TeamQuotaConfig {
	if infra == nil || infra.Spec.TeamQuota == nil {
		return nil
	}
	spec := infra.Spec.TeamQuota.DeepCopy()
	if spec.StateID == "" && infra.Status.TeamQuota != nil {
		spec.StateID = infra.Status.TeamQuota.StateID
	}
	return spec
}

// ToTeamQuota builds the configuration consumed by the region entrypoint.
func ToTeamQuota(spec *infrav1alpha1.TeamQuotaConfig) apiconfig.TeamQuotaConfig {
	if spec == nil {
		return apiconfig.TeamQuotaConfig{
			PolicyOwner:            true,
			DistributedEnforcement: defaultTeamQuotaDistributedEnforcement(),
		}
	}

	out := apiconfig.TeamQuotaConfig{
		PolicyOwner:            true,
		Defaults:               make([]apiconfig.TeamQuotaPolicyConfig, 0, len(spec.Defaults)),
		DistributedEnforcement: ToTeamQuotaDistributedEnforcement(spec),
	}
	for _, policy := range spec.Defaults {
		out.Defaults = append(out.Defaults, apiconfig.TeamQuotaPolicyConfig{
			Key:      policy.Key,
			Kind:     policy.Kind,
			Limit:    cloneInt64(policy.Limit),
			Tokens:   cloneInt64(policy.Tokens),
			Interval: cloneDuration(policy.Interval),
			Burst:    cloneInt64(policy.Burst),
		})
	}
	return out
}

// SetTeamQuotaOwnerVersion injects the Kubernetes object incarnation and
// generation into policy-owner runtime config.
func SetTeamQuotaOwnerVersion(
	out *apiconfig.TeamQuotaConfig,
	owner metav1.Object,
) {
	if out == nil || owner == nil {
		return
	}
	out.DefaultsOwnerEpoch = owner.GetCreationTimestamp().UTC().Format(time.RFC3339Nano)
	out.DefaultsGeneration = owner.GetGeneration()
}

// ToTeamQuotaDistributedEnforcement builds the runtime-only configuration
// copied to distributed consumers. It intentionally excludes default policies.
func ToTeamQuotaDistributedEnforcement(spec *infrav1alpha1.TeamQuotaConfig) apiconfig.TeamQuotaDistributedEnforcementConfig {
	if spec == nil {
		return defaultTeamQuotaDistributedEnforcement()
	}
	out := apiconfig.TeamQuotaDistributedEnforcementConfig{
		StateID:        spec.StateID,
		PolicyCacheTTL: spec.DistributedEnforcement.PolicyCacheTTL,
		LeaseTTL:       spec.DistributedEnforcement.LeaseTTL,
		RenewInterval:  spec.DistributedEnforcement.RenewInterval,
	}
	if out.LeaseTTL.Duration == 0 {
		out.LeaseTTL.Duration = defaultTeamQuotaLeaseTTL
	}
	if out.RenewInterval.Duration == 0 {
		out.RenewInterval.Duration = defaultTeamQuotaLeaseRenewInterval
	}
	return out
}

func defaultTeamQuotaDistributedEnforcement() apiconfig.TeamQuotaDistributedEnforcementConfig {
	return apiconfig.TeamQuotaDistributedEnforcementConfig{
		PolicyCacheTTL: metav1.Duration{Duration: defaultTeamQuotaPolicyCacheTTL},
		LeaseTTL:       metav1.Duration{Duration: defaultTeamQuotaLeaseTTL},
		RenewInterval:  metav1.Duration{Duration: defaultTeamQuotaLeaseRenewInterval},
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneDuration(value *metav1.Duration) *metav1.Duration {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
