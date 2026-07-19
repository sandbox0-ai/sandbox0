package teamquota

import (
	"context"
	"fmt"
	"strings"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

// PoliciesFromConfig validates and converts region-owner runtime defaults.
func PoliciesFromConfig(configured []apiconfig.TeamQuotaPolicyConfig) ([]coreteamquota.Policy, error) {
	policies := make([]coreteamquota.Policy, 0, len(configured))
	for _, entry := range configured {
		policy := coreteamquota.Policy{
			Key:  coreteamquota.Key(entry.Key),
			Kind: coreteamquota.Kind(entry.Kind),
		}
		switch policy.Kind {
		case coreteamquota.KindCapacity, coreteamquota.KindConcurrency:
			if entry.Limit == nil {
				return nil, fmt.Errorf("%s policy %q requires limit", policy.Kind, entry.Key)
			}
			if entry.Tokens != nil || entry.Interval != nil || entry.Burst != nil {
				return nil, fmt.Errorf("%s policy %q must not set rate fields", policy.Kind, entry.Key)
			}
			policy.Limit = *entry.Limit
		case coreteamquota.KindRate:
			if entry.Limit != nil {
				return nil, fmt.Errorf("rate policy %q must not set limit", entry.Key)
			}
			if entry.Tokens == nil || entry.Interval == nil || entry.Burst == nil {
				return nil, fmt.Errorf("rate policy %q requires tokens, interval, and burst", entry.Key)
			}
			intervalMillis, err := coreteamquota.RateIntervalMillis(entry.Interval.Duration)
			if err != nil {
				return nil, fmt.Errorf("rate policy %q: %w", entry.Key, err)
			}
			policy.Tokens = *entry.Tokens
			policy.IntervalMillis = intervalMillis
			policy.Burst = *entry.Burst
		default:
			return nil, fmt.Errorf("unknown team quota kind %q", entry.Kind)
		}
		if err := policy.Validate(); err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

// ReplaceConfiguredDefaults atomically makes the owner configuration the
// complete region default policy set.
func ReplaceConfiguredDefaults(
	ctx context.Context,
	manager coreteamquota.PolicyManager,
	configured apiconfig.TeamQuotaConfig,
) error {
	if manager == nil {
		return fmt.Errorf("team quota policy manager is required")
	}
	policies, err := PoliciesFromConfig(configured.Defaults)
	if err != nil {
		return err
	}
	ownerEpoch, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(configured.DefaultsOwnerEpoch))
	if err != nil {
		return fmt.Errorf("parse Team Quota defaults owner epoch: %w", err)
	}
	return manager.ReplaceDefaultPoliciesVersioned(
		ctx,
		policies,
		coreteamquota.DefaultPolicyVersion{
			OwnerEpoch: ownerEpoch,
			Generation: configured.DefaultsGeneration,
		},
	)
}
