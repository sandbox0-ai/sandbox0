package testutil

import "github.com/sandbox0-ai/sandbox0/pkg/teamquota"

// CompleteDefaultPolicies returns a valid, permissive default for every known
// key, with optional per-key replacements.
func CompleteDefaultPolicies(overrides ...teamquota.Policy) []teamquota.Policy {
	byKey := make(map[teamquota.Key]teamquota.Policy, len(teamquota.Keys()))
	for _, key := range teamquota.Keys() {
		kind, _ := teamquota.KindForKey(key)
		policy := teamquota.Policy{Key: key, Kind: kind}
		switch kind {
		case teamquota.KindCapacity:
			policy.Limit = 1 << 60
		case teamquota.KindConcurrency:
			policy.Limit = 1 << 52
		default:
			policy.Tokens = 1000
			policy.IntervalMillis = 1000
			policy.Burst = 2000
		}
		byKey[key] = policy
	}
	for _, policy := range overrides {
		byKey[policy.Key] = policy
	}
	policies := make([]teamquota.Policy, 0, len(byKey))
	for _, key := range teamquota.Keys() {
		policies = append(policies, byKey[key])
	}
	return policies
}
