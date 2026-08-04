package networkpolicy

import (
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
)

// NetworkPolicyService builds network policy specs for pod annotations.
type NetworkPolicyService struct {
	logger *zap.Logger
}

// NewNetworkPolicyService creates a new NetworkPolicyService.
func NewNetworkPolicyService(logger *zap.Logger) *NetworkPolicyService {
	return &NetworkPolicyService{
		logger: logger,
	}
}

// BuildNetworkPolicyRequest contains the request to build a network policy.
type BuildNetworkPolicyRequest struct {
	SandboxID        string
	TeamID           string
	TemplateSpec     *v1alpha1.SandboxNetworkPolicy // From template.
	RequestSpec      *v1alpha1.SandboxNetworkPolicy // From claim/update request.
	TemplateBindings []v1alpha1.CredentialBinding   // From template.
	RequestBindings  []v1alpha1.CredentialBinding   // From claim/update request.
}

// BuildNetworkPolicyResult contains the split effective runtime state.
type BuildNetworkPolicyResult struct {
	PolicySpec         *v1alpha1.NetworkPolicySpec
	CredentialBindings []v1alpha1.CredentialBinding
}

// BuildNetworkPolicyState builds the state used by the ctld network runtime and manager runtime resolver.
func (s *NetworkPolicyService) BuildNetworkPolicyState(req *BuildNetworkPolicyRequest) *BuildNetworkPolicyResult {
	mergedSpec := s.mergeNetworkPolicies(req.TemplateSpec, req.RequestSpec)
	if mergedSpec == nil {
		mergedSpec = &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	mergedBindings := mergeByKey(req.TemplateBindings, req.RequestBindings, func(binding v1alpha1.CredentialBinding) string {
		return binding.Ref
	})
	if err := v1alpha1.ValidateSandboxNetworkTrafficPolicy(mergedSpec); err != nil {
		s.logger.Warn("Ignoring invalid traffic or protocol rules", zap.Error(err))
		mergedSpec = mergedSpec.DeepCopy()
		if mergedSpec.Egress != nil {
			mergedSpec.Egress.TrafficRules = nil
			mergedSpec.Egress.ProtocolRules = nil
		}
	}
	if err := v1alpha1.ValidateSandboxNetworkCredentialPolicy(mergedSpec, mergedBindings); err != nil {
		s.logger.Warn("Ignoring invalid credential bindings and rules", zap.Error(err))
		mergedSpec = mergedSpec.DeepCopy()
		if mergedSpec.Egress != nil {
			mergedSpec.Egress.CredentialRules = nil
		}
		mergedBindings = nil
	}

	return &BuildNetworkPolicyResult{
		PolicySpec: &v1alpha1.NetworkPolicySpec{
			Version:   "v1",
			SandboxID: req.SandboxID,
			TeamID:    req.TeamID,
			Mode:      mergedSpec.Mode,
			Egress:    v1alpha1.BuildEgressSpec(mergedSpec),
		},
		CredentialBindings: mergedBindings,
	}
}

// ValidateNetworkPolicyRequest validates the merged policy that would be produced for a request.
func (s *NetworkPolicyService) ValidateNetworkPolicyRequest(req *BuildNetworkPolicyRequest) error {
	if req == nil {
		return nil
	}
	mergedSpec := s.mergeNetworkPolicies(req.TemplateSpec, req.RequestSpec)
	if mergedSpec == nil {
		mergedSpec = &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	mergedBindings := mergeByKey(req.TemplateBindings, req.RequestBindings, func(binding v1alpha1.CredentialBinding) string {
		return binding.Ref
	})
	return v1alpha1.ValidateSandboxNetworkPolicy(mergedSpec, mergedBindings)
}

// mergeNetworkPolicies merges template and request network policies.
// Request values override template values.
func (s *NetworkPolicyService) mergeNetworkPolicies(
	template *v1alpha1.SandboxNetworkPolicy,
	request *v1alpha1.SandboxNetworkPolicy,
) *v1alpha1.SandboxNetworkPolicy {
	if template == nil && request == nil {
		return &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeAllowAll,
		}
	}
	if template == nil {
		return request
	}
	if request == nil {
		return template
	}

	merged := template.DeepCopy()
	if request.Mode != "" {
		merged.Mode = request.Mode
	}
	if request.Egress != nil {
		if merged.Egress == nil {
			merged.Egress = request.Egress
		} else {
			merged.Egress.AllowedCIDRs = append(merged.Egress.AllowedCIDRs, request.Egress.AllowedCIDRs...)
			merged.Egress.AllowedDomains = append(merged.Egress.AllowedDomains, request.Egress.AllowedDomains...)
			merged.Egress.DeniedCIDRs = append(merged.Egress.DeniedCIDRs, request.Egress.DeniedCIDRs...)
			merged.Egress.DeniedDomains = append(merged.Egress.DeniedDomains, request.Egress.DeniedDomains...)
			merged.Egress.AllowedPorts = append(merged.Egress.AllowedPorts, request.Egress.AllowedPorts...)
			merged.Egress.DeniedPorts = append(merged.Egress.DeniedPorts, request.Egress.DeniedPorts...)
			merged.Egress.TrafficRules = mergeByKey(merged.Egress.TrafficRules, request.Egress.TrafficRules, func(rule v1alpha1.TrafficRule) string {
				return rule.Name
			})
			merged.Egress.ProtocolRules = mergeByKey(merged.Egress.ProtocolRules, request.Egress.ProtocolRules, func(rule v1alpha1.ProtocolRule) string {
				return rule.Name
			})
			merged.Egress.CredentialRules = mergeByKey(merged.Egress.CredentialRules, request.Egress.CredentialRules, func(rule v1alpha1.EgressCredentialRule) string {
				return rule.Name
			})
			if request.Egress.Proxy != nil {
				merged.Egress.Proxy = CloneEgressProxyPolicy(request.Egress.Proxy)
			}
		}
	}
	return merged
}

// CloneEgressProxyPolicy copies the optional proxy policy without sharing mutable state.
func CloneEgressProxyPolicy(in *v1alpha1.EgressProxyPolicy) *v1alpha1.EgressProxyPolicy {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func mergeByKey[T any](base, override []T, key func(T) string) []T {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	if len(base) == 0 {
		return append([]T(nil), override...)
	}
	if len(override) == 0 {
		return append([]T(nil), base...)
	}

	out := append([]T(nil), base...)
	indexByKey := make(map[string]int, len(base))
	for i, item := range out {
		itemKey := key(item)
		if itemKey == "" {
			continue
		}
		indexByKey[itemKey] = i
	}
	for _, item := range override {
		itemKey := key(item)
		if itemKey != "" {
			if idx, ok := indexByKey[itemKey]; ok {
				out[idx] = item
				continue
			}
			indexByKey[itemKey] = len(out)
		}
		out = append(out, item)
	}
	return out
}
