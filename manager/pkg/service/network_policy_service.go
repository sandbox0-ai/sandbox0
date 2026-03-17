package service

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
)

// NetworkPolicyService builds network policy specs for pod annotations
type NetworkPolicyService struct {
	logger *zap.Logger
}

// NewNetworkPolicyService creates a new NetworkPolicyService
func NewNetworkPolicyService(logger *zap.Logger) *NetworkPolicyService {
	return &NetworkPolicyService{
		logger: logger,
	}
}

// BuildNetworkPolicyRequest contains the request to build a network policy
type BuildNetworkPolicyRequest struct {
	SandboxID    string
	TeamID       string
	TemplateSpec *v1alpha1.TplSandboxNetworkPolicy // From template
	RequestSpec  *v1alpha1.TplSandboxNetworkPolicy // From claim request (overrides template)
}

// BuildNetworkPolicyAnnotation builds the network policy annotation JSON
func (s *NetworkPolicyService) BuildNetworkPolicyAnnotation(req *BuildNetworkPolicyRequest) (string, error) {
	spec := s.BuildNetworkPolicySpec(req)
	return v1alpha1.NetworkPolicyToAnnotation(spec)
}

// BuildNetworkPolicySpec builds the network policy spec without serialization.
func (s *NetworkPolicyService) BuildNetworkPolicySpec(req *BuildNetworkPolicyRequest) *v1alpha1.NetworkPolicySpec {
	// Merge template and request specs
	mergedSpec := s.mergeNetworkPolicies(req.TemplateSpec, req.RequestSpec)
	if mergedSpec != nil && mergedSpec.Egress != nil {
		if err := validateEgressAuthRules(mergedSpec.Egress.AuthRules); err != nil {
			s.logger.Warn("Ignoring invalid egress auth rules", zap.Error(err))
			mergedSpec = mergedSpec.DeepCopy()
			mergedSpec.Egress.AuthRules = nil
		}
	}

	// Build the policy spec
	return &v1alpha1.NetworkPolicySpec{
		Version:   "v1",
		SandboxID: req.SandboxID,
		TeamID:    req.TeamID,
		Mode:      mergedSpec.Mode,
		Egress:    v1alpha1.BuildEgressSpec(mergedSpec),
	}
}

// mergeNetworkPolicies merges template and request network policies
// Request values override template values
func (s *NetworkPolicyService) mergeNetworkPolicies(
	template *v1alpha1.TplSandboxNetworkPolicy,
	request *v1alpha1.TplSandboxNetworkPolicy,
) *v1alpha1.TplSandboxNetworkPolicy {
	if template == nil && request == nil {
		return &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeAllowAll, // Default to allow all
		}
	}

	if template == nil {
		return request
	}

	if request == nil {
		return template
	}

	// Merge: request overrides template
	merged := template.DeepCopy()

	// Mode from request takes precedence
	if request.Mode != "" {
		merged.Mode = request.Mode
	}

	// Merge egress
	if request.Egress != nil {
		if merged.Egress == nil {
			merged.Egress = request.Egress
		} else {
			// Append allowed CIDRs and domains
			merged.Egress.AllowedCIDRs = append(merged.Egress.AllowedCIDRs, request.Egress.AllowedCIDRs...)
			merged.Egress.AllowedDomains = append(merged.Egress.AllowedDomains, request.Egress.AllowedDomains...)
			merged.Egress.DeniedCIDRs = append(merged.Egress.DeniedCIDRs, request.Egress.DeniedCIDRs...)
			merged.Egress.DeniedDomains = append(merged.Egress.DeniedDomains, request.Egress.DeniedDomains...)
			merged.Egress.AllowedPorts = append(merged.Egress.AllowedPorts, request.Egress.AllowedPorts...)
			merged.Egress.DeniedPorts = append(merged.Egress.DeniedPorts, request.Egress.DeniedPorts...)
			merged.Egress.AuthRules = mergeEgressAuthRules(merged.Egress.AuthRules, request.Egress.AuthRules)
		}
	}

	return merged
}

func mergeEgressAuthRules(base, override []v1alpha1.EgressAuthRule) []v1alpha1.EgressAuthRule {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	if len(base) == 0 {
		return append([]v1alpha1.EgressAuthRule(nil), override...)
	}
	if len(override) == 0 {
		return append([]v1alpha1.EgressAuthRule(nil), base...)
	}

	out := append([]v1alpha1.EgressAuthRule(nil), base...)
	indexByName := make(map[string]int, len(base))
	for i, rule := range out {
		if rule.Name == "" {
			continue
		}
		indexByName[rule.Name] = i
	}

	for _, rule := range override {
		if rule.Name != "" {
			if idx, ok := indexByName[rule.Name]; ok {
				out[idx] = rule
				continue
			}
			indexByName[rule.Name] = len(out)
		}
		out = append(out, rule)
	}

	return out
}

func validateEgressAuthRules(rules []v1alpha1.EgressAuthRule) error {
	seenNames := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		if rule.AuthRef == "" {
			return fmt.Errorf("auth rule authRef is required")
		}
		if rule.Name == "" {
			continue
		}
		if _, ok := seenNames[rule.Name]; ok {
			return fmt.Errorf("duplicate auth rule name %q", rule.Name)
		}
		seenNames[rule.Name] = struct{}{}
	}
	return nil
}
