package service

import (
	"fmt"
	"strings"
	"time"

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
	TemplateSpec     *v1alpha1.TplSandboxNetworkPolicy // From template.
	RequestSpec      *v1alpha1.TplSandboxNetworkPolicy // From claim/update request.
	TemplateBindings []v1alpha1.CredentialBinding      // From template.
	RequestBindings  []v1alpha1.CredentialBinding      // From claim/update request.
}

// BuildNetworkPolicyResult contains the split effective runtime state.
type BuildNetworkPolicyResult struct {
	PolicySpec         *v1alpha1.NetworkPolicySpec
	CredentialBindings []v1alpha1.CredentialBinding
}

// BuildNetworkPolicyAnnotation builds the network policy annotation JSON.
func (s *NetworkPolicyService) BuildNetworkPolicyAnnotation(req *BuildNetworkPolicyRequest) (string, error) {
	result := s.BuildNetworkPolicyState(req)
	return v1alpha1.NetworkPolicyToAnnotation(result.PolicySpec)
}

// BuildNetworkPolicySpec builds only the netd-consumed policy spec.
func (s *NetworkPolicyService) BuildNetworkPolicySpec(req *BuildNetworkPolicyRequest) *v1alpha1.NetworkPolicySpec {
	return s.BuildNetworkPolicyState(req).PolicySpec
}

// BuildNetworkPolicyState builds the split runtime state used by netd and the manager runtime resolver.
func (s *NetworkPolicyService) BuildNetworkPolicyState(req *BuildNetworkPolicyRequest) *BuildNetworkPolicyResult {
	mergedSpec := s.mergeNetworkPolicies(req.TemplateSpec, req.RequestSpec)
	if mergedSpec == nil {
		mergedSpec = &v1alpha1.TplSandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	mergedBindings := mergeCredentialBindings(req.TemplateBindings, req.RequestBindings)
	if err := validateNetworkCredentialConfig(mergedSpec, mergedBindings); err != nil {
		s.logger.Warn("Ignoring invalid credential bindings and rules", zap.Error(err))
		mergedSpec = mergedSpec.DeepCopy()
		if mergedSpec.Egress != nil {
			mergedSpec.Egress.Rules = nil
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

// mergeNetworkPolicies merges template and request network policies.
// Request values override template values.
func (s *NetworkPolicyService) mergeNetworkPolicies(
	template *v1alpha1.TplSandboxNetworkPolicy,
	request *v1alpha1.TplSandboxNetworkPolicy,
) *v1alpha1.TplSandboxNetworkPolicy {
	if template == nil && request == nil {
		return &v1alpha1.TplSandboxNetworkPolicy{
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
			merged.Egress.Rules = mergeEgressCredentialRules(merged.Egress.Rules, request.Egress.Rules)
		}
	}
	return merged
}

func mergeEgressCredentialRules(base, override []v1alpha1.EgressCredentialRule) []v1alpha1.EgressCredentialRule {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	if len(base) == 0 {
		return append([]v1alpha1.EgressCredentialRule(nil), override...)
	}
	if len(override) == 0 {
		return append([]v1alpha1.EgressCredentialRule(nil), base...)
	}

	out := append([]v1alpha1.EgressCredentialRule(nil), base...)
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

func mergeCredentialBindings(base, override []v1alpha1.CredentialBinding) []v1alpha1.CredentialBinding {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	if len(base) == 0 {
		return append([]v1alpha1.CredentialBinding(nil), override...)
	}
	if len(override) == 0 {
		return append([]v1alpha1.CredentialBinding(nil), base...)
	}

	out := append([]v1alpha1.CredentialBinding(nil), base...)
	indexByRef := make(map[string]int, len(base))
	for i, binding := range out {
		if binding.Ref == "" {
			continue
		}
		indexByRef[binding.Ref] = i
	}
	for _, binding := range override {
		if binding.Ref != "" {
			if idx, ok := indexByRef[binding.Ref]; ok {
				out[idx] = binding
				continue
			}
			indexByRef[binding.Ref] = len(out)
		}
		out = append(out, binding)
	}
	return out
}

func validateNetworkCredentialConfig(policy *v1alpha1.TplSandboxNetworkPolicy, bindings []v1alpha1.CredentialBinding) error {
	if policy == nil {
		return nil
	}

	bindingRefs := make(map[string]struct{})
	for _, binding := range bindings {
		if binding.Ref == "" {
			return fmt.Errorf("credential binding ref is required")
		}
		if strings.TrimSpace(binding.SourceRef) == "" {
			return fmt.Errorf("credential binding sourceRef is required for %q", binding.Ref)
		}
		if err := validateProjection(binding.Ref, binding.Projection); err != nil {
			return err
		}
		if binding.CachePolicy != nil {
			if ttl := strings.TrimSpace(binding.CachePolicy.TTL); ttl != "" {
				if _, err := time.ParseDuration(ttl); err != nil {
					return fmt.Errorf("credential binding cachePolicy ttl is invalid for %q: %w", binding.Ref, err)
				}
			}
		}
		if _, ok := bindingRefs[binding.Ref]; ok {
			return fmt.Errorf("duplicate credential binding ref %q", binding.Ref)
		}
		bindingRefs[binding.Ref] = struct{}{}
	}
	if policy.Egress == nil {
		return nil
	}

	seenNames := make(map[string]struct{}, len(policy.Egress.Rules))
	for _, rule := range policy.Egress.Rules {
		if rule.CredentialRef == "" {
			return fmt.Errorf("egress rule credentialRef is required")
		}
		if len(bindingRefs) > 0 {
			if _, ok := bindingRefs[rule.CredentialRef]; !ok {
				return fmt.Errorf("egress rule credentialRef %q not found", rule.CredentialRef)
			}
		}
		if rule.Name == "" {
			continue
		}
		if _, ok := seenNames[rule.Name]; ok {
			return fmt.Errorf("duplicate egress rule name %q", rule.Name)
		}
		seenNames[rule.Name] = struct{}{}
	}
	return nil
}

func validateProjection(ref string, projection v1alpha1.ProjectionSpec) error {
	switch projection.Type {
	case v1alpha1.CredentialProjectionTypeHTTPHeaders:
		if projection.HTTPHeaders == nil {
			return fmt.Errorf("credential binding projection.httpHeaders is required for %q", ref)
		}
		for _, header := range projection.HTTPHeaders.Headers {
			if strings.TrimSpace(header.Name) == "" {
				return fmt.Errorf("credential binding projected header name is required for %q", ref)
			}
			if strings.TrimSpace(header.ValueTemplate) == "" {
				return fmt.Errorf("credential binding projected header valueTemplate is required for %q", ref)
			}
		}
	default:
		return fmt.Errorf("credential binding projection type %q is not supported for %q", projection.Type, ref)
	}
	return nil
}
