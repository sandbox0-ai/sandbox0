package service

import (
	"fmt"
	"net"
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
		mergedSpec = &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	mergedBindings := mergeCredentialBindings(req.TemplateBindings, req.RequestBindings)
	if err := validateTrafficRuleConfig(mergedSpec); err != nil {
		s.logger.Warn("Ignoring invalid traffic rules", zap.Error(err))
		mergedSpec = mergedSpec.DeepCopy()
		if mergedSpec.Egress != nil {
			mergedSpec.Egress.TrafficRules = nil
		}
	}
	if err := validateNetworkCredentialConfig(mergedSpec, mergedBindings); err != nil {
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
			merged.Egress.TrafficRules = mergeTrafficRules(merged.Egress.TrafficRules, request.Egress.TrafficRules)
			merged.Egress.CredentialRules = mergeEgressCredentialRules(merged.Egress.CredentialRules, request.Egress.CredentialRules)
		}
	}
	return merged
}

func mergeTrafficRules(base, override []v1alpha1.TrafficRule) []v1alpha1.TrafficRule {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	if len(base) == 0 {
		return append([]v1alpha1.TrafficRule(nil), override...)
	}
	if len(override) == 0 {
		return append([]v1alpha1.TrafficRule(nil), base...)
	}

	out := append([]v1alpha1.TrafficRule(nil), base...)
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

func validateTrafficRuleConfig(policy *v1alpha1.SandboxNetworkPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.Egress == nil {
		return nil
	}
	if len(policy.Egress.TrafficRules) > 0 && hasLegacyTrafficLists(policy.Egress) {
		return fmt.Errorf("egress trafficRules cannot be combined with legacy allowed*/denied* fields")
	}

	seenTrafficRuleNames := make(map[string]struct{}, len(policy.Egress.TrafficRules))
	for _, rule := range policy.Egress.TrafficRules {
		switch rule.Action {
		case v1alpha1.TrafficRuleActionAllow, v1alpha1.TrafficRuleActionDeny:
		default:
			return fmt.Errorf("egress traffic rule %q has unsupported action %q", rule.Name, rule.Action)
		}
		if len(rule.CIDRs) == 0 && len(rule.Domains) == 0 && len(rule.Ports) == 0 && len(rule.AppProtocols) == 0 {
			return fmt.Errorf("egress traffic rule %q must define at least one matcher", rule.Name)
		}
		if err := validateTrafficRule(rule); err != nil {
			return fmt.Errorf("egress traffic rule %q is invalid: %w", rule.Name, err)
		}
		if rule.Name == "" {
			continue
		}
		if _, ok := seenTrafficRuleNames[rule.Name]; ok {
			return fmt.Errorf("duplicate egress traffic rule name %q", rule.Name)
		}
		seenTrafficRuleNames[rule.Name] = struct{}{}
	}
	return nil
}

func validateNetworkCredentialConfig(policy *v1alpha1.SandboxNetworkPolicy, bindings []v1alpha1.CredentialBinding) error {
	if policy == nil {
		return nil
	}
	bindingRefs := make(map[string]struct{})
	bindingProjectionTypes := make(map[string]v1alpha1.CredentialProjectionType, len(bindings))
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
		bindingProjectionTypes[binding.Ref] = binding.Projection.Type
	}
	if policy.Egress == nil {
		return nil
	}

	seenNames := make(map[string]struct{}, len(policy.Egress.CredentialRules))
	for _, rule := range policy.Egress.CredentialRules {
		if rule.CredentialRef == "" {
			return fmt.Errorf("egress rule credentialRef is required")
		}
		if len(bindingRefs) > 0 {
			if _, ok := bindingRefs[rule.CredentialRef]; !ok {
				return fmt.Errorf("egress rule credentialRef %q not found", rule.CredentialRef)
			}
		}
		if rule.Protocol == v1alpha1.EgressAuthProtocolTLS {
			if rule.TLSMode != v1alpha1.EgressTLSModeTerminateReoriginate {
				return fmt.Errorf("egress rule %q with protocol tls requires tlsMode terminate-reoriginate", rule.Name)
			}
			if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; !ok || projectionType != v1alpha1.CredentialProjectionTypeTLSClientCertificate {
				return fmt.Errorf("egress rule %q with protocol tls requires tls_client_certificate projection on %q", rule.Name, rule.CredentialRef)
			}
		}
		if rule.Protocol == v1alpha1.EgressAuthProtocolSOCKS5 || rule.Protocol == v1alpha1.EgressAuthProtocolMQTT || rule.Protocol == v1alpha1.EgressAuthProtocolRedis {
			if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; !ok || projectionType != v1alpha1.CredentialProjectionTypeUsernamePassword {
				return fmt.Errorf("egress rule %q with protocol %s requires username_password projection on %q", rule.Name, rule.Protocol, rule.CredentialRef)
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

func hasLegacyTrafficLists(egress *v1alpha1.NetworkEgressPolicy) bool {
	if egress == nil {
		return false
	}
	return len(egress.AllowedCIDRs) > 0 ||
		len(egress.AllowedDomains) > 0 ||
		len(egress.DeniedCIDRs) > 0 ||
		len(egress.DeniedDomains) > 0 ||
		len(egress.AllowedPorts) > 0 ||
		len(egress.DeniedPorts) > 0
}

func validateTrafficRule(rule v1alpha1.TrafficRule) error {
	for _, cidr := range rule.CIDRs {
		value := strings.TrimSpace(cidr)
		if value == "" {
			continue
		}
		if _, _, err := net.ParseCIDR(value); err != nil {
			return fmt.Errorf("invalid CIDR %q", cidr)
		}
	}
	for _, domain := range rule.Domains {
		value := strings.ToLower(strings.TrimSpace(domain))
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "*.") && strings.TrimPrefix(value, "*.") == "" {
			return fmt.Errorf("invalid wildcard domain %q", domain)
		}
	}
	for _, port := range rule.Ports {
		if port.Port <= 0 || port.Port > 65535 {
			return fmt.Errorf("invalid port %d", port.Port)
		}
		if port.EndPort != nil {
			if *port.EndPort < port.Port || *port.EndPort > 65535 {
				return fmt.Errorf("invalid end port %d", *port.EndPort)
			}
		}
		proto := strings.ToLower(strings.TrimSpace(port.Protocol))
		if proto != "" && proto != "tcp" && proto != "udp" {
			return fmt.Errorf("unsupported protocol %q", port.Protocol)
		}
	}
	for _, appProtocol := range rule.AppProtocols {
		switch strings.ToLower(strings.TrimSpace(string(appProtocol))) {
		case "http", "tls", "ssh", "socks5", "mqtt", "redis", "amqp", "dns", "mongodb", "udp":
		default:
			return fmt.Errorf("unsupported app protocol %q", appProtocol)
		}
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
	case v1alpha1.CredentialProjectionTypeTLSClientCertificate:
		if projection.TLSClientCertificate == nil {
			return fmt.Errorf("credential binding projection.tlsClientCertificate is required for %q", ref)
		}
	case v1alpha1.CredentialProjectionTypeUsernamePassword:
		if projection.UsernamePassword == nil {
			return fmt.Errorf("credential binding projection.usernamePassword is required for %q", ref)
		}
	default:
		return fmt.Errorf("credential binding projection type %q is not supported for %q", projection.Type, ref)
	}
	return nil
}
