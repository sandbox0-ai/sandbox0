package service

import (
	"fmt"
	"net"
	"net/http"
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
		s.logger.Warn("Ignoring invalid traffic or protocol rules", zap.Error(err))
		mergedSpec = mergedSpec.DeepCopy()
		if mergedSpec.Egress != nil {
			mergedSpec.Egress.TrafficRules = nil
			mergedSpec.Egress.ProtocolRules = nil
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
			merged.Egress.ProtocolRules = mergeProtocolRules(merged.Egress.ProtocolRules, request.Egress.ProtocolRules)
			merged.Egress.CredentialRules = mergeEgressCredentialRules(merged.Egress.CredentialRules, request.Egress.CredentialRules)
			if request.Egress.Proxy != nil {
				merged.Egress.Proxy = cloneEgressProxyPolicy(request.Egress.Proxy)
			}
		}
	}
	return merged
}

func cloneEgressProxyPolicy(in *v1alpha1.EgressProxyPolicy) *v1alpha1.EgressProxyPolicy {
	if in == nil {
		return nil
	}
	out := *in
	return &out
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

func mergeProtocolRules(base, override []v1alpha1.ProtocolRule) []v1alpha1.ProtocolRule {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	if len(base) == 0 {
		return append([]v1alpha1.ProtocolRule(nil), override...)
	}
	if len(override) == 0 {
		return append([]v1alpha1.ProtocolRule(nil), base...)
	}

	out := append([]v1alpha1.ProtocolRule(nil), base...)
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
	seenProtocolRuleNames := make(map[string]struct{}, len(policy.Egress.ProtocolRules))
	for _, rule := range policy.Egress.ProtocolRules {
		if err := validateProtocolRule(rule); err != nil {
			return fmt.Errorf("egress protocol rule %q is invalid: %w", rule.Name, err)
		}
		if rule.Name == "" {
			continue
		}
		if _, ok := seenProtocolRuleNames[rule.Name]; ok {
			return fmt.Errorf("duplicate egress protocol rule name %q", rule.Name)
		}
		seenProtocolRuleNames[rule.Name] = struct{}{}
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
	if err := validateEgressProxyConfig(policy.Egress.Proxy, bindingProjectionTypes); err != nil {
		return err
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
		if rule.Protocol == v1alpha1.EgressAuthProtocolSSH {
			if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; !ok || projectionType != v1alpha1.CredentialProjectionTypeSSHProxy {
				return fmt.Errorf("egress rule %q with protocol ssh requires ssh_proxy projection on %q", rule.Name, rule.CredentialRef)
			}
		}
		if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; ok && projectionType == v1alpha1.CredentialProjectionTypePlaceholderSubstitution {
			if !isHTTPFamilyEgressAuthProtocol(rule.Protocol) {
				return fmt.Errorf("egress rule %q with placeholder_substitution projection requires protocol http, https, or grpc", rule.Name)
			}
			if rule.Protocol == v1alpha1.EgressAuthProtocolHTTPS || rule.Protocol == v1alpha1.EgressAuthProtocolGRPC {
				if rule.TLSMode != v1alpha1.EgressTLSModeTerminateReoriginate {
					return fmt.Errorf("egress rule %q with placeholder_substitution projection and protocol %s requires tlsMode terminate-reoriginate", rule.Name, rule.Protocol)
				}
			}
		}
		if err := validateHTTPMatch(rule); err != nil {
			return err
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

func validateEgressProxyConfig(proxy *v1alpha1.EgressProxyPolicy, bindingProjectionTypes map[string]v1alpha1.CredentialProjectionType) error {
	if proxy == nil {
		return nil
	}
	if proxy.Type != v1alpha1.EgressProxyTypeSOCKS5 {
		return fmt.Errorf("egress proxy type %q is not supported", proxy.Type)
	}
	if strings.TrimSpace(proxy.Address) == "" {
		return fmt.Errorf("egress proxy address is required")
	}
	if proxy.CredentialRef == "" {
		return nil
	}
	projectionType, ok := bindingProjectionTypes[proxy.CredentialRef]
	if !ok {
		return fmt.Errorf("egress proxy credentialRef %q not found", proxy.CredentialRef)
	}
	if projectionType != v1alpha1.CredentialProjectionTypeUsernamePassword {
		return fmt.Errorf("egress proxy credentialRef %q requires username_password projection", proxy.CredentialRef)
	}
	return nil
}

func validateHTTPMatch(rule v1alpha1.EgressCredentialRule) error {
	if rule.HTTPMatch == nil {
		return nil
	}
	switch rule.Protocol {
	case v1alpha1.EgressAuthProtocolHTTP, v1alpha1.EgressAuthProtocolHTTPS, v1alpha1.EgressAuthProtocolGRPC:
	default:
		return fmt.Errorf("egress rule %q httpMatch requires protocol http, https, or grpc", rule.Name)
	}
	if rule.Protocol == v1alpha1.EgressAuthProtocolHTTPS || rule.Protocol == v1alpha1.EgressAuthProtocolGRPC {
		if rule.TLSMode != v1alpha1.EgressTLSModeTerminateReoriginate {
			return fmt.Errorf("egress rule %q httpMatch with protocol %s requires tlsMode terminate-reoriginate", rule.Name, rule.Protocol)
		}
	}
	return validateHTTPMatchFields("egress rule "+rule.Name, rule.HTTPMatch)
}

func isHTTPFamilyEgressAuthProtocol(protocol v1alpha1.EgressAuthProtocol) bool {
	switch protocol {
	case v1alpha1.EgressAuthProtocolHTTP, v1alpha1.EgressAuthProtocolHTTPS, v1alpha1.EgressAuthProtocolGRPC:
		return true
	default:
		return false
	}
}

func validHTTPMethod(method string) bool {
	switch method {
	case http.MethodGet,
		http.MethodHead,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodConnect,
		http.MethodOptions,
		http.MethodTrace:
		return true
	default:
		return false
	}
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

func validateProtocolRule(rule v1alpha1.ProtocolRule) error {
	switch rule.Protocol {
	case v1alpha1.ProtocolRuleProtocolHTTP:
		if rule.HTTP == nil {
			return fmt.Errorf("http config is required")
		}
		if rule.MCP != nil {
			return fmt.Errorf("mcp config is not supported for http protocol rules")
		}
	case v1alpha1.ProtocolRuleProtocolMCP:
		if rule.MCP == nil {
			return fmt.Errorf("mcp config is required")
		}
		if rule.HTTP != nil {
			return fmt.Errorf("http config is not supported for mcp protocol rules")
		}
	default:
		return fmt.Errorf("unsupported protocol %q", rule.Protocol)
	}
	switch rule.TLSMode {
	case "", v1alpha1.EgressTLSModeTerminateReoriginate:
	default:
		return fmt.Errorf("unsupported tlsMode %q", rule.TLSMode)
	}
	if err := validateProtocolRuleDestinations(rule); err != nil {
		return err
	}
	if rule.HTTPMatch != nil {
		if err := validateHTTPMatchFields("egress protocol rule "+rule.Name, rule.HTTPMatch); err != nil {
			return err
		}
	}
	if rule.HTTP != nil {
		if err := validateHTTPProtocolRule(rule.HTTP); err != nil {
			return err
		}
	}
	if rule.MCP != nil && rule.MCP.Tools != nil {
		if err := validateMCPToolPolicy(rule.MCP.Tools); err != nil {
			return err
		}
	}
	return nil
}

func validateProtocolRuleDestinations(rule v1alpha1.ProtocolRule) error {
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
		if proto != "" && proto != "tcp" {
			return fmt.Errorf("unsupported protocol %q", port.Protocol)
		}
	}
	return nil
}

func validateHTTPMatchFields(label string, match *v1alpha1.HTTPMatch) error {
	for _, method := range match.Methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			return fmt.Errorf("%s httpMatch method is required", label)
		}
		if !validHTTPMethod(method) {
			return fmt.Errorf("%s httpMatch method %q is invalid", label, method)
		}
	}
	for _, path := range match.Paths {
		if strings.TrimSpace(path) == "" || !strings.HasPrefix(path, "/") {
			return fmt.Errorf("%s httpMatch path %q must start with /", label, path)
		}
	}
	for _, prefix := range match.PathPrefixes {
		if strings.TrimSpace(prefix) == "" || !strings.HasPrefix(prefix, "/") {
			return fmt.Errorf("%s httpMatch pathPrefix %q must start with /", label, prefix)
		}
	}
	for _, matcher := range match.Headers {
		if strings.TrimSpace(matcher.Name) == "" {
			return fmt.Errorf("%s httpMatch header name is required", label)
		}
	}
	for _, matcher := range match.Query {
		if strings.TrimSpace(matcher.Name) == "" {
			return fmt.Errorf("%s httpMatch query name is required", label)
		}
	}
	return nil
}

func validateHTTPProtocolRule(rule *v1alpha1.HTTPProtocolRule) error {
	if rule.Methods != nil {
		if err := validateHTTPMethodPolicy(rule.Methods); err != nil {
			return err
		}
	}
	if rule.Paths != nil {
		if err := validateHTTPPathPolicy(rule.Paths); err != nil {
			return err
		}
	}
	return nil
}

func validateHTTPMethodPolicy(policy *v1alpha1.HTTPMethodPolicy) error {
	seenAllowed := make(map[string]struct{}, len(policy.Allowed))
	for _, method := range policy.Allowed {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			return fmt.Errorf("http methods allowed method is required")
		}
		if !validHTTPMethod(method) {
			return fmt.Errorf("http methods allowed method %q is invalid", method)
		}
		if _, ok := seenAllowed[method]; ok {
			return fmt.Errorf("duplicate http allowed method %q", method)
		}
		seenAllowed[method] = struct{}{}
	}
	seenDenied := make(map[string]struct{}, len(policy.Denied))
	for _, method := range policy.Denied {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			return fmt.Errorf("http methods denied method is required")
		}
		if !validHTTPMethod(method) {
			return fmt.Errorf("http methods denied method %q is invalid", method)
		}
		if _, ok := seenDenied[method]; ok {
			return fmt.Errorf("duplicate http denied method %q", method)
		}
		seenDenied[method] = struct{}{}
	}
	return nil
}

func validateHTTPPathPolicy(policy *v1alpha1.HTTPPathPolicy) error {
	if err := validateHTTPPathList("http paths allowed", policy.Allowed); err != nil {
		return err
	}
	if err := validateHTTPPathList("http paths denied", policy.Denied); err != nil {
		return err
	}
	if err := validateHTTPPathList("http path prefixes allowed", policy.AllowedPrefixes); err != nil {
		return err
	}
	if err := validateHTTPPathList("http path prefixes denied", policy.DeniedPrefixes); err != nil {
		return err
	}
	return nil
}

func validateHTTPPathList(label string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, path := range values {
		path = strings.TrimSpace(path)
		if path == "" || !strings.HasPrefix(path, "/") {
			return fmt.Errorf("%s path %q must start with /", label, path)
		}
		if _, ok := seen[path]; ok {
			return fmt.Errorf("duplicate %s path %q", label, path)
		}
		seen[path] = struct{}{}
	}
	return nil
}

func validateMCPToolPolicy(policy *v1alpha1.MCPToolPolicy) error {
	seenAllowed := make(map[string]struct{}, len(policy.Allowed))
	for _, name := range policy.Allowed {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("mcp tools allowed name is required")
		}
		if _, ok := seenAllowed[name]; ok {
			return fmt.Errorf("duplicate mcp allowed tool %q", name)
		}
		seenAllowed[name] = struct{}{}
	}
	seenDenied := make(map[string]struct{}, len(policy.Denied))
	for _, name := range policy.Denied {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("mcp tools denied name is required")
		}
		if _, ok := seenDenied[name]; ok {
			return fmt.Errorf("duplicate mcp denied tool %q", name)
		}
		seenDenied[name] = struct{}{}
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
	case v1alpha1.CredentialProjectionTypePlaceholderSubstitution:
		if projection.PlaceholderSubstitution == nil {
			return fmt.Errorf("credential binding projection.placeholderSubstitution is required for %q", ref)
		}
		if len(projection.PlaceholderSubstitution.Replacements) == 0 {
			return fmt.Errorf("credential binding projection.placeholderSubstitution replacements are required for %q", ref)
		}
		seen := map[string]struct{}{}
		for _, replacement := range projection.PlaceholderSubstitution.Replacements {
			if strings.TrimSpace(replacement.Placeholder) == "" {
				return fmt.Errorf("credential binding placeholder is required for %q", ref)
			}
			if strings.TrimSpace(replacement.ValueTemplate) == "" {
				return fmt.Errorf("credential binding placeholder valueTemplate is required for %q", ref)
			}
			if len(replacement.Locations) == 0 {
				return fmt.Errorf("credential binding placeholder locations are required for %q", ref)
			}
			for _, location := range replacement.Locations {
				switch location {
				case v1alpha1.PlaceholderSubstitutionLocationHeader,
					v1alpha1.PlaceholderSubstitutionLocationQuery,
					v1alpha1.PlaceholderSubstitutionLocationBody:
				default:
					return fmt.Errorf("credential binding placeholder location %q is not supported for %q", location, ref)
				}
				key := replacement.Placeholder + "\x00" + string(location)
				if _, ok := seen[key]; ok {
					return fmt.Errorf("credential binding placeholder %q has duplicate location %q for %q", replacement.Placeholder, location, ref)
				}
				seen[key] = struct{}{}
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
	case v1alpha1.CredentialProjectionTypeSSHProxy:
		if projection.SSHProxy == nil {
			return fmt.Errorf("credential binding projection.sshProxy is required for %q", ref)
		}
		if strings.TrimSpace(projection.SSHProxy.UpstreamUsername) == "" {
			return fmt.Errorf("credential binding projection.sshProxy upstreamUsername is required for %q", ref)
		}
		if len(projection.SSHProxy.SandboxPublicKeys) == 0 {
			return fmt.Errorf("credential binding projection.sshProxy sandboxPublicKeys is required for %q", ref)
		}
		for _, key := range projection.SSHProxy.SandboxPublicKeys {
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("credential binding projection.sshProxy sandbox public key is required for %q", ref)
			}
		}
	default:
		return fmt.Errorf("credential binding projection type %q is not supported for %q", projection.Type, ref)
	}
	return nil
}
