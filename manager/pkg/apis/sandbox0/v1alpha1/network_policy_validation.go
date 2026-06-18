package v1alpha1

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// ValidateSandboxNetworkPolicy validates the user-supplied sandbox network policy fields.
func ValidateSandboxNetworkPolicy(policy *SandboxNetworkPolicy, bindings []CredentialBinding) error {
	if err := ValidateSandboxNetworkTrafficPolicy(policy); err != nil {
		return err
	}
	return ValidateSandboxNetworkCredentialPolicy(policy, bindings)
}

// ValidateSandboxNetworkTrafficPolicy validates traffic and protocol rule configuration.
func ValidateSandboxNetworkTrafficPolicy(policy *SandboxNetworkPolicy) error {
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
		case TrafficRuleActionAllow, TrafficRuleActionDeny:
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

// ValidateSandboxNetworkCredentialPolicy validates credential bindings and egress credential rules.
func ValidateSandboxNetworkCredentialPolicy(policy *SandboxNetworkPolicy, bindings []CredentialBinding) error {
	if policy == nil {
		return nil
	}
	bindingRefs := make(map[string]struct{})
	bindingProjectionTypes := make(map[string]CredentialProjectionType, len(bindings))
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
		if _, ok := bindingRefs[rule.CredentialRef]; !ok {
			return fmt.Errorf("egress rule credentialRef %q not found", rule.CredentialRef)
		}
		if rule.Protocol == EgressAuthProtocolTLS {
			if rule.TLSMode != EgressTLSModeTerminateReoriginate {
				return fmt.Errorf("egress rule %q with protocol tls requires tlsMode terminate-reoriginate", rule.Name)
			}
			if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; !ok || projectionType != CredentialProjectionTypeTLSClientCertificate {
				return fmt.Errorf("egress rule %q with protocol tls requires tls_client_certificate projection on %q", rule.Name, rule.CredentialRef)
			}
		}
		if rule.Protocol == EgressAuthProtocolSOCKS5 || rule.Protocol == EgressAuthProtocolMQTT || rule.Protocol == EgressAuthProtocolRedis {
			if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; !ok || projectionType != CredentialProjectionTypeUsernamePassword {
				return fmt.Errorf("egress rule %q with protocol %s requires username_password projection on %q", rule.Name, rule.Protocol, rule.CredentialRef)
			}
		}
		if rule.Protocol == EgressAuthProtocolSSH {
			if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; !ok || projectionType != CredentialProjectionTypeSSHProxy {
				return fmt.Errorf("egress rule %q with protocol ssh requires ssh_proxy projection on %q", rule.Name, rule.CredentialRef)
			}
		}
		if projectionType, ok := bindingProjectionTypes[rule.CredentialRef]; ok && projectionType == CredentialProjectionTypePlaceholderSubstitution {
			if !isHTTPFamilyEgressAuthProtocol(rule.Protocol) {
				return fmt.Errorf("egress rule %q with placeholder_substitution projection requires protocol http, https, or grpc", rule.Name)
			}
			if rule.Protocol == EgressAuthProtocolHTTPS || rule.Protocol == EgressAuthProtocolGRPC {
				if rule.TLSMode != EgressTLSModeTerminateReoriginate {
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

func validateEgressProxyConfig(proxy *EgressProxyPolicy, bindingProjectionTypes map[string]CredentialProjectionType) error {
	if proxy == nil {
		return nil
	}
	if proxy.Type != EgressProxyTypeSOCKS5 {
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
	if projectionType != CredentialProjectionTypeUsernamePassword {
		return fmt.Errorf("egress proxy credentialRef %q requires username_password projection", proxy.CredentialRef)
	}
	return nil
}

func validateHTTPMatch(rule EgressCredentialRule) error {
	if rule.HTTPMatch == nil {
		return nil
	}
	switch rule.Protocol {
	case EgressAuthProtocolHTTP, EgressAuthProtocolHTTPS, EgressAuthProtocolGRPC:
	default:
		return fmt.Errorf("egress rule %q httpMatch requires protocol http, https, or grpc", rule.Name)
	}
	if rule.Protocol == EgressAuthProtocolHTTPS || rule.Protocol == EgressAuthProtocolGRPC {
		if rule.TLSMode != EgressTLSModeTerminateReoriginate {
			return fmt.Errorf("egress rule %q httpMatch with protocol %s requires tlsMode terminate-reoriginate", rule.Name, rule.Protocol)
		}
	}
	return validateHTTPMatchFields("egress rule "+rule.Name, rule.HTTPMatch)
}

func isHTTPFamilyEgressAuthProtocol(protocol EgressAuthProtocol) bool {
	switch protocol {
	case EgressAuthProtocolHTTP, EgressAuthProtocolHTTPS, EgressAuthProtocolGRPC:
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

func hasLegacyTrafficLists(egress *NetworkEgressPolicy) bool {
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

func validateTrafficRule(rule TrafficRule) error {
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

func validateProtocolRule(rule ProtocolRule) error {
	switch rule.Protocol {
	case ProtocolRuleProtocolHTTP:
		if rule.HTTP == nil {
			return fmt.Errorf("http config is required")
		}
		if rule.MCP != nil {
			return fmt.Errorf("mcp config is not supported for http protocol rules")
		}
	case ProtocolRuleProtocolMCP:
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
	case "", EgressTLSModeTerminateReoriginate:
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

func validateProtocolRuleDestinations(rule ProtocolRule) error {
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

func validateHTTPMatchFields(label string, match *HTTPMatch) error {
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

func validateHTTPProtocolRule(rule *HTTPProtocolRule) error {
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

func validateHTTPMethodPolicy(policy *HTTPMethodPolicy) error {
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

func validateHTTPPathPolicy(policy *HTTPPathPolicy) error {
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

func validateMCPToolPolicy(policy *MCPToolPolicy) error {
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

func validateProjection(ref string, projection ProjectionSpec) error {
	switch projection.Type {
	case CredentialProjectionTypeHTTPHeaders:
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
	case CredentialProjectionTypePlaceholderSubstitution:
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
				case PlaceholderSubstitutionLocationHeader,
					PlaceholderSubstitutionLocationQuery,
					PlaceholderSubstitutionLocationBody:
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
	case CredentialProjectionTypeTLSClientCertificate:
		if projection.TLSClientCertificate == nil {
			return fmt.Errorf("credential binding projection.tlsClientCertificate is required for %q", ref)
		}
	case CredentialProjectionTypeUsernamePassword:
		if projection.UsernamePassword == nil {
			return fmt.Errorf("credential binding projection.usernamePassword is required for %q", ref)
		}
	case CredentialProjectionTypeSSHProxy:
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
