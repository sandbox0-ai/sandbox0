package policy

import (
	"fmt"
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

type DomainMatchType string

const (
	DomainMatchExact  DomainMatchType = "exact"
	DomainMatchSuffix DomainMatchType = "suffix"
)

type DomainRule struct {
	Pattern string
	Type    DomainMatchType
}

type PortRange struct {
	Protocol string
	Start    int32
	End      int32
}

type CompiledTrafficRule struct {
	Name         string
	Action       v1alpha1.TrafficRuleAction
	CIDRs        []*net.IPNet
	Ports        []PortRange
	Domains      []DomainRule
	AppProtocols []string
}

type CompiledRuleSet struct {
	AllowedCIDRs   []*net.IPNet
	DeniedCIDRs    []*net.IPNet
	AllowedPorts   []PortRange
	DeniedPorts    []PortRange
	AllowedDomains []DomainRule
	DeniedDomains  []DomainRule
	TrafficRules   []CompiledTrafficRule
	AuthRules      []CompiledEgressAuthRule
}

type CompiledEgressAuthRule struct {
	Name          string
	AuthRef       string
	Rollout       v1alpha1.EgressAuthRolloutMode
	Protocol      v1alpha1.EgressAuthProtocol
	TLSMode       v1alpha1.EgressTLSMode
	FailurePolicy v1alpha1.EgressAuthFailurePolicy
	Domains       []DomainRule
	Ports         []PortRange
}

type CompiledPolicy struct {
	SandboxID string
	TeamID    string
	Mode      v1alpha1.NetworkPolicyMode
	Egress    CompiledRuleSet
	Platform  *PlatformPolicy
}

func CompileNetworkPolicy(spec *v1alpha1.NetworkPolicySpec) (*CompiledPolicy, error) {
	if spec == nil {
		return &CompiledPolicy{
			Mode:   v1alpha1.NetworkModeAllowAll,
			Egress: CompiledRuleSet{},
		}, nil
	}

	mode := spec.Mode
	if mode == "" {
		mode = v1alpha1.NetworkModeAllowAll
	}

	compiled := &CompiledPolicy{
		SandboxID: spec.SandboxID,
		TeamID:    spec.TeamID,
		Mode:      mode,
	}

	if spec.Egress != nil {
		if len(spec.Egress.TrafficRules) > 0 && hasLegacyTrafficLists(spec.Egress) {
			return nil, fmt.Errorf("trafficRules cannot be combined with legacy allowed*/denied* fields")
		}
		trafficRules, err := compileTrafficRules(spec.Egress.TrafficRules)
		if err != nil {
			return nil, fmt.Errorf("compile egress traffic rules: %w", err)
		}
		if len(trafficRules) == 0 {
			trafficRules, err = compileLegacyTrafficRules(mode, spec.Egress.AllowedCIDRs, spec.Egress.DeniedCIDRs, spec.Egress.AllowedPorts, spec.Egress.DeniedPorts, spec.Egress.AllowedDomains, spec.Egress.DeniedDomains)
			if err != nil {
				return nil, fmt.Errorf("compile legacy egress rules: %w", err)
			}
		}
		authRules, err := compileEgressAuthRules(spec.Egress.CredentialRules)
		if err != nil {
			return nil, fmt.Errorf("compile egress auth rules: %w", err)
		}
		compiled.Egress = CompiledRuleSet{
			TrafficRules: trafficRules,
			AuthRules:    authRules,
		}
	}

	return compiled, nil
}

func compileRuleSet(
	allowedCIDRs []string,
	deniedCIDRs []string,
	allowedPorts []v1alpha1.PortSpec,
	deniedPorts []v1alpha1.PortSpec,
	allowedDomains []string,
	deniedDomains []string,
) (CompiledRuleSet, error) {
	result := CompiledRuleSet{}

	var err error
	result.AllowedCIDRs, err = parseCIDRs(allowedCIDRs)
	if err != nil {
		return result, err
	}
	deniedCIDRsParsed, err := parseCIDRs(deniedCIDRs)
	if err != nil {
		return result, err
	}
	result.DeniedCIDRs = append(result.DeniedCIDRs, deniedCIDRsParsed...)

	result.AllowedPorts, err = parsePorts(allowedPorts)
	if err != nil {
		return result, err
	}
	result.DeniedPorts, err = parsePorts(deniedPorts)
	if err != nil {
		return result, err
	}

	result.AllowedDomains, err = parseDomains(allowedDomains)
	if err != nil {
		return result, err
	}
	result.DeniedDomains, err = parseDomains(deniedDomains)
	if err != nil {
		return result, err
	}

	return result, nil
}

func parseCIDRs(values []string) ([]*net.IPNet, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		netIP, netCIDR, err := net.ParseCIDR(value)
		if err != nil || netIP == nil {
			return nil, fmt.Errorf("invalid CIDR %q", value)
		}
		out = append(out, netCIDR)
	}
	return out, nil
}

func parsePorts(values []v1alpha1.PortSpec) ([]PortRange, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]PortRange, 0, len(values))
	for _, port := range values {
		if port.Port <= 0 || port.Port > 65535 {
			return nil, fmt.Errorf("invalid port %d", port.Port)
		}
		end := port.Port
		if port.EndPort != nil {
			if *port.EndPort < port.Port || *port.EndPort > 65535 {
				return nil, fmt.Errorf("invalid end port %d", *port.EndPort)
			}
			end = *port.EndPort
		}
		proto := strings.ToLower(strings.TrimSpace(port.Protocol))
		if proto == "" {
			proto = "tcp"
		}
		if proto != "tcp" && proto != "udp" {
			return nil, fmt.Errorf("unsupported protocol %q", port.Protocol)
		}
		out = append(out, PortRange{
			Protocol: proto,
			Start:    port.Port,
			End:      end,
		})
	}
	return out, nil
}

func parseDomains(values []string) ([]DomainRule, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]DomainRule, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "*.") {
			suffix := strings.TrimPrefix(value, "*.")
			if suffix == "" {
				return nil, fmt.Errorf("invalid wildcard domain %q", value)
			}
			out = append(out, DomainRule{Pattern: suffix, Type: DomainMatchSuffix})
			continue
		}
		out = append(out, DomainRule{Pattern: value, Type: DomainMatchExact})
	}
	return out, nil
}

func compileEgressAuthRules(values []v1alpha1.EgressCredentialRule) ([]CompiledEgressAuthRule, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]CompiledEgressAuthRule, 0, len(values))
	for _, value := range values {
		authRef := strings.TrimSpace(value.CredentialRef)
		if authRef == "" {
			return nil, fmt.Errorf("credentialRef is required")
		}
		domains, err := parseDomains(value.Domains)
		if err != nil {
			return nil, fmt.Errorf("parse auth rule domains for %q: %w", authRef, err)
		}
		ports, err := parsePorts(value.Ports)
		if err != nil {
			return nil, fmt.Errorf("parse auth rule ports for %q: %w", authRef, err)
		}
		rule := CompiledEgressAuthRule{
			Name:          strings.TrimSpace(value.Name),
			AuthRef:       authRef,
			Rollout:       value.Rollout,
			Protocol:      value.Protocol,
			TLSMode:       value.TLSMode,
			FailurePolicy: value.FailurePolicy,
			Domains:       domains,
			Ports:         ports,
		}
		switch rule.Rollout {
		case "", v1alpha1.EgressAuthRolloutEnabled, v1alpha1.EgressAuthRolloutDisabled:
		default:
			return nil, fmt.Errorf("unsupported auth rule rollout %q", value.Rollout)
		}
		switch rule.Protocol {
		case "",
			v1alpha1.EgressAuthProtocolHTTP,
			v1alpha1.EgressAuthProtocolHTTPS,
			v1alpha1.EgressAuthProtocolGRPC,
			v1alpha1.EgressAuthProtocolTLS,
			v1alpha1.EgressAuthProtocolSOCKS5,
			v1alpha1.EgressAuthProtocolMQTT,
			v1alpha1.EgressAuthProtocolRedis:
		default:
			return nil, fmt.Errorf("unsupported auth rule protocol %q", value.Protocol)
		}
		switch rule.TLSMode {
		case "", v1alpha1.EgressTLSModePassthrough, v1alpha1.EgressTLSModeTerminateReoriginate:
		default:
			return nil, fmt.Errorf("unsupported auth rule tls mode %q", value.TLSMode)
		}
		switch rule.FailurePolicy {
		case "", v1alpha1.EgressAuthFailurePolicyFailClosed, v1alpha1.EgressAuthFailurePolicyFailOpen:
		default:
			return nil, fmt.Errorf("unsupported auth rule failure policy %q", value.FailurePolicy)
		}
		out = append(out, rule)
	}
	return out, nil
}

func compileTrafficRules(values []v1alpha1.TrafficRule) ([]CompiledTrafficRule, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]CompiledTrafficRule, 0, len(values))
	for _, value := range values {
		cidrs, err := parseCIDRs(value.CIDRs)
		if err != nil {
			return nil, fmt.Errorf("parse traffic rule cidrs for %q: %w", value.Name, err)
		}
		ports, err := parsePorts(value.Ports)
		if err != nil {
			return nil, fmt.Errorf("parse traffic rule ports for %q: %w", value.Name, err)
		}
		domains, err := parseDomains(value.Domains)
		if err != nil {
			return nil, fmt.Errorf("parse traffic rule domains for %q: %w", value.Name, err)
		}
		rule := CompiledTrafficRule{
			Name:         strings.TrimSpace(value.Name),
			Action:       value.Action,
			CIDRs:        cidrs,
			Ports:        ports,
			Domains:      domains,
			AppProtocols: normalizeTrafficRuleAppProtocols(value.AppProtocols),
		}
		switch rule.Action {
		case v1alpha1.TrafficRuleActionAllow, v1alpha1.TrafficRuleActionDeny:
		default:
			return nil, fmt.Errorf("unsupported traffic rule action %q", value.Action)
		}
		if len(rule.CIDRs) == 0 && len(rule.Ports) == 0 && len(rule.Domains) == 0 && len(rule.AppProtocols) == 0 {
			return nil, fmt.Errorf("traffic rule %q must define at least one matcher", value.Name)
		}
		if err := validateTrafficRuleAppProtocols(rule.AppProtocols); err != nil {
			return nil, fmt.Errorf("traffic rule %q app protocols: %w", value.Name, err)
		}
		out = append(out, rule)
	}
	return out, nil
}

func compileLegacyTrafficRules(
	mode v1alpha1.NetworkPolicyMode,
	allowedCIDRs []string,
	deniedCIDRs []string,
	allowedPorts []v1alpha1.PortSpec,
	deniedPorts []v1alpha1.PortSpec,
	allowedDomains []string,
	deniedDomains []string,
) ([]CompiledTrafficRule, error) {
	switch mode {
	case "", v1alpha1.NetworkModeAllowAll:
		return compileLegacyDenyRules(deniedCIDRs, deniedPorts, deniedDomains)
	case v1alpha1.NetworkModeBlockAll:
		return compileLegacyAllowRules(allowedCIDRs, allowedPorts, allowedDomains)
	default:
		return nil, fmt.Errorf("unsupported network mode %q", mode)
	}
}

func compileLegacyAllowRules(cidrs []string, ports []v1alpha1.PortSpec, domains []string) ([]CompiledTrafficRule, error) {
	if len(cidrs) == 0 && len(ports) == 0 && len(domains) == 0 {
		return nil, nil
	}
	compiledCIDRs, err := parseCIDRs(cidrs)
	if err != nil {
		return nil, err
	}
	compiledPorts, err := parsePorts(ports)
	if err != nil {
		return nil, err
	}
	compiledDomains, err := parseDomains(domains)
	if err != nil {
		return nil, err
	}
	return []CompiledTrafficRule{{
		Name:    "legacy-allow",
		Action:  v1alpha1.TrafficRuleActionAllow,
		CIDRs:   compiledCIDRs,
		Ports:   compiledPorts,
		Domains: compiledDomains,
	}}, nil
}

func compileLegacyDenyRules(cidrs []string, ports []v1alpha1.PortSpec, domains []string) ([]CompiledTrafficRule, error) {
	rules := make([]CompiledTrafficRule, 0, 3)
	if len(cidrs) > 0 {
		compiledCIDRs, err := parseCIDRs(cidrs)
		if err != nil {
			return nil, err
		}
		rules = append(rules, CompiledTrafficRule{
			Name:   "legacy-deny-cidrs",
			Action: v1alpha1.TrafficRuleActionDeny,
			CIDRs:  compiledCIDRs,
		})
	}
	if len(ports) > 0 {
		compiledPorts, err := parsePorts(ports)
		if err != nil {
			return nil, err
		}
		rules = append(rules, CompiledTrafficRule{
			Name:   "legacy-deny-ports",
			Action: v1alpha1.TrafficRuleActionDeny,
			Ports:  compiledPorts,
		})
	}
	if len(domains) > 0 {
		compiledDomains, err := parseDomains(domains)
		if err != nil {
			return nil, err
		}
		rules = append(rules, CompiledTrafficRule{
			Name:    "legacy-deny-domains",
			Action:  v1alpha1.TrafficRuleActionDeny,
			Domains: compiledDomains,
		})
	}
	if len(rules) == 0 {
		return nil, nil
	}
	return rules, nil
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

func normalizeTrafficRuleAppProtocols(values []v1alpha1.TrafficRuleAppProtocol) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		proto := strings.ToLower(strings.TrimSpace(string(value)))
		if proto == "" {
			continue
		}
		out = append(out, proto)
	}
	return out
}

func validateTrafficRuleAppProtocols(values []string) error {
	for _, value := range values {
		switch value {
		case "http", "tls", "ssh", "socks5", "mqtt", "redis", "amqp", "dns", "mongodb", "udp":
		default:
			return fmt.Errorf("unsupported protocol %q", value)
		}
	}
	return nil
}
