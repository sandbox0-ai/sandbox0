package policy

import (
	"net"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

type UnknownTrafficAction string

const (
	UnknownTrafficPassThrough UnknownTrafficAction = "pass-through"
	UnknownTrafficDeny        UnknownTrafficAction = "deny"
)

func AllowEgressL4(policy *CompiledPolicy, destIP net.IP, destPort int, protocol string) bool {
	return allowEgressDestination(policy, destIP, destPort, protocol, "")
}

// AllowEgressDestination evaluates the L4 phase of an egress decision with
// optional host classification context.
func AllowEgressDestination(policy *CompiledPolicy, destIP net.IP, destPort int, protocol string, host string) bool {
	return allowEgressDestination(policy, destIP, destPort, protocol, host)
}

func allowEgressDestination(policy *CompiledPolicy, destIP net.IP, destPort int, protocol string, host string) bool {
	if policy == nil {
		return true
	}
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		protocol = "tcp"
	}
	if policy.Platform != nil {
		if isOtherSandboxPod(policy.Platform, destIP) {
			return false
		}
		if matchCIDR(destIP, policy.Platform.DeniedCIDRs) {
			return false
		}
		if matchCIDR(destIP, policy.Platform.AllowedCIDRs) {
			return true
		}
	}
	switch policy.Mode {
	case v1alpha1.NetworkModeAllowAll:
		// allow-all defaults to permit and applies denied* fields as subtractive rules.
		if matchCIDR(destIP, policy.Egress.DeniedCIDRs) {
			return false
		}
		if matchPort(destPort, protocol, policy.Egress.DeniedPorts) {
			return false
		}
		return true
	case v1alpha1.NetworkModeBlockAll:
		// block-all defaults to deny and applies allowed* fields as additive rules.
		// denied* fields do not participate in block-all evaluation.
		if !hasExplicitL4AllowList(policy) {
			return host != "" && hasExplicitDomainAllowList(policy)
		}
		if len(policy.Egress.AllowedCIDRs) > 0 && !matchCIDR(destIP, policy.Egress.AllowedCIDRs) {
			return false
		}
		if len(policy.Egress.AllowedPorts) > 0 && !matchPort(destPort, protocol, policy.Egress.AllowedPorts) {
			return false
		}
		return true
	default:
		return false
	}
}

func UnknownFallbackAction(policy *CompiledPolicy) UnknownTrafficAction {
	if policy == nil {
		return UnknownTrafficPassThrough
	}
	switch policy.Mode {
	case v1alpha1.NetworkModeBlockAll:
		return UnknownTrafficDeny
	default:
		return UnknownTrafficPassThrough
	}
}

// AllowUnknownEgressFallback evaluates whether unknown traffic should be
// passed through after L4 evaluation. Platform-managed destinations retain
// pass-through behavior even under block-all so sandbox bootstrap traffic
// to core services remains functional.
func AllowUnknownEgressFallback(policy *CompiledPolicy, destIP net.IP, host string) bool {
	if policy == nil {
		return true
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if policy.Platform != nil {
		if isOtherSandboxPod(policy.Platform, destIP) {
			return false
		}
		if matchCIDR(destIP, policy.Platform.DeniedCIDRs) {
			return false
		}
		if host != "" && matchDomain(host, policy.Platform.DeniedDomains) {
			return false
		}
		if matchCIDR(destIP, policy.Platform.AllowedCIDRs) {
			return true
		}
		if host != "" && matchDomain(host, policy.Platform.AllowedDomains) {
			return true
		}
	}
	return UnknownFallbackAction(policy) == UnknownTrafficPassThrough
}

func AllowEgressDomain(policy *CompiledPolicy, host string) bool {
	if policy == nil {
		return true
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	if policy.Platform != nil {
		if matchDomain(host, policy.Platform.DeniedDomains) {
			return false
		}
		if matchDomain(host, policy.Platform.AllowedDomains) {
			return true
		}
	}
	switch policy.Mode {
	case v1alpha1.NetworkModeAllowAll:
		// allow-all defaults to permit and applies denied* fields as subtractive rules.
		if matchDomain(host, policy.Egress.DeniedDomains) {
			return false
		}
		return true
	case v1alpha1.NetworkModeBlockAll:
		// block-all defaults to deny and applies allowed* fields as additive rules.
		// denied* fields do not participate in block-all evaluation.
		if len(policy.Egress.AllowedDomains) == 0 {
			return false
		}
		return matchDomain(host, policy.Egress.AllowedDomains)
	default:
		return false
	}
}

func HasDomainRules(policy *CompiledPolicy) bool {
	if policy == nil {
		return false
	}
	if policy.Platform != nil {
		if len(policy.Platform.AllowedDomains) > 0 || len(policy.Platform.DeniedDomains) > 0 {
			return true
		}
	}
	return len(policy.Egress.AllowedDomains) > 0 || len(policy.Egress.DeniedDomains) > 0
}

func MatchEgressAuthRule(policy *CompiledPolicy, transport, protocol string, destPort int, host string) *CompiledEgressAuthRule {
	if policy == nil || len(policy.Egress.AuthRules) == 0 {
		return nil
	}
	transport = strings.ToLower(strings.TrimSpace(transport))
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	host = strings.ToLower(strings.TrimSpace(host))
	for idx := range policy.Egress.AuthRules {
		rule := &policy.Egress.AuthRules[idx]
		if !matchEgressAuthProtocol(rule.Protocol, transport, protocol) {
			continue
		}
		if len(rule.Ports) > 0 && !matchPort(destPort, transport, rule.Ports) {
			continue
		}
		if len(rule.Domains) > 0 {
			if host == "" || !matchDomain(host, rule.Domains) {
				continue
			}
		}
		return rule
	}
	return nil
}

func matchEgressAuthProtocol(ruleProtocol v1alpha1.EgressAuthProtocol, transport, classifiedProtocol string) bool {
	switch ruleProtocol {
	case "":
		return true
	case v1alpha1.EgressAuthProtocolHTTP:
		return transport == "tcp" && classifiedProtocol == "http"
	case v1alpha1.EgressAuthProtocolHTTPS:
		return transport == "tcp" && classifiedProtocol == "tls"
	case v1alpha1.EgressAuthProtocolGRPC:
		return transport == "tcp" && classifiedProtocol == "grpc"
	default:
		return false
	}
}

func hasExplicitL4AllowList(policy *CompiledPolicy) bool {
	if policy == nil {
		return false
	}
	return len(policy.Egress.AllowedCIDRs) > 0 || len(policy.Egress.AllowedPorts) > 0
}

func hasExplicitDomainAllowList(policy *CompiledPolicy) bool {
	if policy == nil {
		return false
	}
	if policy.Platform != nil && len(policy.Platform.AllowedDomains) > 0 {
		return true
	}
	return len(policy.Egress.AllowedDomains) > 0
}

func isOtherSandboxPod(platform *PlatformPolicy, destIP net.IP) bool {
	if platform == nil || destIP == nil || len(platform.SandboxPodIPs) == 0 {
		return false
	}
	dest := destIP.String()
	if dest == "" {
		return false
	}
	if dest == platform.SourcePodIP {
		return false
	}
	_, ok := platform.SandboxPodIPs[dest]
	return ok
}

func matchCIDR(ip net.IP, nets []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, network := range nets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func matchPort(port int, protocol string, ranges []PortRange) bool {
	for _, r := range ranges {
		if r.Protocol != "" && r.Protocol != protocol {
			continue
		}
		if int32(port) >= r.Start && int32(port) <= r.End {
			return true
		}
	}
	return false
}

func matchDomain(host string, rules []DomainRule) bool {
	for _, rule := range rules {
		switch rule.Type {
		case DomainMatchExact:
			if host == rule.Pattern {
				return true
			}
		case DomainMatchSuffix:
			if host == rule.Pattern || strings.HasSuffix(host, "."+rule.Pattern) {
				return true
			}
		default:
			if host == rule.Pattern {
				return true
			}
		}
	}
	return false
}
