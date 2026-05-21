package policy

import (
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func MatchMCPProtocolRule(policy *CompiledPolicy, host string, destPort int, req *http.Request) *CompiledProtocolRule {
	return matchProtocolRule(policy, string(v1alpha1.ProtocolRuleProtocolMCP), host, destPort, req)
}

func HasProtocolRules(policy *CompiledPolicy, protocol string) bool {
	if policy == nil {
		return false
	}
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		return false
	}
	for _, rule := range policy.Egress.ProtocolRules {
		if rule.Protocol == protocol {
			return true
		}
	}
	return false
}

func RequiresProtocolTLSTermination(policy *CompiledPolicy, host string, destPort int, protocol string) bool {
	if policy == nil {
		return false
	}
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		return false
	}
	for idx := range policy.Egress.ProtocolRules {
		rule := &policy.Egress.ProtocolRules[idx]
		if rule.Protocol != protocol {
			continue
		}
		if rule.TLSMode != v1alpha1.EgressTLSModeTerminateReoriginate {
			continue
		}
		if !matchProtocolRuleDestination(rule, host, destPort) {
			continue
		}
		return true
	}
	return false
}

func AllowMCPTool(rule *CompiledProtocolRule, name string) (bool, string) {
	if rule == nil || rule.MCP == nil {
		return true, "no_rule"
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false, "missing_tool_name"
	}
	if matchString(name, rule.MCP.DeniedTools) {
		return false, "tool_denied"
	}
	if len(rule.MCP.AllowedTools) > 0 && !matchString(name, rule.MCP.AllowedTools) {
		return false, "tool_not_allowed"
	}
	return true, "tool_allowed"
}

func matchProtocolRule(policy *CompiledPolicy, protocol string, host string, destPort int, req *http.Request) *CompiledProtocolRule {
	if policy == nil {
		return nil
	}
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	for idx := range policy.Egress.ProtocolRules {
		rule := &policy.Egress.ProtocolRules[idx]
		if rule.Protocol != protocol {
			continue
		}
		if !matchProtocolRuleDestination(rule, host, destPort) {
			continue
		}
		if !MatchHTTPRequest(rule.HTTPMatch, req) {
			continue
		}
		return rule
	}
	return nil
}

func matchProtocolRuleDestination(rule *CompiledProtocolRule, host string, destPort int) bool {
	if rule == nil {
		return false
	}
	host = strings.ToLower(strings.TrimSpace(host))
	if len(rule.Domains) > 0 {
		if host == "" || !matchDomain(host, rule.Domains) {
			return false
		}
	}
	if len(rule.Ports) > 0 && !matchPort(destPort, "tcp", rule.Ports) {
		return false
	}
	return true
}
