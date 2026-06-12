package policy

import (
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func MatchMCPProtocolRule(policy *CompiledPolicy, host string, destPort int, req *http.Request) *CompiledProtocolRule {
	return matchProtocolRule(policy, string(v1alpha1.ProtocolRuleProtocolMCP), host, destPort, req)
}

func MatchHTTPProtocolRule(policy *CompiledPolicy, host string, destPort int, req *http.Request) *CompiledProtocolRule {
	return matchProtocolRule(policy, string(v1alpha1.ProtocolRuleProtocolHTTP), host, destPort, req)
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

func AllowHTTPRequest(rule *CompiledProtocolRule, req *http.Request) (bool, string) {
	if rule == nil || rule.HTTP == nil {
		return true, "no_rule"
	}
	if req == nil {
		return false, "missing_request"
	}
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		return false, "missing_method"
	}
	if matchString(method, rule.HTTP.DeniedMethods) {
		return false, "method_denied"
	}
	if len(rule.HTTP.AllowedMethods) > 0 && !matchString(method, rule.HTTP.AllowedMethods) {
		return false, "method_not_allowed"
	}
	path := "/"
	if req.URL != nil && req.URL.Path != "" {
		path = req.URL.Path
	}
	if matchString(path, rule.HTTP.DeniedPaths) || matchPathPrefix(path, rule.HTTP.DeniedPathPrefixes) {
		return false, "path_denied"
	}
	if hasHTTPPathAllowList(rule.HTTP) && !matchString(path, rule.HTTP.AllowedPaths) && !matchPathPrefix(path, rule.HTTP.AllowedPathPrefixes) {
		return false, "path_not_allowed"
	}
	return true, "request_allowed"
}

func hasHTTPPathAllowList(rule *CompiledHTTPProtocolRule) bool {
	if rule == nil {
		return false
	}
	return len(rule.AllowedPaths) > 0 || len(rule.AllowedPathPrefixes) > 0
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
