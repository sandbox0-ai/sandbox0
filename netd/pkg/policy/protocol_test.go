package policy

import (
	"net/http"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func TestMatchMCPProtocolRule(t *testing.T) {
	compiled, err := CompileNetworkPolicy(&v1alpha1.NetworkPolicySpec{
		Egress: &v1alpha1.NetworkEgressPolicy{
			ProtocolRules: []v1alpha1.ProtocolRule{{
				Name:     "docs-mcp",
				Protocol: v1alpha1.ProtocolRuleProtocolMCP,
				Domains:  []string{"mcp.example.com"},
				Ports:    []v1alpha1.PortSpec{{Port: 443, Protocol: "tcp"}},
				HTTPMatch: &v1alpha1.HTTPMatch{
					Methods:      []string{"POST"},
					PathPrefixes: []string{"/mcp"},
				},
				MCP: &v1alpha1.MCPProtocolRule{Tools: &v1alpha1.MCPToolPolicy{Allowed: []string{"read_file"}}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("compile policy: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, "https://mcp.example.com/mcp/session", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	rule := MatchMCPProtocolRule(compiled, "mcp.example.com", 443, req)
	if rule == nil || rule.Name != "docs-mcp" {
		t.Fatalf("expected docs-mcp rule, got %+v", rule)
	}
	if rule := MatchMCPProtocolRule(compiled, "mcp.example.com", 80, req); rule != nil {
		t.Fatalf("unexpected rule on wrong port: %+v", rule)
	}
}

func TestMatchHTTPProtocolRule(t *testing.T) {
	compiled, err := CompileNetworkPolicy(&v1alpha1.NetworkPolicySpec{
		Egress: &v1alpha1.NetworkEgressPolicy{
			ProtocolRules: []v1alpha1.ProtocolRule{{
				Name:     "api-readonly",
				Protocol: v1alpha1.ProtocolRuleProtocolHTTP,
				Domains:  []string{"api.example.com"},
				Ports:    []v1alpha1.PortSpec{{Port: 80, Protocol: "tcp"}},
				HTTPMatch: &v1alpha1.HTTPMatch{
					PathPrefixes: []string{"/v1"},
				},
				HTTP: &v1alpha1.HTTPProtocolRule{
					Methods: &v1alpha1.HTTPMethodPolicy{Allowed: []string{"GET"}},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("compile policy: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://api.example.com/v1/models", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	rule := MatchHTTPProtocolRule(compiled, "api.example.com", 80, req)
	if rule == nil || rule.Name != "api-readonly" {
		t.Fatalf("expected api-readonly rule, got %+v", rule)
	}
	if rule := MatchHTTPProtocolRule(compiled, "api.example.com", 80, mustHTTPPolicyTestRequest(t, http.MethodGet, "http://api.example.com/v2/models")); rule != nil {
		t.Fatalf("unexpected rule on unmatched httpMatch: %+v", rule)
	}
}

func TestAllowHTTPRequest(t *testing.T) {
	rule := &CompiledProtocolRule{
		HTTP: &CompiledHTTPProtocolRule{
			AllowedMethods:      []string{"GET", "HEAD"},
			DeniedMethods:       []string{"POST"},
			AllowedPathPrefixes: []string{"/v1/read"},
			DeniedPaths:         []string{"/v1/read/private"},
		},
	}
	if ok, reason := AllowHTTPRequest(rule, mustHTTPPolicyTestRequest(t, http.MethodGet, "http://api.example.com/v1/read/files")); !ok || reason != "request_allowed" {
		t.Fatalf("GET /v1/read/files allowed = %v reason = %q", ok, reason)
	}
	if ok, reason := AllowHTTPRequest(rule, mustHTTPPolicyTestRequest(t, http.MethodPost, "http://api.example.com/v1/read/files")); ok || reason != "method_denied" {
		t.Fatalf("POST allowed = %v reason = %q", ok, reason)
	}
	if ok, reason := AllowHTTPRequest(rule, mustHTTPPolicyTestRequest(t, http.MethodDelete, "http://api.example.com/v1/read/files")); ok || reason != "method_not_allowed" {
		t.Fatalf("DELETE allowed = %v reason = %q", ok, reason)
	}
	if ok, reason := AllowHTTPRequest(rule, mustHTTPPolicyTestRequest(t, http.MethodGet, "http://api.example.com/v1/read/private")); ok || reason != "path_denied" {
		t.Fatalf("private path allowed = %v reason = %q", ok, reason)
	}
	if ok, reason := AllowHTTPRequest(rule, mustHTTPPolicyTestRequest(t, http.MethodGet, "http://api.example.com/v1/write/files")); ok || reason != "path_not_allowed" {
		t.Fatalf("write path allowed = %v reason = %q", ok, reason)
	}
}

func TestAllowMCPTool(t *testing.T) {
	rule := &CompiledProtocolRule{
		MCP: &CompiledMCPProtocolRule{
			AllowedTools: []string{"read_file"},
			DeniedTools:  []string{"run_command"},
		},
	}
	if ok, reason := AllowMCPTool(rule, "read_file"); !ok || reason != "tool_allowed" {
		t.Fatalf("read_file allowed = %v reason = %q", ok, reason)
	}
	if ok, reason := AllowMCPTool(rule, "run_command"); ok || reason != "tool_denied" {
		t.Fatalf("run_command allowed = %v reason = %q", ok, reason)
	}
	if ok, reason := AllowMCPTool(rule, "write_file"); ok || reason != "tool_not_allowed" {
		t.Fatalf("write_file allowed = %v reason = %q", ok, reason)
	}
}

func mustHTTPPolicyTestRequest(t *testing.T, method, rawURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return req
}
