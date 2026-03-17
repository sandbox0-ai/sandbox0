package proxy

import (
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

func TestDecideTrafficKnownAllowedUsesAdapter(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			AuthRules: []policy.CompiledEgressAuthRule{
				{
					Name:     "example-http",
					AuthRef:  "example-api",
					Protocol: v1alpha1.EgressAuthProtocolHTTP,
					Domains: []policy.DomainRule{
						{Pattern: "example.com", Type: policy.DomainMatchExact},
					},
				},
			},
		},
	}
	decision := decideTraffic(compiled, classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com"))
	if decision.Action != decisionActionUseAdapter {
		t.Fatalf("expected use-adapter, got %s", decision.Action)
	}
	if !decision.NeedsAdapter {
		t.Fatalf("expected adapter to be required")
	}
	if decision.Reason != "allowed" {
		t.Fatalf("expected allowed reason, got %s", decision.Reason)
	}
	if !decision.NeedsEgressAuth || decision.MatchedAuthRule == nil {
		t.Fatalf("expected matched auth rule")
	}
	if decision.MatchedAuthRule.AuthRef != "example-api" {
		t.Fatalf("unexpected auth ref %q", decision.MatchedAuthRule.AuthRef)
	}
}

func TestDecideTrafficL4Denied(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
		},
	}
	decision := decideTraffic(compiled, classifyKnownTraffic("tcp", "http", net.ParseIP("10.1.0.1"), 443, "example.com"))
	if decision.Action != decisionActionDeny {
		t.Fatalf("expected deny, got %s", decision.Action)
	}
	if decision.Reason != "l4_denied" {
		t.Fatalf("expected l4_denied reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficL7Denied(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			DeniedDomains: []policy.DomainRule{{Pattern: "blocked.example.com", Type: policy.DomainMatchExact}},
		},
	}
	decision := decideTraffic(compiled, classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "blocked.example.com"))
	if decision.Action != decisionActionDeny {
		t.Fatalf("expected deny, got %s", decision.Action)
	}
	if decision.Reason != "l7_denied" {
		t.Fatalf("expected l7_denied reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficBlockAllDomainOnlyAllowedUsesAdapter(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: policy.CompiledRuleSet{
			AllowedDomains: []policy.DomainRule{{Pattern: "example.com", Type: policy.DomainMatchExact}},
		},
	}
	decision := decideTraffic(compiled, classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com"))
	if decision.Action != decisionActionUseAdapter {
		t.Fatalf("expected use-adapter, got %s", decision.Action)
	}
	if decision.Reason != "allowed" {
		t.Fatalf("expected allowed reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficUnknownAllowAllPassesThrough(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
	}
	decision := decideTraffic(compiled, classifyUnknownTraffic("tcp", "tls", net.ParseIP("8.8.8.8"), 443, "parse_failed"))
	if decision.Action != decisionActionPassThrough {
		t.Fatalf("expected pass-through, got %s", decision.Action)
	}
	if decision.Reason != "parse_failed" {
		t.Fatalf("expected parse_failed reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficUnknownBlockAllDenies(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: policy.CompiledRuleSet{
			AllowedCIDRs: []*net.IPNet{mustCIDR("8.8.8.0/24")},
			AllowedPorts: []policy.PortRange{{Protocol: "tcp", Start: 443, End: 443}},
		},
	}
	decision := decideTraffic(compiled, classifyUnknownTraffic("tcp", "tls", net.ParseIP("8.8.8.8"), 443, "missing_sni"))
	if decision.Action != decisionActionDeny {
		t.Fatalf("expected deny, got %s", decision.Action)
	}
	if decision.Reason != "missing_sni" {
		t.Fatalf("expected missing_sni reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficUnknownBlockAllPlatformDestinationPassesThrough(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Platform: &policy.PlatformPolicy{
			AllowedCIDRs: []*net.IPNet{mustCIDR("10.96.0.10/32")},
		},
	}
	decision := decideTraffic(compiled, classifyUnknownTraffic("tcp", "opaque", net.ParseIP("10.96.0.10"), 8090, "unclassified"))
	if decision.Action != decisionActionPassThrough {
		t.Fatalf("expected pass-through, got %s", decision.Action)
	}
	if decision.Reason != "unclassified" {
		t.Fatalf("expected unclassified reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficUnknownAllowAllPlatformDeniedStillDenies(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Platform: &policy.PlatformPolicy{
			DeniedCIDRs: []*net.IPNet{mustCIDR("10.96.0.10/32")},
		},
	}
	decision := decideTraffic(compiled, classifyUnknownTraffic("tcp", "opaque", net.ParseIP("10.96.0.10"), 8090, "unclassified"))
	if decision.Action != decisionActionDeny {
		t.Fatalf("expected deny, got %s", decision.Action)
	}
	if decision.Reason != "l4_denied" {
		t.Fatalf("expected l4_denied reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficUDPAllowedUsesAdapter(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: policy.CompiledRuleSet{
			AllowedCIDRs: []*net.IPNet{mustCIDR("8.8.8.0/24")},
			AllowedPorts: []policy.PortRange{{Protocol: "udp", Start: 443, End: 443}},
		},
	}
	decision := decideTraffic(compiled, classifyKnownTraffic("udp", "udp", net.ParseIP("8.8.8.8"), 443, ""))
	if decision.Action != decisionActionUseAdapter {
		t.Fatalf("expected use-adapter, got %s", decision.Action)
	}
}

func TestDecideTrafficBlockAllL4AllowWithoutDomainRulesUsesAdapter(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: policy.CompiledRuleSet{
			AllowedCIDRs: []*net.IPNet{mustCIDR("8.8.8.0/24")},
			AllowedPorts: []policy.PortRange{{Protocol: "tcp", Start: 443, End: 443}},
		},
	}
	decision := decideTraffic(compiled, classifyKnownTraffic("tcp", "http", net.ParseIP("8.8.8.8"), 443, "example.com"))
	if decision.Action != decisionActionUseAdapter {
		t.Fatalf("expected use-adapter, got %s", decision.Action)
	}
	if decision.Reason != "allowed" {
		t.Fatalf("expected allowed reason, got %s", decision.Reason)
	}
}

func TestDecideTrafficUnknownTrafficDoesNotMatchAuthRule(t *testing.T) {
	compiled := &policy.CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: policy.CompiledRuleSet{
			AuthRules: []policy.CompiledEgressAuthRule{
				{
					Name:     "example-http",
					AuthRef:  "example-api",
					Protocol: v1alpha1.EgressAuthProtocolHTTP,
				},
			},
		},
	}

	decision := decideTraffic(compiled, classifyUnknownTraffic("tcp", "tls", net.ParseIP("8.8.8.8"), 443, "parse_failed"))
	if decision.NeedsEgressAuth {
		t.Fatalf("expected unknown traffic to skip auth matching")
	}
	if decision.MatchedAuthRule != nil {
		t.Fatalf("expected no matched auth rule, got %+v", decision.MatchedAuthRule)
	}
}

func mustCIDR(cidr string) *net.IPNet {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	return network
}
