package policy

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func TestCompileNetworkPolicy(t *testing.T) {
	spec := &v1alpha1.NetworkPolicySpec{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			AllowedCIDRs:   []string{"10.0.0.0/24"},
			DeniedCIDRs:    []string{"10.0.0.5/32"},
			AllowedDomains: []string{"example.com", "*.example.org"},
			DeniedDomains:  []string{"blocked.example.com"},
			AllowedPorts: []v1alpha1.PortSpec{
				{Port: 80, Protocol: "tcp"},
			},
			CredentialRules: []v1alpha1.EgressCredentialRule{
				{
					Name:          "example-http",
					CredentialRef: "example-api",
					Protocol:      v1alpha1.EgressAuthProtocolHTTP,
					Domains:       []string{"api.example.com"},
					Ports: []v1alpha1.PortSpec{
						{Port: 80, Protocol: "tcp"},
					},
				},
				{
					Name:          "example-mtls",
					CredentialRef: "example-cert",
					Protocol:      v1alpha1.EgressAuthProtocolTLS,
					TLSMode:       v1alpha1.EgressTLSModeTerminateReoriginate,
					Domains:       []string{"db.example.com"},
					Ports: []v1alpha1.PortSpec{
						{Port: 5432, Protocol: "tcp"},
					},
				},
				{
					Name:          "corp-socks",
					CredentialRef: "proxy-cred",
					Protocol:      v1alpha1.EgressAuthProtocolSOCKS5,
					Ports: []v1alpha1.PortSpec{
						{Port: 1080, Protocol: "tcp"},
					},
				},
				{
					Name:          "broker-auth",
					CredentialRef: "mqtt-cred",
					Protocol:      v1alpha1.EgressAuthProtocolMQTT,
					Domains:       []string{"broker.example.com"},
				},
				{
					Name:          "redis-auth",
					CredentialRef: "redis-cred",
					Protocol:      v1alpha1.EgressAuthProtocolRedis,
					Ports: []v1alpha1.PortSpec{
						{Port: 6379, Protocol: "tcp"},
					},
				},
			},
		},
	}

	compiled, err := CompileNetworkPolicy(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if compiled.Mode != v1alpha1.NetworkModeBlockAll {
		t.Fatalf("unexpected mode: %v", compiled.Mode)
	}
	if len(compiled.Egress.TrafficRules) != 1 {
		t.Fatalf("expected one normalized traffic rule, got %d", len(compiled.Egress.TrafficRules))
	}
	if compiled.Egress.TrafficRules[0].Action != v1alpha1.TrafficRuleActionAllow {
		t.Fatalf("unexpected traffic rule action: %s", compiled.Egress.TrafficRules[0].Action)
	}
	if len(compiled.Egress.TrafficRules[0].CIDRs) != 1 {
		t.Fatalf("expected normalized allowed cidrs")
	}
	if len(compiled.Egress.TrafficRules[0].Domains) != 2 {
		t.Fatalf("expected normalized allowed domains")
	}
	if len(compiled.Egress.TrafficRules[0].Ports) != 1 {
		t.Fatalf("expected normalized allowed ports")
	}
	if len(compiled.Egress.AuthRules) != 5 {
		t.Fatalf("expected auth rules")
	}
	if compiled.Egress.AuthRules[2].Protocol != v1alpha1.EgressAuthProtocolSOCKS5 {
		t.Fatalf("unexpected third auth rule protocol: %s", compiled.Egress.AuthRules[2].Protocol)
	}
	if compiled.Egress.AuthRules[3].Protocol != v1alpha1.EgressAuthProtocolMQTT {
		t.Fatalf("unexpected fourth auth rule protocol: %s", compiled.Egress.AuthRules[3].Protocol)
	}
	if compiled.Egress.AuthRules[4].Protocol != v1alpha1.EgressAuthProtocolRedis {
		t.Fatalf("unexpected fifth auth rule protocol: %s", compiled.Egress.AuthRules[4].Protocol)
	}
}

func TestCompileNetworkPolicyTrafficRules(t *testing.T) {
	spec := &v1alpha1.NetworkPolicySpec{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			TrafficRules: []v1alpha1.TrafficRule{
				{
					Name:    "allow-github-https",
					Action:  v1alpha1.TrafficRuleActionAllow,
					Domains: []string{"github.com"},
					Ports:   []v1alpha1.PortSpec{{Port: 443, Protocol: "tcp"}},
				},
				{
					Name:         "deny-private",
					Action:       v1alpha1.TrafficRuleActionDeny,
					CIDRs:        []string{"10.0.0.0/8"},
					AppProtocols: []v1alpha1.TrafficRuleAppProtocol{v1alpha1.TrafficRuleAppProtocolSSH},
				},
			},
		},
	}

	compiled, err := CompileNetworkPolicy(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(compiled.Egress.TrafficRules) != 2 {
		t.Fatalf("traffic rule count = %d, want 2", len(compiled.Egress.TrafficRules))
	}
	if compiled.Egress.TrafficRules[0].Action != v1alpha1.TrafficRuleActionAllow {
		t.Fatalf("unexpected first traffic rule action: %s", compiled.Egress.TrafficRules[0].Action)
	}
	if len(compiled.Egress.TrafficRules[0].Domains) != 1 {
		t.Fatalf("expected compiled traffic rule domains")
	}
	if len(compiled.Egress.TrafficRules[1].AppProtocols) != 1 || compiled.Egress.TrafficRules[1].AppProtocols[0] != "ssh" {
		t.Fatalf("expected compiled traffic rule app protocols, got %#v", compiled.Egress.TrafficRules[1].AppProtocols)
	}
}

func TestCompileNetworkPolicyLegacyAllowAllNormalizesToDenyTrafficRules(t *testing.T) {
	spec := &v1alpha1.NetworkPolicySpec{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			DeniedCIDRs:   []string{"10.0.0.0/8"},
			DeniedDomains: []string{"blocked.example.com"},
			DeniedPorts:   []v1alpha1.PortSpec{{Port: 25, Protocol: "tcp"}},
		},
	}

	compiled, err := CompileNetworkPolicy(spec)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(compiled.Egress.TrafficRules) != 3 {
		t.Fatalf("traffic rule count = %d, want 3", len(compiled.Egress.TrafficRules))
	}
	for _, rule := range compiled.Egress.TrafficRules {
		if rule.Action != v1alpha1.TrafficRuleActionDeny {
			t.Fatalf("unexpected traffic rule action: %s", rule.Action)
		}
	}
}

func TestCompileNetworkPolicyRejectsMixedLegacyAndTrafficRules(t *testing.T) {
	spec := &v1alpha1.NetworkPolicySpec{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			DeniedCIDRs: []string{"10.0.0.0/8"},
			TrafficRules: []v1alpha1.TrafficRule{{
				Action: v1alpha1.TrafficRuleActionDeny,
				CIDRs:  []string{"192.168.0.0/16"},
			}},
		},
	}

	if _, err := CompileNetworkPolicy(spec); err == nil {
		t.Fatal("expected mixed legacy and traffic rules to fail")
	}
}

func TestCompileNetworkPolicyRejectsUnsupportedTrafficRuleAppProtocol(t *testing.T) {
	spec := &v1alpha1.NetworkPolicySpec{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			TrafficRules: []v1alpha1.TrafficRule{{
				Action:       v1alpha1.TrafficRuleActionAllow,
				AppProtocols: []v1alpha1.TrafficRuleAppProtocol{"scp"},
			}},
		},
	}

	if _, err := CompileNetworkPolicy(spec); err == nil {
		t.Fatal("expected unsupported app protocol to fail")
	}
}
