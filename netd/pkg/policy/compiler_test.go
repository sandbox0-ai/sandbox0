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
			Rules: []v1alpha1.EgressCredentialRule{
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
	if len(compiled.Egress.AllowedCIDRs) != 1 {
		t.Fatalf("expected allowed cidrs")
	}
	if len(compiled.Egress.DeniedCIDRs) != 1 {
		t.Fatalf("expected denied cidrs")
	}
	if len(compiled.Egress.AllowedDomains) != 2 {
		t.Fatalf("expected allowed domains")
	}
	if len(compiled.Egress.AllowedPorts) != 1 {
		t.Fatalf("expected allowed ports")
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
