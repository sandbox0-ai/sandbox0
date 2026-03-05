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
}
