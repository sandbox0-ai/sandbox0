package policy

import (
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func TestAllowEgressL4AllowAll(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: CompiledRuleSet{
			DeniedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
		},
	}
	if AllowEgressL4(p, net.ParseIP("8.8.8.8"), 443, "tcp") != true {
		t.Fatalf("expected allow")
	}
	if AllowEgressL4(p, net.ParseIP("10.1.0.1"), 443, "tcp") != false {
		t.Fatalf("expected deny for denied cidr")
	}
}

func TestAllowEgressL4BlockAll(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: CompiledRuleSet{
			AllowedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
			AllowedPorts: []PortRange{{Protocol: "tcp", Start: 443, End: 443}},
		},
	}
	if AllowEgressL4(p, net.ParseIP("10.1.0.1"), 443, "tcp") != true {
		t.Fatalf("expected allow")
	}
	if AllowEgressL4(p, net.ParseIP("10.1.0.1"), 80, "tcp") != false {
		t.Fatalf("expected deny due to port")
	}
	if AllowEgressL4(p, net.ParseIP("8.8.8.8"), 443, "tcp") != false {
		t.Fatalf("expected deny due to cidr")
	}
}

func TestAllowEgressL4BlockAllNoAllowList(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
	}
	if AllowEgressL4(p, net.ParseIP("8.8.8.8"), 443, "tcp") != true {
		t.Fatalf("expected allow without L4 allow list")
	}
}

func TestAllowEgressDomain(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: CompiledRuleSet{
			AllowedDomains: []DomainRule{{Pattern: "example.com", Type: DomainMatchExact}},
			DeniedDomains:  []DomainRule{{Pattern: "blocked.example.com", Type: DomainMatchExact}},
		},
	}
	if AllowEgressDomain(p, "example.com") != true {
		t.Fatalf("expected allow")
	}
	if AllowEgressDomain(p, "other.com") != false {
		t.Fatalf("expected deny due to allow list")
	}
}

func TestAllowEgressDomainBlockAllNoAllowList(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
	}
	if AllowEgressDomain(p, "example.com") != false {
		t.Fatalf("expected deny without allow list")
	}
}

func TestAllowEgressDomainAllowAllDeniedList(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Egress: CompiledRuleSet{
			DeniedDomains: []DomainRule{{Pattern: "blocked.example.com", Type: DomainMatchExact}},
		},
	}
	if AllowEgressDomain(p, "blocked.example.com") != false {
		t.Fatalf("expected deny due to deny list")
	}
	if AllowEgressDomain(p, "example.com") != true {
		t.Fatalf("expected allow without deny match")
	}
}

func TestPlatformAllowOverridesUserDeny(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: CompiledRuleSet{
			DeniedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
		},
		Platform: &PlatformPolicy{
			AllowedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
		},
	}
	if AllowEgressL4(p, net.ParseIP("10.1.0.1"), 443, "tcp") != true {
		t.Fatalf("expected platform allow to override user deny")
	}
}

func TestPlatformDenyOverridesUserAllow(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: CompiledRuleSet{
			AllowedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
		},
		Platform: &PlatformPolicy{
			DeniedCIDRs: []*net.IPNet{mustCIDR("10.0.0.0/8")},
		},
	}
	if AllowEgressL4(p, net.ParseIP("10.1.0.1"), 443, "tcp") != false {
		t.Fatalf("expected platform deny to override user allow")
	}
}

func TestSandboxPodDenyBlocksPeerButNotSelf(t *testing.T) {
	p := &CompiledPolicy{
		Mode: v1alpha1.NetworkModeAllowAll,
		Platform: &PlatformPolicy{
			SandboxPodIPs: map[string]struct{}{
				"10.0.0.2": {},
				"10.0.0.3": {},
			},
			SourcePodIP: "10.0.0.2",
		},
	}
	if AllowEgressL4(p, net.ParseIP("10.0.0.3"), 443, "tcp") != false {
		t.Fatalf("expected peer sandbox pod to be denied")
	}
	if AllowEgressL4(p, net.ParseIP("10.0.0.2"), 443, "tcp") != true {
		t.Fatalf("expected self sandbox pod ip to be allowed")
	}
	if AllowEgressL4(p, net.ParseIP("8.8.8.8"), 443, "tcp") != true {
		t.Fatalf("expected non-sandbox ip to be allowed")
	}
}

func TestHasDomainRules(t *testing.T) {
	if HasDomainRules(nil) {
		t.Fatalf("expected false for nil policy")
	}
	p := &CompiledPolicy{
		Egress: CompiledRuleSet{
			AllowedDomains: []DomainRule{{Pattern: "example.com", Type: DomainMatchExact}},
		},
	}
	if !HasDomainRules(p) {
		t.Fatalf("expected true for policy with domains")
	}
}

func mustCIDR(cidr string) *net.IPNet {
	_, netCIDR, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	return netCIDR
}
