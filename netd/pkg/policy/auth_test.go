package policy

import (
	"net"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func TestMatchEgressAuthRuleMatchesHTTPRule(t *testing.T) {
	p := &CompiledPolicy{
		Egress: CompiledRuleSet{
			AuthRules: []CompiledEgressAuthRule{
				{
					Name:     "example-http",
					AuthRef:  "example-api",
					Protocol: v1alpha1.EgressAuthProtocolHTTP,
					Domains: []DomainRule{
						{Pattern: "api.example.com", Type: DomainMatchExact},
					},
					Ports: []PortRange{
						{Protocol: "tcp", Start: 80, End: 80},
					},
				},
			},
		},
	}

	rule := MatchEgressAuthRule(p, "tcp", "http", 80, "api.example.com")
	if rule == nil {
		t.Fatal("expected auth rule match")
	}
	if rule.AuthRef != "example-api" {
		t.Fatalf("unexpected auth ref %q", rule.AuthRef)
	}
}

func TestMatchEgressAuthRuleSkipsTLSRuleForHTTPClassifier(t *testing.T) {
	p := &CompiledPolicy{
		Egress: CompiledRuleSet{
			AuthRules: []CompiledEgressAuthRule{
				{
					Name:     "example-https",
					AuthRef:  "example-api",
					Protocol: v1alpha1.EgressAuthProtocolHTTPS,
					Domains: []DomainRule{
						{Pattern: "api.example.com", Type: DomainMatchExact},
					},
				},
			},
		},
	}

	if rule := MatchEgressAuthRule(p, "tcp", "http", 443, "api.example.com"); rule != nil {
		t.Fatalf("expected no auth rule, got %+v", rule)
	}
}

func TestMatchEgressAuthRuleMatchesGRPCRuleForTLSClassifier(t *testing.T) {
	p := &CompiledPolicy{
		Egress: CompiledRuleSet{
			AuthRules: []CompiledEgressAuthRule{
				{
					Name:     "example-grpc",
					AuthRef:  "example-api",
					Protocol: v1alpha1.EgressAuthProtocolGRPC,
					TLSMode:  v1alpha1.EgressTLSModeTerminateReoriginate,
					Domains: []DomainRule{
						{Pattern: "api.example.com", Type: DomainMatchExact},
					},
					Ports: []PortRange{
						{Protocol: "tcp", Start: 443, End: 443},
					},
				},
			},
		},
	}

	rule := MatchEgressAuthRule(p, "tcp", "tls", 443, "api.example.com")
	if rule == nil {
		t.Fatal("expected grpc auth rule match for tls classifier")
	}
	if rule.AuthRef != "example-api" {
		t.Fatalf("unexpected auth ref %q", rule.AuthRef)
	}
}

func TestMatchEgressAuthRuleSkipsDisabledRolloutRule(t *testing.T) {
	p := &CompiledPolicy{
		Egress: CompiledRuleSet{
			AuthRules: []CompiledEgressAuthRule{
				{
					Name:     "example-http",
					AuthRef:  "example-api",
					Rollout:  v1alpha1.EgressAuthRolloutDisabled,
					Protocol: v1alpha1.EgressAuthProtocolHTTP,
					Domains: []DomainRule{
						{Pattern: "api.example.com", Type: DomainMatchExact},
					},
				},
			},
		},
	}

	if rule := MatchEgressAuthRule(p, "tcp", "http", 80, "api.example.com"); rule != nil {
		t.Fatalf("expected disabled rollout rule to be ignored, got %+v", rule)
	}
}

func TestCloneRuleSetCopiesAuthRules(t *testing.T) {
	in := CompiledRuleSet{
		AuthRules: []CompiledEgressAuthRule{
			{Name: "example", AuthRef: "ref"},
		},
		AllowedCIDRs: []*net.IPNet{mustTestCIDR("10.0.0.0/24")},
	}

	out := cloneRuleSet(in)
	out.AuthRules[0].AuthRef = "other"

	if in.AuthRules[0].AuthRef != "ref" {
		t.Fatalf("expected auth rules clone to be independent")
	}
}

func mustTestCIDR(value string) *net.IPNet {
	_, cidr, err := net.ParseCIDR(value)
	if err != nil {
		panic(err)
	}
	return cidr
}
