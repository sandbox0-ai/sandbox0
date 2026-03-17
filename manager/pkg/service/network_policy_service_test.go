package service

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
)

func TestBuildNetworkPolicySpecMergesNamedAuthRulesByName(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	spec := svc.BuildNetworkPolicySpec(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				AuthRules: []v1alpha1.EgressAuthRule{
					{
						Name:     "github-api",
						AuthRef:  "template-ref",
						Protocol: v1alpha1.EgressAuthProtocolHTTPS,
						Domains:  []string{"api.github.com"},
					},
				},
			},
		},
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				AuthRules: []v1alpha1.EgressAuthRule{
					{
						Name:     "github-api",
						AuthRef:  "request-ref",
						Protocol: v1alpha1.EgressAuthProtocolHTTPS,
						Domains:  []string{"uploads.github.com"},
					},
				},
			},
		},
	})

	if spec == nil || spec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(spec.Egress.AuthRules) != 1 {
		t.Fatalf("auth rule count = %d, want 1", len(spec.Egress.AuthRules))
	}
	rule := spec.Egress.AuthRules[0]
	if rule.AuthRef != "request-ref" {
		t.Fatalf("authRef = %q, want request-ref", rule.AuthRef)
	}
	if len(rule.Domains) != 1 || rule.Domains[0] != "uploads.github.com" {
		t.Fatalf("domains = %#v, want request override", rule.Domains)
	}
}

func TestBuildNetworkPolicySpecAppendsUnnamedAuthRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	spec := svc.BuildNetworkPolicySpec(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				AuthRules: []v1alpha1.EgressAuthRule{
					{AuthRef: "template-ref"},
				},
			},
		},
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				AuthRules: []v1alpha1.EgressAuthRule{
					{AuthRef: "request-ref"},
				},
			},
		},
	})

	if spec == nil || spec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(spec.Egress.AuthRules) != 2 {
		t.Fatalf("auth rule count = %d, want 2", len(spec.Egress.AuthRules))
	}
}

func TestBuildNetworkPolicySpecDropsInvalidAuthRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	spec := svc.BuildNetworkPolicySpec(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				AuthRules: []v1alpha1.EgressAuthRule{
					{Name: "missing-auth-ref"},
				},
			},
		},
	})

	if spec == nil || spec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(spec.Egress.AuthRules) != 0 {
		t.Fatalf("auth rules = %#v, want invalid rules dropped", spec.Egress.AuthRules)
	}
}
