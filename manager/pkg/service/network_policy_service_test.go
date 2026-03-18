package service

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
)

func testCredentialBinding(ref string) v1alpha1.CredentialBinding {
	return v1alpha1.CredentialBinding{
		Ref:       ref,
		SourceRef: ref + "-source",
		Projection: v1alpha1.ProjectionSpec{
			Type: v1alpha1.CredentialProjectionTypeHTTPHeaders,
			HTTPHeaders: &v1alpha1.HTTPHeadersProjection{
				Headers: []v1alpha1.ProjectedHeader{{
					Name:          "Authorization",
					ValueTemplate: "Bearer {{ .token }}",
				}},
			},
		},
	}
}

func TestBuildNetworkPolicyStateMergesNamedRulesAndBindings(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{
					{
						Name:          "github-api",
						CredentialRef: "template-ref",
						Protocol:      v1alpha1.EgressAuthProtocolHTTPS,
						Domains:       []string{"api.github.com"},
					},
				},
			},
			Credentials: &v1alpha1.NetworkCredentialsSpec{
				Bindings: []v1alpha1.CredentialBinding{
					testCredentialBinding("template-ref"),
				},
			},
		},
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{
					{
						Name:          "github-api",
						CredentialRef: "request-ref",
						Protocol:      v1alpha1.EgressAuthProtocolHTTPS,
						Domains:       []string{"uploads.github.com"},
					},
				},
			},
			Credentials: &v1alpha1.NetworkCredentialsSpec{
				Bindings: []v1alpha1.CredentialBinding{
					testCredentialBinding("request-ref"),
				},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.Rules))
	}
	rule := result.PolicySpec.Egress.Rules[0]
	if rule.CredentialRef != "request-ref" {
		t.Fatalf("credentialRef = %q, want request-ref", rule.CredentialRef)
	}
	if len(rule.Domains) != 1 || rule.Domains[0] != "uploads.github.com" {
		t.Fatalf("domains = %#v, want request override", rule.Domains)
	}
	if len(result.CredentialBindings) != 2 {
		t.Fatalf("binding count = %d, want 2", len(result.CredentialBindings))
	}
}

func TestBuildNetworkPolicyStateAppendsUnnamedRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{
					{CredentialRef: "template-ref"},
				},
			},
			Credentials: &v1alpha1.NetworkCredentialsSpec{
				Bindings: []v1alpha1.CredentialBinding{
					testCredentialBinding("template-ref"),
					testCredentialBinding("request-ref"),
				},
			},
		},
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{
					{CredentialRef: "request-ref"},
				},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 2 {
		t.Fatalf("rule count = %d, want 2", len(result.PolicySpec.Egress.Rules))
	}
}

func TestBuildNetworkPolicyStateDropsInvalidBindingReferences(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{
					{Name: "missing-binding", CredentialRef: "missing"},
				},
			},
			Credentials: &v1alpha1.NetworkCredentialsSpec{
				Bindings: []v1alpha1.CredentialBinding{
					testCredentialBinding("other"),
				},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 0 {
		t.Fatalf("rules = %#v, want invalid rules dropped", result.PolicySpec.Egress.Rules)
	}
	if len(result.CredentialBindings) != 0 {
		t.Fatalf("bindings = %#v, want invalid bindings dropped", result.CredentialBindings)
	}
}
