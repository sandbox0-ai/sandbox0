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

func testTLSCredentialBinding(ref string) v1alpha1.CredentialBinding {
	return v1alpha1.CredentialBinding{
		Ref:       ref,
		SourceRef: ref + "-source",
		Projection: v1alpha1.ProjectionSpec{
			Type:                 v1alpha1.CredentialProjectionTypeTLSClientCertificate,
			TLSClientCertificate: &v1alpha1.TLSClientCertificateProjection{},
		},
	}
}

func testUsernamePasswordCredentialBinding(ref string) v1alpha1.CredentialBinding {
	return v1alpha1.CredentialBinding{
		Ref:       ref,
		SourceRef: ref + "-source",
		Projection: v1alpha1.ProjectionSpec{
			Type:             v1alpha1.CredentialProjectionTypeUsernamePassword,
			UsernamePassword: &v1alpha1.UsernamePasswordProjection{},
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
		},
		TemplateBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("template-ref"),
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
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("request-ref"),
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
		},
		TemplateBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("template-ref"),
			testCredentialBinding("request-ref"),
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
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("other"),
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

func TestBuildNetworkPolicyStateDropsTLSRulesWithoutTLSProjection(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{{
					Name:          "db-mtls",
					CredentialRef: "db-cert",
					Protocol:      v1alpha1.EgressAuthProtocolTLS,
					TLSMode:       v1alpha1.EgressTLSModeTerminateReoriginate,
					Domains:       []string{"db.example.com"},
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("db-cert"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 0 {
		t.Fatalf("rules = %#v, want invalid tls rules dropped", result.PolicySpec.Egress.Rules)
	}
	if len(result.CredentialBindings) != 0 {
		t.Fatalf("bindings = %#v, want invalid bindings dropped with tls rule", result.CredentialBindings)
	}
}

func TestBuildNetworkPolicyStateKeepsValidTLSRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{{
					Name:          "db-mtls",
					CredentialRef: "db-cert",
					Protocol:      v1alpha1.EgressAuthProtocolTLS,
					TLSMode:       v1alpha1.EgressTLSModeTerminateReoriginate,
					Domains:       []string{"db.example.com"},
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testTLSCredentialBinding("db-cert"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.Rules))
	}
}

func TestBuildNetworkPolicyStateDropsSOCKS5RulesWithoutUsernamePasswordProjection(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{{
					Name:          "corp-socks",
					CredentialRef: "proxy-cred",
					Protocol:      v1alpha1.EgressAuthProtocolSOCKS5,
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("proxy-cred"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 0 {
		t.Fatalf("rules = %#v, want invalid socks5 rules dropped", result.PolicySpec.Egress.Rules)
	}
	if len(result.CredentialBindings) != 0 {
		t.Fatalf("bindings = %#v, want invalid bindings dropped with socks5 rule", result.CredentialBindings)
	}
}

func TestBuildNetworkPolicyStateKeepsValidMQTTRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{{
					Name:          "broker-auth",
					CredentialRef: "mqtt-cred",
					Protocol:      v1alpha1.EgressAuthProtocolMQTT,
					Domains:       []string{"broker.example.com"},
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testUsernamePasswordCredentialBinding("mqtt-cred"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.Rules))
	}
}

func TestBuildNetworkPolicyStateKeepsValidRedisRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{{
					Name:          "redis-auth",
					CredentialRef: "redis-cred",
					Protocol:      v1alpha1.EgressAuthProtocolRedis,
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testUsernamePasswordCredentialBinding("redis-cred"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.Rules))
	}
}

func TestBuildNetworkPolicyStateKeepsValidPostgresRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.TplSandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Rules: []v1alpha1.EgressCredentialRule{{
					Name:          "db-auth",
					CredentialRef: "db-cred",
					Protocol:      v1alpha1.EgressAuthProtocolPostgres,
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testUsernamePasswordCredentialBinding("db-cred"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.Rules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.Rules))
	}
}
