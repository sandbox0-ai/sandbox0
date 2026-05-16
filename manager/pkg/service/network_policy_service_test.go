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

func testSSHProxyCredentialBinding(ref string) v1alpha1.CredentialBinding {
	return v1alpha1.CredentialBinding{
		Ref:       ref,
		SourceRef: ref + "-source",
		Projection: v1alpha1.ProjectionSpec{
			Type: v1alpha1.CredentialProjectionTypeSSHProxy,
			SSHProxy: &v1alpha1.SSHProxyProjection{
				SandboxPublicKeys: []string{"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID////////////////////////////////////////// fake"},
				UpstreamUsername:  "git",
			},
		},
	}
}

func TestBuildNetworkPolicyStateMergesNamedRulesAndBindings(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{
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
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.CredentialRules))
	}
	rule := result.PolicySpec.Egress.CredentialRules[0]
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

func TestBuildNetworkPolicyStateMergesEgressProxy(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				Proxy: &v1alpha1.EgressProxyPolicy{
					Type:    v1alpha1.EgressProxyTypeSOCKS5,
					Address: "template-proxy.example.com:1080",
				},
			},
		},
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				Proxy: &v1alpha1.EgressProxyPolicy{
					Type:          v1alpha1.EgressProxyTypeSOCKS5,
					Address:       "request-proxy.example.com:1080",
					CredentialRef: "corp-proxy",
				},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testUsernamePasswordCredentialBinding("corp-proxy"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil || result.PolicySpec.Egress.Proxy == nil {
		t.Fatalf("expected egress proxy")
	}
	if result.PolicySpec.Egress.Proxy.Address != "request-proxy.example.com:1080" {
		t.Fatalf("proxy address = %q, want request override", result.PolicySpec.Egress.Proxy.Address)
	}
	if result.PolicySpec.Egress.Proxy.CredentialRef != "corp-proxy" {
		t.Fatalf("credentialRef = %q, want corp-proxy", result.PolicySpec.Egress.Proxy.CredentialRef)
	}
}

func TestBuildNetworkPolicyStateAppendsUnnamedRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{
					{CredentialRef: "template-ref"},
				},
			},
		},
		TemplateBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("template-ref"),
			testCredentialBinding("request-ref"),
		},
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{
					{CredentialRef: "request-ref"},
				},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.CredentialRules) != 2 {
		t.Fatalf("rule count = %d, want 2", len(result.PolicySpec.Egress.CredentialRules))
	}
}

func TestBuildNetworkPolicyStateMergesNamedTrafficRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		TemplateSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				TrafficRules: []v1alpha1.TrafficRule{{
					Name:    "github",
					Action:  v1alpha1.TrafficRuleActionAllow,
					Domains: []string{"github.com"},
				}},
			},
		},
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				TrafficRules: []v1alpha1.TrafficRule{{
					Name:    "github",
					Action:  v1alpha1.TrafficRuleActionDeny,
					Domains: []string{"github.com"},
				}},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.TrafficRules) != 1 {
		t.Fatalf("traffic rule count = %d, want 1", len(result.PolicySpec.Egress.TrafficRules))
	}
	if result.PolicySpec.Egress.TrafficRules[0].Action != v1alpha1.TrafficRuleActionDeny {
		t.Fatalf("unexpected merged traffic rule action: %s", result.PolicySpec.Egress.TrafficRules[0].Action)
	}
}

func TestBuildNetworkPolicyStateRejectsMixedLegacyAndTrafficRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				AllowedDomains: []string{"example.com"},
				TrafficRules: []v1alpha1.TrafficRule{{
					Action:  v1alpha1.TrafficRuleActionAllow,
					Domains: []string{"github.com"},
				}},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.TrafficRules) != 0 {
		t.Fatalf("expected invalid traffic rules to be dropped, got %#v", result.PolicySpec.Egress.TrafficRules)
	}
}

func TestBuildNetworkPolicyStateRejectsUnsupportedTrafficRuleAppProtocol(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Egress: &v1alpha1.NetworkEgressPolicy{
				TrafficRules: []v1alpha1.TrafficRule{{
					Action:       v1alpha1.TrafficRuleActionAllow,
					AppProtocols: []v1alpha1.TrafficRuleAppProtocol{"scp"},
				}},
			},
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.TrafficRules) != 0 {
		t.Fatalf("expected invalid traffic rules to be dropped, got %#v", result.PolicySpec.Egress.TrafficRules)
	}
}

func TestBuildNetworkPolicyStateDropsInvalidBindingReferences(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 0 {
		t.Fatalf("rules = %#v, want invalid rules dropped", result.PolicySpec.Egress.CredentialRules)
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
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 0 {
		t.Fatalf("rules = %#v, want invalid tls rules dropped", result.PolicySpec.Egress.CredentialRules)
	}
	if len(result.CredentialBindings) != 0 {
		t.Fatalf("bindings = %#v, want invalid bindings dropped with tls rule", result.CredentialBindings)
	}
}

func TestBuildNetworkPolicyStateKeepsHTTPMatchForHTTPSRuleWithTLSInterception(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
					Name:          "github-write",
					CredentialRef: "github-token",
					Protocol:      v1alpha1.EgressAuthProtocolHTTPS,
					TLSMode:       v1alpha1.EgressTLSModeTerminateReoriginate,
					Domains:       []string{"api.github.com"},
					HTTPMatch: &v1alpha1.HTTPMatch{
						Methods:      []string{"POST"},
						PathPrefixes: []string{"/repos/"},
						Headers: []v1alpha1.HTTPValueMatch{{
							Name:   "accept",
							Values: []string{"application/vnd.github+json"},
						}},
					},
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("github-token"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.CredentialRules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.CredentialRules))
	}
	if result.PolicySpec.Egress.CredentialRules[0].HTTPMatch == nil {
		t.Fatal("expected httpMatch to be preserved")
	}
}

func TestBuildNetworkPolicyStateDropsHTTPMatchForHTTPSRuleWithoutTLSInterception(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
					Name:          "github-write",
					CredentialRef: "github-token",
					Protocol:      v1alpha1.EgressAuthProtocolHTTPS,
					Domains:       []string{"api.github.com"},
					HTTPMatch: &v1alpha1.HTTPMatch{
						Methods: []string{"POST"},
					},
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("github-token"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.CredentialRules) != 0 {
		t.Fatalf("rules = %#v, want invalid httpMatch rules dropped", result.PolicySpec.Egress.CredentialRules)
	}
	if len(result.CredentialBindings) != 0 {
		t.Fatalf("bindings = %#v, want invalid bindings dropped with httpMatch rule", result.CredentialBindings)
	}
}

func TestBuildNetworkPolicyStateKeepsValidTLSRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.CredentialRules))
	}
}

func TestBuildNetworkPolicyStateDropsSOCKS5RulesWithoutUsernamePasswordProjection(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 0 {
		t.Fatalf("rules = %#v, want invalid socks5 rules dropped", result.PolicySpec.Egress.CredentialRules)
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
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.CredentialRules))
	}
}

func TestBuildNetworkPolicyStateKeepsValidRedisRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
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
	if len(result.PolicySpec.Egress.CredentialRules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.CredentialRules))
	}
}

func TestBuildNetworkPolicyStateDropsSSHRulesWithoutSSHProxyProjection(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
					Name:          "git-ssh",
					CredentialRef: "git-cred",
					Protocol:      v1alpha1.EgressAuthProtocolSSH,
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testCredentialBinding("git-cred"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.CredentialRules) != 0 {
		t.Fatalf("rules = %#v, want invalid ssh rules dropped", result.PolicySpec.Egress.CredentialRules)
	}
	if len(result.CredentialBindings) != 0 {
		t.Fatalf("bindings = %#v, want invalid bindings dropped with ssh rule", result.CredentialBindings)
	}
}

func TestBuildNetworkPolicyStateKeepsValidSSHRules(t *testing.T) {
	svc := NewNetworkPolicyService(zap.NewNop())
	result := svc.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID: "sb-1",
		TeamID:    "team-1",
		RequestSpec: &v1alpha1.SandboxNetworkPolicy{
			Mode: v1alpha1.NetworkModeBlockAll,
			Egress: &v1alpha1.NetworkEgressPolicy{
				CredentialRules: []v1alpha1.EgressCredentialRule{{
					Name:          "git-ssh",
					CredentialRef: "git-cred",
					Protocol:      v1alpha1.EgressAuthProtocolSSH,
					Domains:       []string{"github.com"},
				}},
			},
		},
		RequestBindings: []v1alpha1.CredentialBinding{
			testSSHProxyCredentialBinding("git-cred"),
		},
	})

	if result == nil || result.PolicySpec == nil || result.PolicySpec.Egress == nil {
		t.Fatalf("expected egress policy")
	}
	if len(result.PolicySpec.Egress.CredentialRules) != 1 {
		t.Fatalf("rule count = %d, want 1", len(result.PolicySpec.Egress.CredentialRules))
	}
}
