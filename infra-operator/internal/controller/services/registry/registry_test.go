package registry

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestBuiltinPushRegistryUsesServiceEndpointByDefault(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	infra.Namespace = "sandbox0-system"

	host := builtinPushRegistry(infra, infrav1alpha1.BuiltinRegistryConfig{Port: 5000})
	if host != "demo-registry.sandbox0-system.svc:5000" {
		t.Fatalf("unexpected registry host: %q", host)
	}
}

func TestBuiltinPushRegistryUsesIngressHostWhenEnabled(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	infra.Namespace = "sandbox0-system"

	host := builtinPushRegistry(infra, infrav1alpha1.BuiltinRegistryConfig{
		Port: 5000,
		Ingress: &infrav1alpha1.IngressConfig{
			Enabled: true,
			Host:    "registry.example.com",
		},
	})
	if host != "registry.example.com" {
		t.Fatalf("unexpected registry host: %q", host)
	}
}

func TestBuiltinPushRegistryUsesExplicitPushEndpoint(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	infra.Namespace = "sandbox0-system"

	host := builtinPushRegistry(infra, infrav1alpha1.BuiltinRegistryConfig{
		Port:         5000,
		PushEndpoint: "http://registry-push.example.com:5443",
		Ingress: &infrav1alpha1.IngressConfig{
			Enabled: true,
			Host:    "registry.example.com",
		},
	})
	if host != "registry-push.example.com:5443" {
		t.Fatalf("unexpected registry host: %q", host)
	}
}

func TestResolveBuiltinRegistryConfigPreservesIngress(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Enabled: true,
					Service: &infrav1alpha1.ServiceNetworkConfig{
						Type: corev1.ServiceTypeClusterIP,
						Port: 5000,
					},
					Ingress: &infrav1alpha1.IngressConfig{
						Enabled:   true,
						ClassName: "nginx",
						Host:      "registry.example.com",
						TLSSecret: "registry-tls",
					},
				},
			},
		},
	}

	cfg := resolveBuiltinRegistryConfig(infra)
	if cfg.Ingress == nil || !cfg.Ingress.Enabled || cfg.Ingress.Host != "registry.example.com" {
		t.Fatalf("expected ingress configuration to be preserved, got %#v", cfg.Ingress)
	}
}
