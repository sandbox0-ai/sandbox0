package registry

import (
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestResolveRegistryConfigReturnsNilWhenRegistryIsNotDeclared(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "minimal"
	infra.Namespace = "sandbox0-system"

	if cfg := ResolveRegistryConfig(infra); cfg != nil {
		t.Fatalf("expected nil registry config, got %#v", cfg)
	}
}

func TestResolveBuiltinRegistryConfigIsDisabledWhenRegistryIsNotDeclared(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}

	cfg := resolveBuiltinRegistryConfig(infra)
	if cfg.Enabled {
		t.Fatalf("expected builtin registry to be disabled when registry is not declared")
	}
}

func TestResolveBuiltinRegistryConfigDefaultsToEnabledForBuiltinProvider(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
			},
		},
	}

	cfg := resolveBuiltinRegistryConfig(infra)
	if !cfg.Enabled {
		t.Fatal("expected builtin registry to default to enabled for builtin provider")
	}
}

func TestResolveBuiltinRegistryConfigDefaultsToEnabledWhenProviderIsOmitted(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{},
		},
	}

	cfg := resolveBuiltinRegistryConfig(infra)
	if !cfg.Enabled {
		t.Fatal("expected builtin registry to default to enabled when provider is omitted")
	}
}
