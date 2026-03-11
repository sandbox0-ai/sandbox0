package manager

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestResolveNetworkPolicyProvider(t *testing.T) {
	t.Run("defaults to noop when netd is disabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{}
		if got := resolveNetworkPolicyProvider(infra); got != "noop" {
			t.Fatalf("expected noop provider, got %q", got)
		}
	})

	t.Run("uses netd when netd is enabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Services: &infrav1alpha1.ServicesConfig{
					Netd: &infrav1alpha1.NetdServiceConfig{
						BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
							Enabled: true,
						},
					},
				},
			},
		}
		if got := resolveNetworkPolicyProvider(infra); got != "netd" {
			t.Fatalf("expected netd provider, got %q", got)
		}
	})
}

func TestResolveSandboxPodPlacementPrefersSharedPlacement(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{
					"sandbox0.ai/node-role": "shared",
				},
				Tolerations: []corev1.Toleration{
					{
						Key:      "sandbox0.ai/sandbox",
						Operator: corev1.TolerationOpEqual,
						Value:    "true",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "legacy",
					},
				},
			},
		},
	}

	placement := resolveSandboxPodPlacement(infra)
	if got := placement.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared placement to win, got %q", got)
	}
	if len(placement.Tolerations) != 1 || placement.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared tolerations, got %#v", placement.Tolerations)
	}
}
