package common

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestResolveSandboxNodePlacementCopiesNetdPlacement(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "sandbox",
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "sandbox.gke.io/runtime",
							Operator: corev1.TolerationOpEqual,
							Value:    "gvisor",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
	}

	nodeSelector, tolerations := ResolveSandboxNodePlacement(infra)
	if got := nodeSelector["sandbox0.ai/node-role"]; got != "sandbox" {
		t.Fatalf("expected sandbox node selector, got %q", got)
	}
	if len(tolerations) != 1 || tolerations[0].Key != "sandbox.gke.io/runtime" {
		t.Fatalf("expected copied toleration, got %#v", tolerations)
	}

	infra.Spec.Services.Netd.NodeSelector["sandbox0.ai/node-role"] = "system"
	infra.Spec.Services.Netd.Tolerations[0].Value = "runc"

	if got := nodeSelector["sandbox0.ai/node-role"]; got != "sandbox" {
		t.Fatalf("expected copied node selector to remain unchanged, got %q", got)
	}
	if got := tolerations[0].Value; got != "gvisor" {
		t.Fatalf("expected copied toleration to remain unchanged, got %q", got)
	}
}
