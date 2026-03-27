package common

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestResolveSandboxNodePlacementFallsBackToNetdPlacement(t *testing.T) {
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

func TestResolveSandboxNodePlacementPrefersSharedPlacement(t *testing.T) {
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
	if got := nodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector to win, got %q", got)
	}
	if len(tolerations) != 1 || tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared tolerations to win, got %#v", tolerations)
	}
}

func TestResolveSandboxNodePlacementFallsBackPerField(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{
					"sandbox0.ai/node-role": "shared",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "legacy",
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
	if got := nodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector to win, got %q", got)
	}
	if len(tolerations) != 1 || tolerations[0].Key != "sandbox.gke.io/runtime" {
		t.Fatalf("expected legacy tolerations fallback, got %#v", tolerations)
	}
}

func TestConfigHashAnnotationChangesWithConfig(t *testing.T) {
	sameA, err := ConfigHashAnnotation(map[string]any{
		"http_port": 8080,
		"metrics":   true,
	})
	if err != nil {
		t.Fatalf("hash annotation for config A: %v", err)
	}
	sameB, err := ConfigHashAnnotation(map[string]any{
		"http_port": 8080,
		"metrics":   true,
	})
	if err != nil {
		t.Fatalf("hash annotation for config B: %v", err)
	}
	changed, err := ConfigHashAnnotation(map[string]any{
		"http_port": 18080,
		"metrics":   true,
	})
	if err != nil {
		t.Fatalf("hash annotation for changed config: %v", err)
	}

	if !reflect.DeepEqual(sameA, sameB) {
		t.Fatalf("expected identical config to have identical hash annotation, got %#v vs %#v", sameA, sameB)
	}
	if sameA[PodTemplateConfigHashAnnotation] == changed[PodTemplateConfigHashAnnotation] {
		t.Fatalf("expected changed config to produce a different hash, got %q", changed[PodTemplateConfigHashAnnotation])
	}
}

func TestEnsurePodTemplateAnnotationsClonesInput(t *testing.T) {
	annotations := map[string]string{
		PodTemplateConfigHashAnnotation: "abc123",
		"custom":                        "value",
	}

	got := EnsurePodTemplateAnnotations(annotations)
	if !reflect.DeepEqual(got, annotations) {
		t.Fatalf("expected cloned annotations to match input, got %#v", got)
	}

	annotations["custom"] = "changed"
	if got["custom"] != "value" {
		t.Fatalf("expected cloned annotations to be isolated from caller mutation, got %#v", got)
	}
}
