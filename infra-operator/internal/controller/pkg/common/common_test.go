package common

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestEnsurePodTemplateAnnotations(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Generation: 7,
		},
	}
	input := map[string]string{"existing": "value"}

	out := EnsurePodTemplateAnnotations(infra, input)

	if out["existing"] != "value" {
		t.Fatalf("expected existing annotation to be preserved")
	}
	if out[PodTemplateSpecGenerationAnnotation] != "7" {
		t.Fatalf("expected generation annotation to be set, got %q", out[PodTemplateSpecGenerationAnnotation])
	}

	input["existing"] = "changed"
	if out["existing"] != "value" {
		t.Fatalf("expected output map to be independent from input")
	}
}
