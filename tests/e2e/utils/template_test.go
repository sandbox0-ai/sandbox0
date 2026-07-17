package utils

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func TestCloneTemplateForCreatePreservesMemory(t *testing.T) {
	t.Parallel()

	base := apispec.Template{
		Spec: apispec.SandboxTemplateSpec{
			MainContainer: &apispec.ContainerSpec{
				Image: "nginx:1.27-alpine",
				Resources: apispec.ResourceQuota{
					Memory: "512Mi",
				},
			},
		},
	}

	created := CloneTemplateForCreate(base, "tpl-e2e")

	if created.Spec.MainContainer == nil {
		t.Fatal("main container should be set")
	}
	if got := created.Spec.MainContainer.Resources.Memory; got != "512Mi" {
		t.Fatalf("main memory = %q, want %q", got, "512Mi")
	}
}
