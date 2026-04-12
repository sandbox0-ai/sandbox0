package utils

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func TestCloneTemplateForCreateNormalizesTeamTemplateMemoryRatio(t *testing.T) {
	t.Parallel()

	base := apispec.Template{
		Spec: apispec.SandboxTemplateSpec{
			MainContainer: &apispec.ContainerSpec{
				Image: "nginx:1.27-alpine",
				Resources: apispec.ResourceQuota{
					Cpu:    ptr("500m"),
					Memory: ptr("512Mi"),
				},
			},
		},
	}

	created := CloneTemplateForCreate(base, "tpl-e2e")

	if created.Spec.MainContainer == nil {
		t.Fatal("main container should be set")
	}
	if created.Spec.MainContainer.Resources.Memory == nil {
		t.Fatal("main memory should be set")
	}
	if got := *created.Spec.MainContainer.Resources.Memory; got != "2Gi" {
		t.Fatalf("main memory = %q, want %q", got, "2Gi")
	}
}
