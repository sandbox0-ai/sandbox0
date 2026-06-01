package common

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	templatev1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestBuildBuiltinTemplateSpecUsesDockerInSandboxPreset(t *testing.T) {
	t.Parallel()

	spec := buildBuiltinTemplateSpec(template.DockerInSandboxTemplateID, infrav1alpha1.BuiltinTemplateConfig{})

	if spec.DisplayName != template.DockerInSandboxTemplateDisplayName {
		t.Fatalf("DisplayName = %q, want %q", spec.DisplayName, template.DockerInSandboxTemplateDisplayName)
	}
	if spec.MainContainer.Image != template.DefaultTemplateImage {
		t.Fatalf("image = %q, want %q", spec.MainContainer.Image, template.DefaultTemplateImage)
	}
	if spec.MainContainer.Resources.CPU.Cmp(resource.MustParse(template.DockerInSandboxCPU)) != 0 {
		t.Fatalf("cpu = %s, want %s", spec.MainContainer.Resources.CPU.String(), template.DockerInSandboxCPU)
	}
	if spec.MainContainer.Resources.Memory.Cmp(resource.MustParse(template.DockerInSandboxMemory)) != 0 {
		t.Fatalf("memory = %s, want %s", spec.MainContainer.Resources.Memory.String(), template.DockerInSandboxMemory)
	}
	if spec.MainContainer.SecurityContext == nil || spec.MainContainer.SecurityContext.Privileged == nil || !*spec.MainContainer.SecurityContext.Privileged {
		t.Fatalf("expected privileged security context, got %#v", spec.MainContainer.SecurityContext)
	}
	if spec.Pod == nil || len(spec.Pod.EmptyDirMounts) != 1 {
		t.Fatalf("expected one emptyDir mount, got %#v", spec.Pod)
	}
	if got := spec.Pod.EmptyDirMounts[0].MountPath; got != template.DockerInSandboxDockerRoot {
		t.Fatalf("emptyDir mount path = %q, want %q", got, template.DockerInSandboxDockerRoot)
	}
	if len(spec.WarmProcesses) != 1 || spec.WarmProcesses[0].Name != template.DockerInSandboxWarmProcessName {
		t.Fatalf("warmProcesses = %#v, want dockerd process", spec.WarmProcesses)
	}
	if spec.WarmProcesses[0].Probes == nil || spec.WarmProcesses[0].Probes.Readiness == nil || spec.WarmProcesses[0].Probes.Readiness.Exec == nil {
		t.Fatalf("expected dockerd readiness exec probe, got %#v", spec.WarmProcesses[0].Probes)
	}
}

func TestBuildBuiltinTemplateSpecAllowsFullSpecOverride(t *testing.T) {
	t.Parallel()

	customCPU := resource.MustParse("3")
	customMemory := resource.MustParse("12Gi")
	spec := buildBuiltinTemplateSpec("custom", infrav1alpha1.BuiltinTemplateConfig{
		Spec: &templatev1alpha1.SandboxTemplateSpec{
			DisplayName: "Custom",
			MainContainer: templatev1alpha1.ContainerSpec{
				Image: "example.com/custom:latest",
				Resources: templatev1alpha1.ResourceQuota{
					CPU:    customCPU,
					Memory: customMemory,
				},
			},
			Pool: templatev1alpha1.PoolStrategy{MinIdle: 2, MaxIdle: 4},
		},
	})

	if spec.DisplayName != "Custom" {
		t.Fatalf("DisplayName = %q, want Custom", spec.DisplayName)
	}
	if spec.MainContainer.Image != "example.com/custom:latest" {
		t.Fatalf("image = %q, want custom image", spec.MainContainer.Image)
	}
	if spec.MainContainer.Resources.CPU.Cmp(customCPU) != 0 || spec.MainContainer.Resources.Memory.Cmp(customMemory) != 0 {
		t.Fatalf("resources = %#v, want custom cpu/memory", spec.MainContainer.Resources)
	}
	if spec.Pool.MinIdle != 2 || spec.Pool.MaxIdle != 4 {
		t.Fatalf("pool = %#v, want 2/4", spec.Pool)
	}
	if spec.Network == nil || spec.Network.Mode != templatev1alpha1.NetworkModeAllowAll {
		t.Fatalf("network = %#v, want default allow-all", spec.Network)
	}
}

func TestBuiltinTemplatePresetsSatisfyResourceRatio(t *testing.T) {
	t.Parallel()

	for _, templateID := range []string{template.DefaultTemplateID, template.DockerInSandboxTemplateID} {
		t.Run(templateID, func(t *testing.T) {
			t.Parallel()
			spec := buildBuiltinTemplateSpec(templateID, infrav1alpha1.BuiltinTemplateConfig{})
			if err := template.ValidateResourceRatio(spec, template.MemoryPerCPUOrDefault(""), "builtin template "+templateID); err != nil {
				t.Fatalf("ValidateResourceRatio(%s): %v", templateID, err)
			}
		})
	}
}
