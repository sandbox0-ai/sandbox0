package common

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	templatev1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestBuildBuiltinTemplateSpecUsesDefaultPreset(t *testing.T) {
	t.Parallel()

	spec := BuildBuiltinTemplateSpec(template.DefaultTemplateID, infrav1alpha1.BuiltinTemplateConfig{})

	if spec.DisplayName != template.DefaultTemplateDisplayName {
		t.Fatalf("DisplayName = %q, want %q", spec.DisplayName, template.DefaultTemplateDisplayName)
	}
	if spec.MainContainer.Image != template.DefaultTemplateImage {
		t.Fatalf("image = %q, want %q", spec.MainContainer.Image, template.DefaultTemplateImage)
	}
	if spec.MainContainer.Resources.CPU.Cmp(resource.MustParse(template.DefaultTemplateCPU)) != 0 {
		t.Fatalf("cpu = %s, want %s", spec.MainContainer.Resources.CPU.String(), template.DefaultTemplateCPU)
	}
	if spec.MainContainer.Resources.Memory.Cmp(resource.MustParse(template.DefaultTemplateMemory)) != 0 {
		t.Fatalf("memory = %s, want %s", spec.MainContainer.Resources.Memory.String(), template.DefaultTemplateMemory)
	}
	if spec.MainContainer.Resources.EphemeralStorage.Cmp(resource.MustParse(template.DefaultTemplateEphemeralStorage)) != 0 {
		t.Fatalf("ephemeralStorage = %s, want %s", spec.MainContainer.Resources.EphemeralStorage.String(), template.DefaultTemplateEphemeralStorage)
	}
	if len(spec.VolumeMounts) != 1 {
		t.Fatalf("volumeMounts = %#v, want one workspace mount", spec.VolumeMounts)
	}
	mount := spec.VolumeMounts[0]
	if mount.Name != template.DefaultTemplateWorkspaceName || mount.MountPath != template.DefaultTemplateWorkspaceMount || mount.ReadOnly {
		t.Fatalf("volumeMounts[0] = %#v, want writable %s at %s", mount, template.DefaultTemplateWorkspaceName, template.DefaultTemplateWorkspaceMount)
	}
	if spec.MainContainer.SecurityContext == nil || spec.MainContainer.SecurityContext.Privileged == nil || !*spec.MainContainer.SecurityContext.Privileged {
		t.Fatalf("expected privileged security context, got %#v", spec.MainContainer.SecurityContext)
	}
	if spec.MainContainer.SecurityContext.AllowPrivilegeEscalation == nil || !*spec.MainContainer.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("expected allowPrivilegeEscalation=true, got %#v", spec.MainContainer.SecurityContext)
	}
	if spec.Pod == nil || len(spec.Pod.EmptyDirMounts) != 1 {
		t.Fatalf("expected one emptyDir mount, got %#v", spec.Pod)
	}
	dockerRoot := spec.Pod.EmptyDirMounts[0]
	if dockerRoot.MountPath != template.DefaultTemplateDockerRoot {
		t.Fatalf("emptyDir mount path = %q, want %q", dockerRoot.MountPath, template.DefaultTemplateDockerRoot)
	}
	if dockerRoot.SizeLimit == nil || dockerRoot.SizeLimit.Cmp(resource.MustParse(template.DefaultTemplateDockerRootSize)) != 0 {
		t.Fatalf("emptyDir sizeLimit = %#v, want %s", dockerRoot.SizeLimit, template.DefaultTemplateDockerRootSize)
	}
}

func TestBuildBuiltinTemplateSpecDoesNotAddDefaultRuntimeShapeToGenericPreset(t *testing.T) {
	t.Parallel()

	spec := BuildBuiltinTemplateSpec("custom", infrav1alpha1.BuiltinTemplateConfig{})

	if len(spec.VolumeMounts) != 0 {
		t.Fatalf("volumeMounts = %#v, want none for generic builtin preset", spec.VolumeMounts)
	}
	if spec.MainContainer.SecurityContext != nil {
		t.Fatalf("securityContext = %#v, want nil for generic builtin preset", spec.MainContainer.SecurityContext)
	}
	if spec.Pod != nil {
		t.Fatalf("pod = %#v, want nil for generic builtin preset", spec.Pod)
	}
}

func TestBuildBuiltinTemplateSpecAllowsFullSpecOverride(t *testing.T) {
	t.Parallel()

	customCPU := resource.MustParse("3")
	customMemory := resource.MustParse("12Gi")
	spec := BuildBuiltinTemplateSpec("custom", infrav1alpha1.BuiltinTemplateConfig{
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

func TestBuildBuiltinTemplateSpecUsesOpenClawPreset(t *testing.T) {
	t.Parallel()

	spec := BuildBuiltinTemplateSpec(template.OpenClawTemplateID, infrav1alpha1.BuiltinTemplateConfig{})

	if spec.DisplayName != template.OpenClawTemplateDisplayName {
		t.Fatalf("DisplayName = %q, want %q", spec.DisplayName, template.OpenClawTemplateDisplayName)
	}
	if spec.MainContainer.Image != template.OpenClawTemplateImage {
		t.Fatalf("image = %q, want %q", spec.MainContainer.Image, template.OpenClawTemplateImage)
	}
	if spec.MainContainer.Resources.CPU.Cmp(resource.MustParse(template.OpenClawCPU)) != 0 {
		t.Fatalf("cpu = %s, want %s", spec.MainContainer.Resources.CPU.String(), template.OpenClawCPU)
	}
	if spec.MainContainer.Resources.Memory.Cmp(resource.MustParse(template.OpenClawMemory)) != 0 {
		t.Fatalf("memory = %s, want %s", spec.MainContainer.Resources.Memory.String(), template.OpenClawMemory)
	}
	if len(spec.VolumeMounts) != 1 || spec.VolumeMounts[0].MountPath != template.OpenClawDataMount {
		t.Fatalf("volumeMounts = %#v, want one mount at %s", spec.VolumeMounts, template.OpenClawDataMount)
	}
	assertAgentRuntimePodShape(t, spec)
	if spec.EnvVars["OPENCLAW_CONFIG_PATH"] != template.OpenClawDataMount+"/openclaw.json" {
		t.Fatalf("OPENCLAW_CONFIG_PATH = %q", spec.EnvVars["OPENCLAW_CONFIG_PATH"])
	}
	if spec.Pool.MinIdle != 0 || spec.Pool.MaxIdle != 2 {
		t.Fatalf("pool = %#v, want 0/2", spec.Pool)
	}
}

func TestBuildBuiltinTemplateSpecUsesHermesPreset(t *testing.T) {
	t.Parallel()

	spec := BuildBuiltinTemplateSpec(template.HermesTemplateID, infrav1alpha1.BuiltinTemplateConfig{})

	if spec.DisplayName != template.HermesTemplateDisplayName {
		t.Fatalf("DisplayName = %q, want %q", spec.DisplayName, template.HermesTemplateDisplayName)
	}
	if spec.MainContainer.Image != template.HermesTemplateImage {
		t.Fatalf("image = %q, want %q", spec.MainContainer.Image, template.HermesTemplateImage)
	}
	if spec.MainContainer.Resources.CPU.Cmp(resource.MustParse(template.HermesCPU)) != 0 {
		t.Fatalf("cpu = %s, want %s", spec.MainContainer.Resources.CPU.String(), template.HermesCPU)
	}
	if spec.MainContainer.Resources.Memory.Cmp(resource.MustParse(template.HermesMemory)) != 0 {
		t.Fatalf("memory = %s, want %s", spec.MainContainer.Resources.Memory.String(), template.HermesMemory)
	}
	if len(spec.VolumeMounts) != 1 || spec.VolumeMounts[0].MountPath != template.HermesDataMount {
		t.Fatalf("volumeMounts = %#v, want one mount at %s", spec.VolumeMounts, template.HermesDataMount)
	}
	assertAgentRuntimePodShape(t, spec)
	if spec.EnvVars["HERMES_HOME"] != template.HermesRuntimeHome {
		t.Fatalf("HERMES_HOME = %q", spec.EnvVars["HERMES_HOME"])
	}
	if spec.EnvVars["HERMES_PERSIST_HOME"] != template.HermesDataMount {
		t.Fatalf("HERMES_PERSIST_HOME = %q", spec.EnvVars["HERMES_PERSIST_HOME"])
	}
	if spec.Pool.MinIdle != 0 || spec.Pool.MaxIdle != 2 {
		t.Fatalf("pool = %#v, want 0/2", spec.Pool)
	}
}

func TestBuiltinTemplatePresetsSatisfyResourceRatio(t *testing.T) {
	t.Parallel()

	for _, templateID := range []string{
		template.DefaultTemplateID,
		template.OpenClawTemplateID,
		template.HermesTemplateID,
	} {
		t.Run(templateID, func(t *testing.T) {
			t.Parallel()
			spec := BuildBuiltinTemplateSpec(templateID, infrav1alpha1.BuiltinTemplateConfig{})
			if err := template.ValidateResourceRatio(spec, template.MemoryPerCPUOrDefault(""), "builtin template "+templateID); err != nil {
				t.Fatalf("ValidateResourceRatio(%s): %v", templateID, err)
			}
		})
	}
}

func TestBuildBuiltinTemplateSpecPreservesExplicitZeroMinIdle(t *testing.T) {
	t.Parallel()

	minIdle := int32(0)
	maxIdle := int32(2)
	spec := BuildBuiltinTemplateSpec(template.DefaultTemplateID, infrav1alpha1.BuiltinTemplateConfig{
		Pool: infrav1alpha1.BuiltinTemplatePoolConfig{
			MinIdle: &minIdle,
			MaxIdle: &maxIdle,
		},
	})

	if spec.Pool.MinIdle != 0 || spec.Pool.MaxIdle != 2 {
		t.Fatalf("pool = %#v, want 0/2", spec.Pool)
	}
}

func TestBuildBuiltinTemplateSpecDefaultsMissingPoolFields(t *testing.T) {
	t.Parallel()

	maxIdle := int32(2)
	spec := BuildBuiltinTemplateSpec(template.DefaultTemplateID, infrav1alpha1.BuiltinTemplateConfig{
		Pool: infrav1alpha1.BuiltinTemplatePoolConfig{
			MaxIdle: &maxIdle,
		},
	})

	if spec.Pool.MinIdle != template.DefaultTemplateMinIdle || spec.Pool.MaxIdle != 2 {
		t.Fatalf("pool = %#v, want %d/2", spec.Pool, template.DefaultTemplateMinIdle)
	}
}

func TestPruneUnconfiguredBuiltinTemplatesDeletesOnlyOwnedPublicTemplates(t *testing.T) {
	t.Parallel()

	store := &fakeBuiltinTemplatePruneStore{
		templates: []*template.Template{
			{TemplateID: template.DefaultTemplateID, Scope: "public", UserID: "infra-operator"},
			{TemplateID: template.OpenClawTemplateID, Scope: "public", UserID: "infra-operator"},
			{TemplateID: "custom-public", Scope: "public", UserID: "user"},
			{TemplateID: "custom-team", Scope: "team", TeamID: "team-1", UserID: "infra-operator"},
		},
	}

	err := pruneUnconfiguredBuiltinTemplates(context.Background(), store, map[string]struct{}{
		template.DefaultTemplateID: {},
	}, "infra-operator")
	if err != nil {
		t.Fatalf("pruneUnconfiguredBuiltinTemplates: %v", err)
	}
	if len(store.deleted) != 1 || store.deleted[0] != template.OpenClawTemplateID {
		t.Fatalf("deleted = %#v, want only %s", store.deleted, template.OpenClawTemplateID)
	}
}

type fakeBuiltinTemplatePruneStore struct {
	templates []*template.Template
	deleted   []string
}

func (s *fakeBuiltinTemplatePruneStore) ListTemplates(context.Context) ([]*template.Template, error) {
	return s.templates, nil
}

func (s *fakeBuiltinTemplatePruneStore) DeleteTemplate(_ context.Context, _, _, templateID string) error {
	s.deleted = append(s.deleted, templateID)
	return nil
}

func assertAgentRuntimePodShape(t *testing.T, spec templatev1alpha1.SandboxTemplateSpec) {
	t.Helper()
	if spec.MainContainer.SecurityContext == nil {
		t.Fatal("expected security context")
	}
	security := spec.MainContainer.SecurityContext
	if security.RunAsUser == nil || *security.RunAsUser != 0 {
		t.Fatalf("runAsUser = %#v, want 0", security.RunAsUser)
	}
	if security.RunAsGroup == nil || *security.RunAsGroup != 0 {
		t.Fatalf("runAsGroup = %#v, want 0", security.RunAsGroup)
	}
	if security.RunAsNonRoot == nil || *security.RunAsNonRoot {
		t.Fatalf("runAsNonRoot = %#v, want false", security.RunAsNonRoot)
	}
	if spec.Pod == nil || len(spec.Pod.EmptyDirMounts) != 1 {
		t.Fatalf("emptyDirMounts = %#v, want one mount", spec.Pod)
	}
	mount := spec.Pod.EmptyDirMounts[0]
	if mount.MountPath != template.AgentWorkspaceMount {
		t.Fatalf("emptyDir mount path = %q, want %q", mount.MountPath, template.AgentWorkspaceMount)
	}
	if mount.SizeLimit == nil || mount.SizeLimit.Cmp(resource.MustParse(template.AgentWorkspaceSizeLimit)) != 0 {
		t.Fatalf("emptyDir sizeLimit = %#v, want %s", mount.SizeLimit, template.AgentWorkspaceSizeLimit)
	}
}
