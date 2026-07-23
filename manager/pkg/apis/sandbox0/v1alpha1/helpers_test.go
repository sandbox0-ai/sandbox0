package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPodSpecAppliesDefaultSandboxPlacement(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
sandbox_pod_placement:
  node_selector:
    sandbox0.ai/node-role: sandbox
  tolerations:
    - key: sandbox0.ai/sandbox
      operator: Equal
      value: "true"
      effect: NoSchedule
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())

	if got := spec.NodeSelector["sandbox0.ai/node-role"]; got != "sandbox" {
		t.Fatalf("expected injected node selector, got %q", got)
	}
	if len(spec.Tolerations) != 1 {
		t.Fatalf("expected 1 toleration, got %d", len(spec.Tolerations))
	}
	if spec.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected injected toleration key, got %q", spec.Tolerations[0].Key)
	}
}

func TestBuildPodSpecKeepsInjectedPlacementAuthoritative(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
sandbox_pod_placement:
  node_selector:
    sandbox0.ai/node-role: sandbox
    topology.kubernetes.io/zone: us-east1-b
  tolerations:
    - key: sandbox0.ai/sandbox
      operator: Equal
      value: "true"
      effect: NoSchedule
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.Pod = &PodSpecOverride{
		NodeSelector: map[string]string{
			"sandbox0.ai/node-role":       "system",
			"kubernetes.io/arch":          "amd64",
			"topology.kubernetes.io/zone": "custom-zone",
		},
		Tolerations: []Toleration{
			{
				Key:      "sandbox0.ai/sandbox",
				Operator: "Equal",
				Value:    "true",
				Effect:   "NoSchedule",
			},
			{
				Key:      "sandbox.gke.io/runtime",
				Operator: "Equal",
				Value:    "gvisor",
				Effect:   "NoSchedule",
			},
		},
	}

	spec := BuildPodSpec(template)

	if got := spec.NodeSelector["sandbox0.ai/node-role"]; got != "sandbox" {
		t.Fatalf("expected manager placement to win conflicting node selector, got %q", got)
	}
	if got := spec.NodeSelector["topology.kubernetes.io/zone"]; got != "us-east1-b" {
		t.Fatalf("expected manager placement to win conflicting zone, got %q", got)
	}
	if got := spec.NodeSelector["kubernetes.io/arch"]; got != "amd64" {
		t.Fatalf("expected template-specific node selector to be preserved, got %q", got)
	}
	if len(spec.Tolerations) != 2 {
		t.Fatalf("expected merged tolerations without duplicates, got %d", len(spec.Tolerations))
	}
}

func TestBuildPodSpecAppliesConfiguredSandboxRuntimeClass(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
sandbox_runtime_class_name: kata-shared
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()

	spec := BuildPodSpec(template)
	if spec.RuntimeClassName == nil || *spec.RuntimeClassName != "kata-shared" {
		t.Fatalf("expected sandbox runtime class kata-shared, got %#v", spec.RuntimeClassName)
	}
}

func TestBuildPodSpecDisablesServiceAccountTokenAutomount(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Fatalf("automountServiceAccountToken = %#v, want false", spec.AutomountServiceAccountToken)
	}
}

func TestBuildPodSpecUsesProcdImageVolumeByDefault(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())

	if len(spec.InitContainers) != 0 {
		t.Fatalf("expected no init containers, got %#v", spec.InitContainers)
	}
	volume := findVolume(spec.Volumes, procdBinVolumeName)
	if volume == nil || volume.Image == nil {
		t.Fatalf("expected procd bin image volume, got %#v", volume)
	}
	if volume.Image.Reference != "sandbox0/manager:test-procd-bin" {
		t.Fatalf("image volume reference = %q, want sandbox0/manager:test-procd-bin", volume.Image.Reference)
	}
	main := spec.Containers[0]
	if len(main.Command) != 1 || main.Command[0] != "/procd-image/usr/local/bin/procd" {
		t.Fatalf("main command = %#v, want /procd-image/usr/local/bin/procd", main.Command)
	}
	mount := findVolumeMount(main.VolumeMounts, procdBinVolumeName)
	if mount == nil || mount.MountPath != "/procd-image" || mount.SubPath != "" || !mount.ReadOnly {
		t.Fatalf("procd bin mount = %#v, want read-only /procd-image without subPath", mount)
	}
}

func TestBuildPodSpecUsesExplicitProcdImageVolumeRef(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
procd_bin_image_ref: sandbox0/manager:test-procd-bin
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())

	if len(spec.InitContainers) != 0 {
		t.Fatalf("expected no init containers, got %#v", spec.InitContainers)
	}
	volume := findVolume(spec.Volumes, procdBinVolumeName)
	if volume == nil || volume.Image == nil {
		t.Fatalf("expected procd bin image volume, got %#v", volume)
	}
	if volume.Image.Reference != "sandbox0/manager:test-procd-bin" {
		t.Fatalf("image volume reference = %q, want sandbox0/manager:test-procd-bin", volume.Image.Reference)
	}
	if volume.Image.PullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("image volume pull policy = %q, want %q", volume.Image.PullPolicy, corev1.PullIfNotPresent)
	}
	main := spec.Containers[0]
	if len(main.Command) != 1 || main.Command[0] != "/procd-image/usr/local/bin/procd" {
		t.Fatalf("main command = %#v, want /procd-image/usr/local/bin/procd", main.Command)
	}
	mount := findVolumeMount(main.VolumeMounts, procdBinVolumeName)
	if mount == nil || mount.MountPath != "/procd-image" || mount.SubPath != "" || !mount.ReadOnly {
		t.Fatalf("procd bin mount = %#v, want read-only /procd-image without subPath", mount)
	}
}

func TestBuildPodSpecLeavesOrdinarySandboxNonPrivileged(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if len(spec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}

	main := spec.Containers[0]
	if main.Name != "procd" {
		t.Fatalf("expected main container procd, got %q", main.Name)
	}
	if main.SecurityContext == nil {
		t.Fatal("expected security context to be initialized")
	}
	if main.SecurityContext.Privileged != nil && *main.SecurityContext.Privileged {
		t.Fatalf("expected ordinary sandbox to remain non-privileged, got %#v", main.SecurityContext)
	}
}

func TestBuildPodSpecUsesIfNotPresentForMainContainerByDefault(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if len(spec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}
	if spec.Containers[0].ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("main container image pull policy = %q, want %q", spec.Containers[0].ImagePullPolicy, corev1.PullIfNotPresent)
	}
}

func TestBuildPodSpecAppliesMainContainerSecurityContext(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.MainContainer.SecurityContext = &SecurityContext{
		Privileged:               ptrBool(true),
		RunAsUser:                ptrInt64(0),
		RunAsGroup:               ptrInt64(0),
		RunAsNonRoot:             ptrBool(false),
		ReadOnlyRootFilesystem:   ptrBool(false),
		AllowPrivilegeEscalation: ptrBool(true),
		Capabilities: &Capabilities{
			Add:  []string{"SYS_ADMIN", "NET_ADMIN"},
			Drop: []string{"NET_RAW"},
		},
		SeccompProfile: &SeccompProfile{
			Type: SeccompProfileTypeUnconfined,
		},
		AppArmorProfile: &AppArmorProfile{
			Type: AppArmorProfileTypeRuntimeDefault,
		},
	}

	spec := BuildPodSpec(template)
	main := spec.Containers[0]
	if main.SecurityContext == nil {
		t.Fatal("expected security context")
	}
	if main.SecurityContext.Privileged == nil || !*main.SecurityContext.Privileged {
		t.Fatalf("privileged = %#v, want true", main.SecurityContext.Privileged)
	}
	if main.SecurityContext.AllowPrivilegeEscalation == nil || !*main.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("allowPrivilegeEscalation = %#v, want true", main.SecurityContext.AllowPrivilegeEscalation)
	}
	if main.SecurityContext.RunAsUser == nil || *main.SecurityContext.RunAsUser != 0 {
		t.Fatalf("runAsUser = %#v, want 0", main.SecurityContext.RunAsUser)
	}
	if main.SecurityContext.RunAsGroup == nil || *main.SecurityContext.RunAsGroup != 0 {
		t.Fatalf("runAsGroup = %#v, want 0", main.SecurityContext.RunAsGroup)
	}
	if main.SecurityContext.RunAsNonRoot == nil || *main.SecurityContext.RunAsNonRoot {
		t.Fatalf("runAsNonRoot = %#v, want false", main.SecurityContext.RunAsNonRoot)
	}
	if main.SecurityContext.ReadOnlyRootFilesystem == nil || *main.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatalf("readOnlyRootFilesystem = %#v, want false", main.SecurityContext.ReadOnlyRootFilesystem)
	}
	if main.SecurityContext.Capabilities == nil {
		t.Fatal("expected capabilities")
	}
	if got := main.SecurityContext.Capabilities.Add; len(got) != 2 || got[0] != corev1.Capability("SYS_ADMIN") || got[1] != corev1.Capability("NET_ADMIN") {
		t.Fatalf("capabilities.add = %#v", got)
	}
	if got := main.SecurityContext.Capabilities.Drop; len(got) != 1 || got[0] != corev1.Capability("NET_RAW") {
		t.Fatalf("capabilities.drop = %#v", got)
	}
	if main.SecurityContext.SeccompProfile == nil || main.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeUnconfined {
		t.Fatalf("seccompProfile = %#v, want Unconfined", main.SecurityContext.SeccompProfile)
	}
	if main.SecurityContext.AppArmorProfile == nil || main.SecurityContext.AppArmorProfile.Type != corev1.AppArmorProfileTypeRuntimeDefault {
		t.Fatalf("appArmorProfile = %#v, want RuntimeDefault", main.SecurityContext.AppArmorProfile)
	}
}

func TestBuildPodSpecInjectsVolumePortalMounts(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.VolumeMounts = []VolumeMountSpec{
		{Name: "workspace", MountPath: "/workspace/bench-volume"},
	}

	spec := BuildPodSpec(template)
	userVolume := findCSIVolumeByPortal(spec.Volumes, "workspace")
	if userVolume == nil {
		t.Fatalf("expected workspace csi volume, got %#v", spec.Volumes)
	}
	if userVolume.CSI.Driver != volumeportal.DriverName {
		t.Fatalf("csi driver = %q, want %q", userVolume.CSI.Driver, volumeportal.DriverName)
	}
	if got := userVolume.CSI.VolumeAttributes[volumeportal.AttributeMountPath]; got != "/workspace/bench-volume" {
		t.Fatalf("mount path attr = %q", got)
	}
	if mount := findVolumeMount(spec.Containers[0].VolumeMounts, userVolume.Name); mount == nil || mount.MountPath != "/workspace/bench-volume" {
		t.Fatalf("expected container mount for workspace volume, got %#v", spec.Containers[0].VolumeMounts)
	}

	webhookVolume := findCSIVolumeByPortal(spec.Volumes, volumeportal.WebhookStatePortalName)
	if webhookVolume == nil {
		t.Fatalf("expected webhook state portal volume, got %#v", spec.Volumes)
	}
	if mount := findVolumeMount(spec.Containers[0].VolumeMounts, webhookVolume.Name); mount == nil || mount.MountPath != volumeportal.WebhookStateMountPath {
		t.Fatalf("expected webhook state mount, got %#v", spec.Containers[0].VolumeMounts)
	}
}

func TestBuildIdlePodSpecPreMountsUserVolumePortals(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.VolumeMounts = []VolumeMountSpec{
		{Name: "workspace", MountPath: "/workspace/bench-volume"},
	}

	spec := BuildIdlePodSpec(template)
	userVolume := findCSIVolumeByPortal(spec.Volumes, "workspace")
	if userVolume == nil {
		t.Fatalf("expected user csi volume in idle pod spec, got %#v", spec.Volumes)
	}
	if mount := findVolumeMount(spec.Containers[0].VolumeMounts, userVolume.Name); mount == nil || mount.MountPath != "/workspace/bench-volume" {
		t.Fatalf("expected user volume mount, got %#v", spec.Containers[0].VolumeMounts)
	}

	webhookVolume := findCSIVolumeByPortal(spec.Volumes, volumeportal.WebhookStatePortalName)
	if webhookVolume == nil {
		t.Fatalf("expected webhook state portal volume, got %#v", spec.Volumes)
	}
	if mount := findVolumeMount(spec.Containers[0].VolumeMounts, webhookVolume.Name); mount == nil || mount.MountPath != volumeportal.WebhookStateMountPath {
		t.Fatalf("expected webhook state mount, got %#v", spec.Containers[0].VolumeMounts)
	}
}

func TestBuildPodSpecInjectsEmptyDirMounts(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	sizeLimit := resource.MustParse("20Gi")
	template.Spec.Pod = &PodSpecOverride{
		EmptyDirMounts: []EmptyDirMountSpec{{
			MountPath: "/var/lib/docker",
			SizeLimit: &sizeLimit,
		}},
	}

	spec := BuildPodSpec(template)
	main := spec.Containers[0]
	mount := findVolumeMountByPath(main.VolumeMounts, "/var/lib/docker")
	if mount == nil {
		t.Fatalf("expected emptyDir mount, got %#v", main.VolumeMounts)
	}
	volume := findVolume(spec.Volumes, mount.Name)
	if volume == nil || volume.EmptyDir == nil {
		t.Fatalf("expected emptyDir volume %q, got %#v", mount.Name, spec.Volumes)
	}
	if volume.EmptyDir.Medium != "" {
		t.Fatalf("emptyDir medium = %q, want default", volume.EmptyDir.Medium)
	}
	if volume.EmptyDir.SizeLimit == nil || volume.EmptyDir.SizeLimit.Cmp(sizeLimit) != 0 {
		t.Fatalf("emptyDir sizeLimit = %#v, want %s", volume.EmptyDir.SizeLimit, sizeLimit.String())
	}
}

func TestBuildPodSpecOmitsKubernetesProbes(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
procd_config:
  http_port: 41000
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if len(spec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}

	main := spec.Containers[0]
	if main.StartupProbe != nil || main.ReadinessProbe != nil || main.LivenessProbe != nil {
		t.Fatalf("expected no Kubernetes probes on sandbox pod, got startup=%#v readiness=%#v liveness=%#v", main.StartupProbe, main.ReadinessProbe, main.LivenessProbe)
	}
	port := findContainerPort(main.Ports, "http")
	if port == nil || port.ContainerPort != 41000 {
		t.Fatalf("expected named http port 41000, got %#v", port)
	}
}

func TestBuildPodSpecUsesRestartPolicyAlways(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if spec.RestartPolicy != corev1.RestartPolicyAlways {
		t.Fatalf("restartPolicy = %q, want %q", spec.RestartPolicy, corev1.RestartPolicyAlways)
	}
}

func TestBuildPodSpecUsesReducedRequestsAndQuotaLimits(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	resources := spec.Containers[0].Resources

	assertResourceQuantity(t, resources.Requests[corev1.ResourceCPU], "100m")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceCPU], "1")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceMemory], "256Mi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceMemory], "1Gi")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceEphemeralStorage], "1Gi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceEphemeralStorage], "8Gi")
}

func TestBuildResourceRequirementsKeepsMinimumCPURequestDense(t *testing.T) {
	resources := BuildResourceRequirements(ResourceQuota{
		CPU:    resource.MustParse("150m"),
		Memory: resource.MustParse("128Mi"),
	})

	assertResourceQuantity(t, resources.Requests[corev1.ResourceCPU], "10m")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceCPU], "150m")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceMemory], "64Mi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceMemory], "128Mi")
}

func TestBuildIdlePodSpecUsesLowRequestsAndTemplateLimits(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.MainContainer.Resources = ResourceQuota{
		CPU:    resource.MustParse("500m"),
		Memory: resource.MustParse("2Gi"),
	}

	spec := BuildIdlePodSpec(template)
	resources := spec.Containers[0].Resources

	assertResourceQuantity(t, resources.Requests[corev1.ResourceCPU], "10m")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceCPU], "500m")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceMemory], "64Mi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceMemory], "2Gi")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceEphemeralStorage], "1Gi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceEphemeralStorage], "8Gi")
}

func TestBuildPodSpecUsesRestartFreeResourceResizePolicy(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	policies := spec.Containers[0].ResizePolicy

	assertResizePolicy(t, policies, corev1.ResourceCPU, corev1.NotRequired)
	assertResizePolicy(t, policies, corev1.ResourceMemory, corev1.NotRequired)
}

func TestBuildPodSpecClampsReducedRequestsToSmallQuota(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.MainContainer.Resources = ResourceQuota{
		CPU:              resource.MustParse("5m"),
		Memory:           resource.MustParse("32Mi"),
		EphemeralStorage: resource.MustParse("32Mi"),
	}

	spec := BuildPodSpec(template)
	resources := spec.Containers[0].Resources

	assertResourceQuantity(t, resources.Requests[corev1.ResourceCPU], "5m")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceCPU], "5m")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceMemory], "32Mi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceMemory], "32Mi")
	assertResourceQuantity(t, resources.Requests[corev1.ResourceEphemeralStorage], "32Mi")
	assertResourceQuantity(t, resources.Limits[corev1.ResourceEphemeralStorage], "32Mi")
}

func assertResizePolicy(t *testing.T, policies []corev1.ContainerResizePolicy, resourceName corev1.ResourceName, want corev1.ResourceResizeRestartPolicy) {
	t.Helper()
	for _, policy := range policies {
		if policy.ResourceName == resourceName {
			if policy.RestartPolicy != want {
				t.Fatalf("resize policy %s = %s, want %s", resourceName, policy.RestartPolicy, want)
			}
			return
		}
	}
	t.Fatalf("missing resize policy for %s", resourceName)
}

func TestBuildPodSpecAddsSandboxReadinessGate(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if len(spec.ReadinessGates) != 1 {
		t.Fatalf("readiness gate count = %d, want 1", len(spec.ReadinessGates))
	}
	if spec.ReadinessGates[0].ConditionType != SandboxPodReadinessConditionType {
		t.Fatalf("readiness gate = %q, want %q", spec.ReadinessGates[0].ConditionType, SandboxPodReadinessConditionType)
	}
}

func TestBuildPodSpecInjectsNetdMITMCATrustMaterialIntoProcdContainer(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
netd_mitm_ca_secret_name: fullmode-netd-mitm-ca
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())

	volume := findVolume(spec.Volumes, netdMITMCAVolume)
	if volume == nil || volume.Secret == nil {
		t.Fatalf("expected %s secret volume to be injected", netdMITMCAVolume)
	}
	if volume.Secret.SecretName != "fullmode-netd-mitm-ca" {
		t.Fatalf("mitm ca secret = %q, want fullmode-netd-mitm-ca", volume.Secret.SecretName)
	}
	if len(volume.Secret.Items) != 1 || volume.Secret.Items[0].Key != netdMITMCACertKey || volume.Secret.Items[0].Path != "mitm-ca.crt" {
		t.Fatalf("unexpected secret items: %#v", volume.Secret.Items)
	}

	for _, name := range []string{"procd"} {
		container := findContainer(spec.Containers, name)
		if container == nil {
			t.Fatalf("expected container %q", name)
		}

		env := findEnvVar(container.Env, netdMITMCAEnvVar)
		if env == nil || env.Value != netdMITMCACertPath {
			t.Fatalf("%s env %s = %#v, want %q", name, netdMITMCAEnvVar, env, netdMITMCACertPath)
		}

		mount := findVolumeMount(container.VolumeMounts, netdMITMCAVolume)
		if mount == nil {
			t.Fatalf("expected %s mount on %s", netdMITMCAVolume, name)
		}
		if mount.MountPath != netdMITMCADir || !mount.ReadOnly {
			t.Fatalf("%s mount = %#v, want path %q readOnly", name, mount, netdMITMCADir)
		}
	}
}

func TestBuildPodSpecSkipsNetdMITMCATrustMaterialWhenManagerConfigOmitsSecret(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())

	if volume := findVolume(spec.Volumes, netdMITMCAVolume); volume != nil {
		t.Fatalf("expected %s volume to be absent, got %#v", netdMITMCAVolume, volume)
	}
	if env := findEnvVar(spec.Containers[0].Env, netdMITMCAEnvVar); env != nil {
		t.Fatalf("expected %s env to be absent, got %#v", netdMITMCAEnvVar, env)
	}
	if mount := findVolumeMount(spec.Containers[0].VolumeMounts, netdMITMCAVolume); mount != nil {
		t.Fatalf("expected %s mount to be absent, got %#v", netdMITMCAVolume, mount)
	}
}

func newTestTemplate() *SandboxTemplate {
	return &SandboxTemplate{
		ObjectMeta: metav1ObjectMeta("default"),
		Spec: SandboxTemplateSpec{
			MainContainer: ContainerSpec{
				Image: "busybox:latest",
				Resources: ResourceQuota{
					CPU:    resource.MustParse("1"),
					Memory: resource.MustParse("1Gi"),
				},
			},
			Pool: PoolStrategy{},
		},
	}
}

func metav1ObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
	}
}

func ptrBool(v bool) *bool {
	return &v
}

func ptrInt64(v int64) *int64 {
	return &v
}

func writeManagerConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}
	return path
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func findCSIVolumeByPortal(volumes []corev1.Volume, portalName string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].CSI == nil {
			continue
		}
		if volumes[i].CSI.VolumeAttributes[volumeportal.AttributePortalName] == portalName {
			return &volumes[i]
		}
	}
	return nil
}

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func findEnvVar(envVars []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range envVars {
		if envVars[i].Name == name {
			return &envVars[i]
		}
	}
	return nil
}

func findVolumeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}

func findVolumeMountByPath(mounts []corev1.VolumeMount, mountPath string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].MountPath == mountPath {
			return &mounts[i]
		}
	}
	return nil
}

func findContainerPort(ports []corev1.ContainerPort, name string) *corev1.ContainerPort {
	for i := range ports {
		if ports[i].Name == name {
			return &ports[i]
		}
	}
	return nil
}

func assertResourceQuantity(t *testing.T, got resource.Quantity, want string) {
	t.Helper()
	wantQuantity := resource.MustParse(want)
	if got.Cmp(wantQuantity) != 0 {
		t.Fatalf("resource quantity = %s, want %s", got.String(), wantQuantity.String())
	}
}

func TestBuildPodSpecOverridesManagerControlledProcdEnvVars(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
procd_config:
  root_path: /workspace
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.EnvVars = map[string]string{
		"root_path": "/tenant-override",
		"node_name": "tenant-node",
	}

	spec := BuildPodSpec(template)
	envByName := map[string]corev1.EnvVar{}
	for _, env := range spec.Containers[0].Env {
		envByName[env.Name] = env
	}

	if got := envByName["root_path"].Value; got != "/workspace" {
		t.Fatalf("root_path = %q, want manager-controlled value", got)
	}

	nodeName := envByName["node_name"]
	if nodeName.ValueFrom == nil || nodeName.ValueFrom.FieldRef == nil || nodeName.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
		t.Fatalf("expected node_name to come from pod fieldRef spec.nodeName")
	}
}

func TestBuildPodSpecKeepsProcessLogsDefaultAndOptOutEnv(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	const processLogsEnvVar = "SANDBOX0_PROCESS_LOGS"

	defaultSpec := BuildPodSpec(template)
	if len(defaultSpec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}
	if env := findEnvVar(defaultSpec.Containers[0].Env, processLogsEnvVar); env != nil {
		t.Fatalf("%s = %#v, want omitted because procd defaults it on", processLogsEnvVar, env)
	}

	template.Spec.EnvVars = map[string]string{processLogsEnvVar: "false"}

	spec := BuildPodSpec(template)
	if len(spec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}

	env := findEnvVar(spec.Containers[0].Env, processLogsEnvVar)
	if env == nil || env.Value != "false" {
		t.Fatalf("%s = %#v, want false", processLogsEnvVar, env)
	}
}

func TestBuildPodSpecKeepsTenantProcdEnvWhenManagerConfigUnset(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.EnvVars = map[string]string{
		"root_path": "/tenant-override",
		"node_name": "tenant-node",
	}

	spec := BuildPodSpec(template)
	envByName := map[string]corev1.EnvVar{}
	for _, env := range spec.Containers[0].Env {
		envByName[env.Name] = env
	}

	if got := envByName["root_path"].Value; got != "/tenant-override" {
		t.Fatalf("root_path = %q, want tenant value when manager config omits it", got)
	}

	nodeName := envByName["node_name"]
	if nodeName.ValueFrom == nil || nodeName.ValueFrom.FieldRef == nil || nodeName.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
		t.Fatalf("expected node_name to come from pod fieldRef spec.nodeName")
	}
}
