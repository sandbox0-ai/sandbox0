package v1alpha1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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

func TestBuildPodSpecAddsProcdProbes(t *testing.T) {
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
	if main.StartupProbe == nil || main.StartupProbe.HTTPGet == nil {
		t.Fatalf("expected procd startup probe, got %#v", main.StartupProbe)
	}
	if main.StartupProbe.HTTPGet.Path != "/healthz" {
		t.Fatalf("startup path = %q, want /healthz", main.StartupProbe.HTTPGet.Path)
	}
	if main.StartupProbe.HTTPGet.Port.StrVal != "http" {
		t.Fatalf("startup port = %#v, want named http port", main.StartupProbe.HTTPGet.Port)
	}
	if main.LivenessProbe == nil || main.LivenessProbe.HTTPGet == nil {
		t.Fatalf("expected procd liveness probe, got %#v", main.LivenessProbe)
	}
	if main.LivenessProbe.HTTPGet.Path != "/healthz" {
		t.Fatalf("liveness path = %q, want /healthz", main.LivenessProbe.HTTPGet.Path)
	}
	if main.LivenessProbe.HTTPGet.Port.StrVal != "http" {
		t.Fatalf("liveness port = %#v, want named http port", main.LivenessProbe.HTTPGet.Port)
	}
	if main.ReadinessProbe == nil || main.ReadinessProbe.HTTPGet == nil {
		t.Fatalf("expected procd readiness probe, got %#v", main.ReadinessProbe)
	}
	if main.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Fatalf("readiness path = %q, want /readyz", main.ReadinessProbe.HTTPGet.Path)
	}
	if main.ReadinessProbe.HTTPGet.Port.StrVal != "http" {
		t.Fatalf("readiness port = %#v, want named http port", main.ReadinessProbe.HTTPGet.Port)
	}
	port := findContainerPort(main.Ports, "http")
	if port == nil || port.ContainerPort != 41000 {
		t.Fatalf("expected named http port 41000, got %#v", port)
	}
}

func TestBuildPodSpecInjectsWarmProcessesEnv(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.WarmProcesses = []WarmProcessSpec{{
		Type:    WarmProcessTypeCMD,
		Command: []string{"/bin/sh", "-lc", "sleep 3600"},
		CWD:     "/workspace",
		EnvVars: map[string]string{"MODE": "warm"},
	}}

	spec := BuildPodSpec(template)
	if len(spec.Containers) != 1 {
		t.Fatalf("expected procd-only pod, got %d containers", len(spec.Containers))
	}

	main := spec.Containers[0]
	if main.Name != "procd" {
		t.Fatalf("expected main container to remain procd, got %q", main.Name)
	}
	env := findEnvVar(main.Env, WarmProcessesEnvVar)
	if env == nil || env.Value == "" {
		t.Fatalf("expected %s env var, got %#v", WarmProcessesEnvVar, env)
	}
	var decoded []WarmProcessSpec
	if err := json.Unmarshal([]byte(env.Value), &decoded); err != nil {
		t.Fatalf("warm process env is invalid JSON: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Type != WarmProcessTypeCMD {
		t.Fatalf("unexpected warm processes: %#v", decoded)
	}
	if len(decoded[0].Command) != 3 || decoded[0].Command[2] != "sleep 3600" {
		t.Fatalf("unexpected warm command: %#v", decoded[0].Command)
	}
}

func TestBuildPodSpecSkipsLivenessProbeWhenCtldEnabled(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
ctld_enabled: true
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate())
	if len(spec.Containers) == 0 {
		t.Fatal("expected at least one container")
	}

	main := spec.Containers[0]
	if main.StartupProbe == nil || main.ReadinessProbe == nil {
		t.Fatalf("expected startup and readiness probes, got startup=%#v readiness=%#v", main.StartupProbe, main.ReadinessProbe)
	}
	if main.LivenessProbe != nil {
		t.Fatalf("liveness probe = %#v, want nil when ctld is enabled", main.LivenessProbe)
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

func findContainerPort(ports []corev1.ContainerPort, name string) *corev1.ContainerPort {
	for i := range ports {
		if ports[i].Name == name {
			return &ports[i]
		}
	}
	return nil
}

func TestBuildPodSpecOverridesTenantStorageProxyEnvVars(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
procd_config:
  root_path: /workspace
  storage_proxy_base_url: storage-proxy.sandbox0-system.svc.cluster.local
  storage_proxy_port: 4001
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.EnvVars = map[string]string{
		"root_path":              "/tenant-override",
		"storage_proxy_base_url": "evil.local",
		"storage_proxy_port":     "65535",
		"node_name":              "tenant-node",
	}

	spec := BuildPodSpec(template)
	envByName := map[string]corev1.EnvVar{}
	for _, env := range spec.Containers[0].Env {
		envByName[env.Name] = env
	}

	if got := envByName["storage_proxy_base_url"].Value; got != "storage-proxy.sandbox0-system.svc.cluster.local" {
		t.Fatalf("storage_proxy_base_url = %q, want manager-controlled value", got)
	}
	if got := envByName["storage_proxy_port"].Value; got != "4001" {
		t.Fatalf("storage_proxy_port = %q, want manager-controlled value", got)
	}
	if got := envByName["root_path"].Value; got != "/workspace" {
		t.Fatalf("root_path = %q, want manager-controlled value", got)
	}

	nodeName := envByName["node_name"]
	if nodeName.ValueFrom == nil || nodeName.ValueFrom.FieldRef == nil || nodeName.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
		t.Fatalf("expected node_name to come from pod fieldRef spec.nodeName")
	}
}

func TestBuildPodSpecFailsClosedForStorageProxyEnvOverridesWhenManagerConfigUnset(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.EnvVars = map[string]string{
		"root_path":              "/tenant-override",
		"storage_proxy_base_url": "evil.local",
		"storage_proxy_port":     "65535",
		"node_name":              "tenant-node",
	}

	spec := BuildPodSpec(template)
	envByName := map[string]corev1.EnvVar{}
	for _, env := range spec.Containers[0].Env {
		envByName[env.Name] = env
	}

	if got := envByName["storage_proxy_base_url"].Value; got != "" {
		t.Fatalf("storage_proxy_base_url = %q, want empty manager-controlled value", got)
	}
	if got := envByName["storage_proxy_port"].Value; got != "0" {
		t.Fatalf("storage_proxy_port = %q, want 0 manager-controlled value", got)
	}
	if got := envByName["root_path"].Value; got != "/tenant-override" {
		t.Fatalf("root_path = %q, want tenant value when manager config omits it", got)
	}

	nodeName := envByName["node_name"]
	if nodeName.ValueFrom == nil || nodeName.ValueFrom.FieldRef == nil || nodeName.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
		t.Fatalf("expected node_name to come from pod fieldRef spec.nodeName")
	}
}
