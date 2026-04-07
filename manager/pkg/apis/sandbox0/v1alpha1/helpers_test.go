package v1alpha1

import (
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

	spec := BuildPodSpec(newTestTemplate(), true)

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

	spec := BuildPodSpec(template, false)

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

func TestBuildPodSpecMountsSharedTemplateVolumes(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.SharedVolumes = []SharedVolumeSpec{
		{
			Name:            "workspace",
			SandboxVolumeID: "vol-1",
			MountPath:       "/workspace",
		},
	}
	template.Spec.Sidecars = []SidecarContainerSpec{
		{
			Name:      "sidecar",
			Image:     "busybox:latest",
			Resources: ResourceQuota{CPU: resource.MustParse("500m"), Memory: resource.MustParse("2Gi")},
			Mounts: []ContainerMountSpec{{
				Name:      "workspace",
				MountPath: "/mnt/workspace",
			}},
		},
	}

	spec := BuildPodSpec(template, false)
	if len(spec.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(spec.Containers))
	}

	volume := findVolume(spec.Volumes, sharedTemplateVolumeName(0))
	if volume == nil || volume.EmptyDir == nil {
		t.Fatalf("expected shared template emptyDir volume, got %#v", volume)
	}

	main := spec.Containers[0]
	mainMount := findVolumeMount(main.VolumeMounts, sharedTemplateVolumeName(0))
	if mainMount == nil || mainMount.MountPath != "/workspace" {
		t.Fatalf("expected main shared mount at /workspace, got %#v", mainMount)
	}
	if main.SecurityContext == nil || main.SecurityContext.Privileged == nil || !*main.SecurityContext.Privileged {
		t.Fatalf("expected shared-volume main container to be privileged, got %#v", main.SecurityContext)
	}
	if mainMount.MountPropagation == nil || *mainMount.MountPropagation != corev1.MountPropagationBidirectional {
		t.Fatalf("expected main mount propagation bidirectional, got %#v", mainMount.MountPropagation)
	}

	sidecar := spec.Containers[1]
	sidecarMount := findVolumeMount(sidecar.VolumeMounts, sharedTemplateVolumeName(0))
	if sidecarMount == nil || sidecarMount.MountPath != "/mnt/workspace" {
		t.Fatalf("expected sidecar shared mount at /mnt/workspace, got %#v", sidecarMount)
	}
	if sidecarMount.MountPropagation == nil || *sidecarMount.MountPropagation != corev1.MountPropagationHostToContainer {
		t.Fatalf("expected sidecar mount propagation host-to-container, got %#v", sidecarMount.MountPropagation)
	}
}

func TestBuildPodSpecAppliesConfiguredSharedVolumeRuntimeClass(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
shared_volume_runtime_class_name: kata-shared
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.SharedVolumes = []SharedVolumeSpec{{
		Name:            "workspace",
		SandboxVolumeID: "vol-1",
		MountPath:       "/workspace",
	}}

	spec := BuildPodSpec(template, false)
	if spec.RuntimeClassName == nil || *spec.RuntimeClassName != "kata-shared" {
		t.Fatalf("expected shared-volume runtime class kata-shared, got %#v", spec.RuntimeClassName)
	}
}

func TestBuildPodSpecLeavesOrdinarySandboxNonPrivileged(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	spec := BuildPodSpec(newTestTemplate(), false)
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

func TestBuildPodSpecPreservesSidecarCommandAndProbes(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.Sidecars = []SidecarContainerSpec{
		{
			Name:      "codex",
			Image:     "busybox:latest",
			Command:   []string{"sh", "-lc", "sleep 30; touch /tmp/ready; tail -f /dev/null"},
			Args:      []string{"--verbose"},
			Resources: ResourceQuota{CPU: resource.MustParse("500m"), Memory: resource.MustParse("2Gi")},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}},
				},
				PeriodSeconds: 2,
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/healthy"}},
				},
				PeriodSeconds: 5,
			},
			StartupProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/started"}},
				},
				FailureThreshold: 30,
			},
		},
	}

	spec := BuildPodSpec(template, false)
	if len(spec.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(spec.Containers))
	}

	main := spec.Containers[0]
	if main.Name != "procd" {
		t.Fatalf("expected main container to remain procd, got %q", main.Name)
	}

	sidecar := spec.Containers[1]
	if sidecar.Name != "codex" {
		t.Fatalf("expected sidecar name codex, got %q", sidecar.Name)
	}
	if len(sidecar.Command) != 3 || sidecar.Command[0] != "sh" || sidecar.Command[1] != "-lc" {
		t.Fatalf("unexpected sidecar command: %v", sidecar.Command)
	}
	if len(sidecar.Args) != 1 || sidecar.Args[0] != "--verbose" {
		t.Fatalf("unexpected sidecar args: %v", sidecar.Args)
	}
	if sidecar.ReadinessProbe == nil || sidecar.ReadinessProbe.Exec == nil {
		t.Fatal("expected readiness probe to be preserved")
	}
	if sidecar.LivenessProbe == nil || sidecar.LivenessProbe.Exec == nil {
		t.Fatal("expected liveness probe to be preserved")
	}
	if sidecar.StartupProbe == nil || sidecar.StartupProbe.Exec == nil {
		t.Fatal("expected startup probe to be preserved")
	}
	if sidecar.ReadinessProbe.PeriodSeconds != 2 {
		t.Fatalf("readiness period = %d, want 2", sidecar.ReadinessProbe.PeriodSeconds)
	}
	if sidecar.LivenessProbe.PeriodSeconds != 5 {
		t.Fatalf("liveness period = %d, want 5", sidecar.LivenessProbe.PeriodSeconds)
	}
	if sidecar.StartupProbe.FailureThreshold != 30 {
		t.Fatalf("startup failureThreshold = %d, want 30", sidecar.StartupProbe.FailureThreshold)
	}
}

func TestBuildPodSpecInjectsNetdMITMCATrustMaterialIntoAllContainers(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
netd_mitm_ca_secret_name: fullmode-netd-mitm-ca
`)
	t.Setenv("CONFIG_PATH", configPath)

	template := newTestTemplate()
	template.Spec.Sidecars = []SidecarContainerSpec{
		{
			Name:      "sidecar",
			Image:     "busybox:latest",
			Resources: ResourceQuota{CPU: resource.MustParse("500m"), Memory: resource.MustParse("2Gi")},
			Env: []EnvVar{
				{Name: netdMITMCAEnvVar, Value: "/tmp/ignored.crt"},
			},
		},
	}

	spec := BuildPodSpec(template, false)

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

	for _, name := range []string{"procd", "sidecar"} {
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

	spec := BuildPodSpec(newTestTemplate(), false)

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

	spec := BuildPodSpec(template, false)
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

	spec := BuildPodSpec(template, false)
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
