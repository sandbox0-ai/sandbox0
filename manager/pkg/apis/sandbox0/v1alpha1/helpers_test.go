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

func TestBuildPodSpecSanitizesSidecarSecurityContext(t *testing.T) {
	configPath := writeManagerConfig(t, `
manager_image: sandbox0/manager:test
`)
	t.Setenv("CONFIG_PATH", configPath)

	runAsUser := int64(1000)
	runAsGroup := int64(1001)
	allowPrivilegeEscalation := true
	privileged := true

	template := newTestTemplate()
	template.Spec.Sidecars = []corev1.Container{
		{
			Name:  "sidecar",
			Image: "busybox:latest",
			SecurityContext: &corev1.SecurityContext{
				RunAsUser:                &runAsUser,
				RunAsGroup:               &runAsGroup,
				AllowPrivilegeEscalation: &allowPrivilegeEscalation,
				Privileged:               &privileged,
				Capabilities: &corev1.Capabilities{
					Add:  []corev1.Capability{"NET_ADMIN"},
					Drop: []corev1.Capability{"NET_RAW"},
				},
			},
		},
	}

	spec := BuildPodSpec(template, false)
	if len(spec.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(spec.Containers))
	}

	sidecar := spec.Containers[1]
	if sidecar.SecurityContext == nil {
		t.Fatal("expected sidecar security context")
	}
	if sidecar.SecurityContext.RunAsUser == nil || *sidecar.SecurityContext.RunAsUser != runAsUser {
		t.Fatalf("expected runAsUser %d, got %v", runAsUser, sidecar.SecurityContext.RunAsUser)
	}
	if sidecar.SecurityContext.RunAsGroup == nil || *sidecar.SecurityContext.RunAsGroup != runAsGroup {
		t.Fatalf("expected runAsGroup %d, got %v", runAsGroup, sidecar.SecurityContext.RunAsGroup)
	}
	if sidecar.SecurityContext.AllowPrivilegeEscalation != nil {
		t.Fatalf("expected allowPrivilegeEscalation to be stripped, got %v", *sidecar.SecurityContext.AllowPrivilegeEscalation)
	}
	if sidecar.SecurityContext.Privileged != nil {
		t.Fatalf("expected privileged to be stripped, got %v", *sidecar.SecurityContext.Privileged)
	}
	if sidecar.SecurityContext.Capabilities == nil {
		t.Fatal("expected capabilities to exist")
	}
	if len(sidecar.SecurityContext.Capabilities.Add) != 0 {
		t.Fatalf("expected capabilities.add to be stripped, got %v", sidecar.SecurityContext.Capabilities.Add)
	}
	if len(sidecar.SecurityContext.Capabilities.Drop) != 1 || sidecar.SecurityContext.Capabilities.Drop[0] != "NET_RAW" {
		t.Fatalf("expected capabilities.drop to be preserved, got %v", sidecar.SecurityContext.Capabilities.Drop)
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
