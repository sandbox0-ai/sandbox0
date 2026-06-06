package ctld

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestReconcileUsesSharedSandboxNodePlacement(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.SandboxNodePlacement = &infrav1alpha1.SandboxNodePlacementConfig{
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
	}
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}

	ds := reconcileCtldDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared toleration, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func TestReconcileFallsBackToLegacyNetdPlacement(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}
	infra.Spec.Services.Netd.Tolerations = []corev1.Toleration{
		{
			Key:      "sandbox.gke.io/runtime",
			Operator: corev1.TolerationOpEqual,
			Value:    "gvisor",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	ds := reconcileCtldDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "legacy" {
		t.Fatalf("expected legacy node selector fallback, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Value != "gvisor" {
		t.Fatalf("expected legacy toleration fallback, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func reconcileCtldDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1 scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add storagev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra.DeepCopy()).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest"); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-ctld",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset to be created: %v", err)
	}

	if got := ds.Spec.Template.Spec.Containers[0].Env[0].Value; got != "ctld" {
		t.Fatalf("expected SERVICE=ctld, got %q", got)
	}
	if ds.Spec.Template.Spec.Containers[0].ReadinessProbe == nil || ds.Spec.Template.Spec.Containers[0].LivenessProbe == nil {
		t.Fatal("expected ctld probes to be configured")
	}
	if ds.Spec.Template.Spec.ServiceAccountName != "demo-ctld" {
		t.Fatalf("expected service account demo-ctld, got %q", ds.Spec.Template.Spec.ServiceAccountName)
	}
	if len(ds.Spec.Template.Spec.Containers[0].Args) < 5 || ds.Spec.Template.Spec.Containers[0].Args[1] != "-cgroup-root=/host-sys/fs/cgroup" || ds.Spec.Template.Spec.Containers[0].Args[2] != "-cri-endpoint=/host-run/containerd/containerd.sock" {
		t.Fatalf("expected cgroup root arg, got %#v", ds.Spec.Template.Spec.Containers[0].Args)
	}
	if ds.Spec.Template.Spec.Containers[0].SecurityContext == nil || ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged == nil || !*ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged {
		t.Fatal("expected ctld container to run privileged")
	}
	if len(ds.Spec.Template.Spec.Containers) != 2 || ds.Spec.Template.Spec.Containers[1].Name != "csi-node-driver-registrar" {
		t.Fatalf("expected csi node-driver-registrar sidecar, got %#v", ds.Spec.Template.Spec.Containers)
	}
	if len(ds.Spec.Template.Spec.Containers[0].VolumeMounts) < 6 {
		t.Fatalf("expected ctld config, csi, kubelet, data, cgroup, and containerd mounts, got %#v", ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
	driver := &storagev1.CSIDriver{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "volume.sandbox0.ai"}, driver); err != nil {
		t.Fatalf("expected csi driver to be created: %v", err)
	}
	if driver.Spec.PodInfoOnMount == nil || !*driver.Spec.PodInfoOnMount {
		t.Fatal("expected csi driver podInfoOnMount=true")
	}

	return ds
}

func TestReconcilePassesDefaultPauseConfigToCtld(t *testing.T) {
	ds := reconcileCtldDaemonSet(t, newCtldTestInfra())
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-pause-min-memory-request=10Mi")
	assertContainsArg(t, args, "-pause-min-memory-limit=32Mi")
	assertContainsArg(t, args, "-pause-memory-buffer-ratio=1.1")
	assertContainsArg(t, args, "-pause-min-cpu=10m")
	assertContainsArg(t, args, "-default-sandbox-ttl=0s")
}

func TestReconcilePassesPauseConfigToCtld(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.Services.Manager.Config = &infrav1alpha1.ManagerConfig{
		PauseMinMemoryRequest:  "24Mi",
		PauseMinMemoryLimit:    "96Mi",
		PauseMemoryBufferRatio: "1.4",
		PauseMinCPU:            "25m",
		DefaultSandboxTTL:      metav1.Duration{Duration: 5 * time.Minute},
	}

	ds := reconcileCtldDaemonSet(t, infra)
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-pause-min-memory-request=24Mi")
	assertContainsArg(t, args, "-pause-min-memory-limit=96Mi")
	assertContainsArg(t, args, "-pause-memory-buffer-ratio=1.4")
	assertContainsArg(t, args, "-pause-min-cpu=25m")
	assertContainsArg(t, args, "-default-sandbox-ttl=5m0s")
}

func TestReconcileMountsObjectEncryptionKeyWhenEnabled(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.Services.StorageProxy = &infrav1alpha1.StorageProxyServiceConfig{
		Config: &infrav1alpha1.StorageProxyConfig{
			ObjectEncryptionEnabled: true,
		},
	}

	ds := reconcileCtldDaemonSet(t, infra)
	assertContainerVolumeMount(t, ds.Spec.Template.Spec.Containers[0].VolumeMounts, "object-encryption-key", common.ObjectEncryptionMountDir)
	assertPodVolume(t, ds.Spec.Template.Spec.Volumes, "object-encryption-key")
}

func assertContainsArg(t *testing.T, args []string, want string) {
	t.Helper()
	for _, arg := range args {
		if arg == want {
			return
		}
	}
	t.Fatalf("expected args to contain %q, got %#v", want, args)
}

func assertContainerVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name, mountPath string) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			if mount.MountPath != mountPath {
				t.Fatalf("volume mount %q path = %q, want %q", name, mount.MountPath, mountPath)
			}
			return
		}
	}
	t.Fatalf("expected volume mount %q, got %#v", name, mounts)
}

func assertPodVolume(t *testing.T, volumes []corev1.Volume, name string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return
		}
	}
	t.Fatalf("expected volume %q, got %#v", name, volumes)
}

func newCtldTestInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{},
			},
		},
	}
}
