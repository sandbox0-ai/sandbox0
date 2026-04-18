package fuseplugin

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestReconcileUsesSharedSandboxNodePlacement(t *testing.T) {
	infra := newFusePluginTestInfra()
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

	ds := reconcileFusePluginDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared toleration, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func TestReconcileFallsBackToLegacyNetdPlacement(t *testing.T) {
	infra := newFusePluginTestInfra()
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

	ds := reconcileFusePluginDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "legacy" {
		t.Fatalf("expected legacy node selector fallback, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Value != "gvisor" {
		t.Fatalf("expected legacy toleration fallback, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func reconcileFusePluginDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1 scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
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
	if len(ds.Spec.Template.Spec.Containers[0].Args) < 3 || ds.Spec.Template.Spec.Containers[0].Args[1] != "-cgroup-root=/host-sys/fs/cgroup" || ds.Spec.Template.Spec.Containers[0].Args[2] != "-cri-endpoint=/host-run/containerd/containerd.sock" {
		t.Fatalf("expected cgroup root arg, got %#v", ds.Spec.Template.Spec.Containers[0].Args)
	}
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-volume-staging-root=/host-var/lib/sandbox0/volumes")
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-volume-cache-root=/host-var/lib/sandbox0/juicefs-cache")
	if ds.Spec.Template.Spec.Containers[0].SecurityContext == nil || ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged == nil || !*ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged {
		t.Fatal("expected ctld container to run privileged")
	}
	if !infrav1alpha1.IsStorageProxyEnabled(infra) && len(ds.Spec.Template.Spec.Containers[0].VolumeMounts) != 5 {
		t.Fatalf("expected device-plugin, host-cgroup, containerd, volume staging, and cache mounts, got %#v", ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}

	return ds
}

func TestReconcilePassesDefaultPauseConfigToCtld(t *testing.T) {
	ds := reconcileFusePluginDaemonSet(t, newFusePluginTestInfra())
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-pause-min-memory-request=10Mi")
	assertContainsArg(t, args, "-pause-min-memory-limit=32Mi")
	assertContainsArg(t, args, "-pause-memory-buffer-ratio=1.1")
	assertContainsArg(t, args, "-pause-min-cpu=10m")
	assertContainsArg(t, args, "-default-sandbox-ttl=0s")
}

func TestReconcilePassesPauseConfigToCtld(t *testing.T) {
	infra := newFusePluginTestInfra()
	infra.Spec.Services.Manager.Config = &infrav1alpha1.ManagerConfig{
		PauseMinMemoryRequest:  "24Mi",
		PauseMinMemoryLimit:    "96Mi",
		PauseMemoryBufferRatio: "1.4",
		PauseMinCPU:            "25m",
		DefaultSandboxTTL:      metav1.Duration{Duration: 5 * time.Minute},
	}

	ds := reconcileFusePluginDaemonSet(t, infra)
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-pause-min-memory-request=24Mi")
	assertContainsArg(t, args, "-pause-min-memory-limit=96Mi")
	assertContainsArg(t, args, "-pause-memory-buffer-ratio=1.4")
	assertContainsArg(t, args, "-pause-min-cpu=25m")
	assertContainsArg(t, args, "-default-sandbox-ttl=5m0s")
}

func TestReconcileMountsStorageProxyConfigWhenStorageProxyEnabled(t *testing.T) {
	infra := newFusePluginTestInfra()
	infra.Spec.Services.StorageProxy = &infrav1alpha1.StorageProxyServiceConfig{
		WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
			EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
		},
	}

	ds := reconcileFusePluginDaemonSet(t, infra)
	container := ds.Spec.Template.Spec.Containers[0]
	assertContainsArg(t, container.Args, "-volume-config=/config/storage-proxy/config.yaml")

	var foundMount bool
	for _, mount := range container.VolumeMounts {
		if mount.Name == "storage-proxy-config" {
			foundMount = true
			if mount.MountPath != "/config/storage-proxy/config.yaml" || mount.SubPath != "config.yaml" || !mount.ReadOnly {
				t.Fatalf("unexpected storage-proxy config mount: %#v", mount)
			}
		}
	}
	if !foundMount {
		t.Fatalf("expected ctld to mount storage-proxy config, got %#v", container.VolumeMounts)
	}

	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == "storage-proxy-config" {
			if volume.ConfigMap == nil || volume.ConfigMap.Name != "demo-storage-proxy" {
				t.Fatalf("unexpected storage-proxy config volume: %#v", volume)
			}
			return
		}
	}
	t.Fatalf("expected ctld storage-proxy config volume, got %#v", ds.Spec.Template.Spec.Volumes)
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

func newFusePluginTestInfra() *infrav1alpha1.Sandbox0Infra {
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
