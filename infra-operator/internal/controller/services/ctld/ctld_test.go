package ctld

import (
	"context"
	"strings"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
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

func TestCtldTerminationGraceCoversStaticShutdownBudget(t *testing.T) {
	const (
		shutdownBudgetSeconds = int64(5 + 7 + 25)
		shutdownMarginSeconds = int64(5)
	)

	assert.LessOrEqual(t, shutdownBudgetSeconds+shutdownMarginSeconds, ctldTerminationGraceSeconds)
}

func reconcileCtldDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()
	ds, _ := reconcileCtldResources(t, infra)
	return ds
}

func reconcileCtldResources(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, extraObjects ...ctrlclient.Object) (*appsv1.DaemonSet, ctrlclient.Client) {
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

	objects := []ctrlclient.Object{infra.DeepCopy()}
	objects = append(objects, extraObjects...)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", "http://demo-cluster-gateway:8443"); err != nil {
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
	assertCtldProbe(t, "liveness", ds.Spec.Template.Spec.Containers[0].LivenessProbe, "/healthz", 10)
	assertCtldProbe(t, "readiness", ds.Spec.Template.Spec.Containers[0].ReadinessProbe, "/readyz", 5)
	if ds.Spec.Template.Spec.ServiceAccountName != "demo-ctld" {
		t.Fatalf("expected service account demo-ctld, got %q", ds.Spec.Template.Spec.ServiceAccountName)
	}
	if ds.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *ds.Spec.Template.Spec.TerminationGracePeriodSeconds != ctldTerminationGraceSeconds {
		t.Fatalf("expected ctld termination grace %ds, got %#v", ctldTerminationGraceSeconds, ds.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-cri-endpoint=/host-run/containerd/containerd.sock")
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-containerd-data-root=/host-var-lib/containerd")
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-kubelet-pods-root=/var/lib/kubelet/pods")
	if ds.Spec.Template.Spec.Containers[0].SecurityContext == nil || ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged == nil || !*ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged {
		t.Fatal("expected ctld container to run privileged")
	}
	if got := ds.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; got.Cmp(resource.MustParse(ctldCPURequest)) != 0 {
		t.Fatalf("expected ctld cpu request %s, got %s", ctldCPURequest, got.String())
	}
	if got := ds.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]; got.Cmp(resource.MustParse(ctldMemoryRequest)) != 0 {
		t.Fatalf("expected ctld memory request %s, got %s", ctldMemoryRequest, got.String())
	}
	if len(ds.Spec.Template.Spec.Containers) != 2 || ds.Spec.Template.Spec.Containers[1].Name != "csi-node-driver-registrar" {
		t.Fatalf("expected csi node-driver-registrar sidecar, got %#v", ds.Spec.Template.Spec.Containers)
	}
	if len(ds.Spec.Template.Spec.Containers[0].VolumeMounts) < 6 {
		t.Fatalf("expected ctld config, csi, kubelet, data, containerd socket, and containerd data mounts, got %#v", ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
	assertContainerVolumeMount(t, ds.Spec.Template.Spec.Containers[0].VolumeMounts, "containerd-data", "/host-var-lib/containerd")
	driver := &storagev1.CSIDriver{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "volume.sandbox0.ai"}, driver); err != nil {
		t.Fatalf("expected csi driver to be created: %v", err)
	}
	if driver.Spec.PodInfoOnMount == nil || !*driver.Spec.PodInfoOnMount {
		t.Fatal("expected csi driver podInfoOnMount=true")
	}

	return ds, client
}

func TestCtldArgsEnableRecoverableNodeFS(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Ctld: &infrav1alpha1.CtldServiceConfig{
					NodeFSShardCount:    8,
					RequireFUSERecovery: true,
				},
			},
		},
	}
	args := ctldArgs(infra, "")
	assertContainsArg(t, args, "-nodefs-shards=8")
	assertContainsArg(t, args, "-nodefs-require-recovery=true")
}

func assertCtldProbe(t *testing.T, name string, probe *corev1.Probe, path string, periodSeconds int32) {
	t.Helper()

	if probe.HTTPGet == nil {
		t.Fatalf("expected ctld %s probe to use HTTP", name)
	}
	if probe.HTTPGet.Path != path {
		t.Fatalf("expected ctld %s probe path %s, got %s", name, path, probe.HTTPGet.Path)
	}
	if probe.HTTPGet.Port.StrVal != "http" {
		t.Fatalf("expected ctld %s probe to use http port, got %#v", name, probe.HTTPGet.Port)
	}
	if probe.PeriodSeconds != periodSeconds {
		t.Fatalf("expected ctld %s probe period %d, got %d", name, periodSeconds, probe.PeriodSeconds)
	}
	if probe.TimeoutSeconds != ctldProbeTimeoutSeconds {
		t.Fatalf("expected ctld %s probe timeout %d, got %d", name, ctldProbeTimeoutSeconds, probe.TimeoutSeconds)
	}
	if probe.FailureThreshold != ctldProbeFailureThreshold {
		t.Fatalf("expected ctld %s probe failure threshold %d, got %d", name, ctldProbeFailureThreshold, probe.FailureThreshold)
	}
}

func TestReconcileUsesDefaultContainerdHostDataRoot(t *testing.T) {
	infra := newCtldTestInfra()

	ds := reconcileCtldDaemonSet(t, infra)
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-containerd-host-data-root=/var/lib/containerd")
	assertHostPathVolume(t, ds.Spec.Template.Spec.Volumes, "containerd-data", "/var/lib/containerd")
}

func TestReconcileUsesConfiguredContainerdHostDataRoot(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.Services.Ctld = &infrav1alpha1.CtldServiceConfig{
		ContainerdHostDataRoot: "/var/lib/sandbox0-worker/containerd",
	}

	ds := reconcileCtldDaemonSet(t, infra)
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-containerd-data-root=/host-var-lib/containerd")
	assertContainsArg(t, args, "-containerd-host-data-root=/var/lib/sandbox0-worker/containerd")
	assertHostPathVolume(t, ds.Spec.Template.Spec.Volumes, "containerd-data", "/var/lib/sandbox0-worker/containerd")
}

func TestReconcilePassesRootFSObjectCacheConfig(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.Services.Ctld = &infrav1alpha1.CtldServiceConfig{
		RootFSObjectCacheMaxBytes:      "10Gi",
		RootFSObjectCacheMinFreeBytes:  "2Gi",
		RootFSObjectCacheMaxAge:        metav1.Duration{Duration: 6 * time.Hour},
		RootFSObjectCacheSweepInterval: metav1.Duration{Duration: 30 * time.Second},
	}

	ds := reconcileCtldDaemonSet(t, infra)
	args := ds.Spec.Template.Spec.Containers[0].Args
	assertContainsArg(t, args, "-rootfs-object-cache-max-bytes=10Gi")
	assertContainsArg(t, args, "-rootfs-object-cache-min-free-bytes=2Gi")
	assertContainsArg(t, args, "-rootfs-object-cache-max-age=6h0m0s")
	assertContainsArg(t, args, "-rootfs-object-cache-sweep-interval=30s")
}

func TestReconcileDoesNotPassPauseConfigToCtld(t *testing.T) {
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
	assertNotContainsArgPrefix(t, args, "-pause-min-memory-request=")
	assertNotContainsArgPrefix(t, args, "-pause-min-memory-limit=")
	assertNotContainsArgPrefix(t, args, "-pause-memory-buffer-ratio=")
	assertNotContainsArgPrefix(t, args, "-pause-min-cpu=")
	assertNotContainsArgPrefix(t, args, "-default-sandbox-ttl=")
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

func TestReconcileDoesNotMountInternalJWTKeyWhenRuntimeSamplesDisabled(t *testing.T) {
	ds := reconcileCtldDaemonSet(t, newCtldTestInfra())

	assertNoContainerVolumeMount(t, ds.Spec.Template.Spec.Containers[0].VolumeMounts, "internal-jwt-private-key")
}

func TestReconcileInjectsRuntimeSampleProducerConfigAndJWTKey(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.SandboxObservability = &infrav1alpha1.SandboxObservabilityConfig{
		Type: infrav1alpha1.SandboxObservabilityTypeExternal,
		External: &infrav1alpha1.ExternalSandboxObservabilityConfig{
			ClickHouse: infrav1alpha1.ExternalSandboxObservabilityClickHouseConfig{
				DSNSecret: infrav1alpha1.SandboxObservabilityClickHouseDSNSecretRef{Name: "sandbox-observability-dsn"},
			},
		},
		Ingest: infrav1alpha1.SandboxObservabilityIngestConfig{
			QueueSize:      2048,
			BatchSize:      64,
			FlushInterval:  metav1.Duration{Duration: 2 * time.Second},
			RequestTimeout: metav1.Duration{Duration: 3 * time.Second},
			MaxRetries:     4,
			RetryBackoff:   metav1.Duration{Duration: 250 * time.Millisecond},
		},
	}
	dsnSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-observability-dsn", Namespace: infra.Namespace},
		Data:       map[string][]byte{"dsn": []byte("clickhouse://sandbox0:password@clickhouse:9000/sandbox0_observability")},
	}

	ds, client := reconcileCtldResources(t, infra, dsnSecret)
	ctldContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerVolumeMount(t, ctldContainer.VolumeMounts, "internal-jwt-private-key", "/secrets/internal_jwt_private.key")
	assertSecretVolume(t, ds.Spec.Template.Spec.Volumes, "internal-jwt-private-key")

	configMapName := ""
	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == "config" && volume.ConfigMap != nil {
			configMapName = volume.ConfigMap.Name
			break
		}
	}
	require.NotEmpty(t, configMapName)
	configMap := &corev1.ConfigMap{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: configMapName, Namespace: infra.Namespace}, configMap))
	cfg := &apiconfig.CtldConfig{}
	require.NoError(t, yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), cfg))
	assert.Equal(t, "http://demo-cluster-gateway:8443/internal/v1/sandbox-observability/runtime-samples", cfg.SandboxObservabilityRuntimeSamplesIngestURL)
	assert.Equal(t, 2048, cfg.SandboxObservabilityIngestQueueSize)
	assert.Equal(t, 64, cfg.SandboxObservabilityIngestBatchSize)
	assert.Equal(t, 2*time.Second, cfg.SandboxObservabilityIngestFlushInterval.Duration)
	assert.Equal(t, 3*time.Second, cfg.SandboxObservabilityIngestRequestTimeout.Duration)
}

func TestBuildStorageConfigDefaultsDataPlaneIdentity(t *testing.T) {
	infra := newCtldTestInfra()
	infra.Spec.PublicExposure = &infrav1alpha1.PublicExposureConfig{
		RegionID: "aws-us-east-1",
	}

	reconciler := NewReconciler(common.NewResourceManager(nil, nil, nil, common.LocalDevConfig{}))
	cfg, err := reconciler.buildStorageConfig(context.Background(), infra)
	if err != nil {
		t.Fatalf("buildStorageConfig returned error: %v", err)
	}
	if cfg.RegionID != "aws-us-east-1" {
		t.Fatalf("region_id = %q, want aws-us-east-1", cfg.RegionID)
	}
	if cfg.DefaultClusterId != naming.DefaultClusterID {
		t.Fatalf("default_cluster_id = %q, want %q", cfg.DefaultClusterId, naming.DefaultClusterID)
	}
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

func assertNotContainsArgPrefix(t *testing.T, args []string, prefix string) {
	t.Helper()
	for _, arg := range args {
		if strings.HasPrefix(arg, prefix) {
			t.Fatalf("expected args to omit prefix %q, got %#v", prefix, args)
		}
	}
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

func assertNoContainerVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name string) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			t.Fatalf("expected volume mount %q to be absent, got %#v", name, mounts)
		}
	}
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

func assertHostPathVolume(t *testing.T, volumes []corev1.Volume, name, path string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != name {
			continue
		}
		if volume.HostPath == nil {
			t.Fatalf("volume %q is not a hostPath volume: %#v", name, volume)
		}
		if volume.HostPath.Path != path {
			t.Fatalf("hostPath volume %q path = %q, want %q", name, volume.HostPath.Path, path)
		}
		return
	}
	t.Fatalf("expected volume %q, got %#v", name, volumes)
}

func assertSecretVolume(t *testing.T, volumes []corev1.Volume, name string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != name {
			continue
		}
		if volume.Secret == nil || volume.Secret.SecretName == "" {
			t.Fatalf("volume %q is not backed by a secret: %#v", name, volume)
		}
		return
	}
	t.Fatalf("expected secret volume %q, got %#v", name, volumes)
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
				Ctld: &infrav1alpha1.CtldServiceConfig{},
				Netd: &infrav1alpha1.NetdServiceConfig{},
			},
		},
	}
}
