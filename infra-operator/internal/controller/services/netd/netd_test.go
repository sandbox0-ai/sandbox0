package netd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestReconcileUsesSharedSandboxNodePlacement(t *testing.T) {
	sharedRuntime := "shared"
	legacyRuntime := "legacy"
	infra := newNetdTestInfra()
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
	infra.Spec.Services.Netd.Tolerations = []corev1.Toleration{
		{
			Key:      "sandbox.gke.io/runtime",
			Operator: corev1.TolerationOpEqual,
			Value:    legacyRuntime,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	ds := reconcileNetdDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared toleration, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
	if ds.Spec.Template.Spec.RuntimeClassName == nil || *ds.Spec.Template.Spec.RuntimeClassName != sharedRuntime {
		t.Fatalf("expected runtimeClassName %q, got %#v", sharedRuntime, ds.Spec.Template.Spec.RuntimeClassName)
	}
}

func TestReconcileFallsBackToLegacyNetdPlacement(t *testing.T) {
	legacyRuntime := "legacy"
	infra := newNetdTestInfra()
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}
	infra.Spec.Services.Netd.Tolerations = []corev1.Toleration{
		{
			Key:      "sandbox.gke.io/runtime",
			Operator: corev1.TolerationOpEqual,
			Value:    legacyRuntime,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	ds := reconcileNetdDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "legacy" {
		t.Fatalf("expected legacy node selector fallback, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Value != legacyRuntime {
		t.Fatalf("expected legacy toleration fallback, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func TestReconcileMountsExplicitMITMCASecret(t *testing.T) {
	infra := newNetdTestInfra()
	infra.Spec.Services.Netd.MITMCASecretName = "netd-mitm-ca"
	client, scheme := newNetdTestClient(t, infra.DeepCopy(), newExplicitMITMCASecret(t, infra, "netd-mitm-ca"))

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	assertNetdMITMSecretMounted(t, client, infra, "netd-mitm-ca")
}

func TestReconcileAutoGeneratesManagedMITMCASecret(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	secretName := managedMITMCASecretName(infra.Name)
	assertNetdMITMSecretMounted(t, client, infra, secretName)

	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected managed secret: %v", err)
	}
	assertValidManagedMITMCASecret(t, secret)
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].Name != infra.Name {
		t.Fatalf("expected secret to be owned by infra, got %#v", secret.OwnerReferences)
	}
}

func TestReconcileReusesManagedMITMCASecretAcrossReconciles(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("first reconcile returned error: %v", err)
	}

	secretName := managedMITMCASecretName(infra.Name)
	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected managed secret after first reconcile: %v", err)
	}
	firstCert := append([]byte(nil), secret.Data[mitmCACertKey]...)
	firstKey := append([]byte(nil), secret.Data[mitmCAKeyKey]...)

	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("second reconcile returned error: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected managed secret after second reconcile: %v", err)
	}
	if string(secret.Data[mitmCACertKey]) != string(firstCert) || string(secret.Data[mitmCAKeyKey]) != string(firstKey) {
		t.Fatalf("expected managed secret to be reused across reconciles")
	}
}

func TestReconcileRepairsInvalidManagedMITMCASecret(t *testing.T) {
	infra := newNetdTestInfra()
	invalidSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      managedMITMCASecretName(infra.Name),
			Namespace: infra.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			mitmCACertKey: []byte("invalid cert"),
			mitmCAKeyKey:  []byte("invalid key"),
		},
	}
	client, scheme := newNetdTestClient(t, infra.DeepCopy(), invalidSecret)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      managedMITMCASecretName(infra.Name),
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected repaired secret: %v", err)
	}
	assertValidManagedMITMCASecret(t, secret)
}

func TestReconcileExplicitMITMCASecretSkipsManagedSecret(t *testing.T) {
	infra := newNetdTestInfra()
	infra.Spec.Services.Netd.MITMCASecretName = "custom-mitm-ca"
	client, scheme := newNetdTestClient(t, infra.DeepCopy(), newExplicitMITMCASecret(t, infra, "custom-mitm-ca"))
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	assertNetdMITMSecretMounted(t, client, infra, "custom-mitm-ca")

	managedSecret := &corev1.Secret{}
	err := client.Get(context.Background(), types.NamespacedName{
		Name:      managedMITMCASecretName(infra.Name),
		Namespace: infra.Namespace,
	}, managedSecret)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no managed secret when explicit secret is configured, got %v", err)
	}
}

func reconcileNetdDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()

	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset to be created: %v", err)
	}
	wantLockPath := filepath.Join(ActiveLockMountDirectory, infra.Namespace, infra.Name, "netd.lock")
	if got := containerEnvValue(ds.Spec.Template.Spec.Containers[0], ActiveLockEnv); got != wantLockPath {
		t.Fatalf("active lock path = %q, want %q", got, wantLockPath)
	}
	if !netdHasVolume(ds, ActiveLockVolumeName) || !netdHasMount(ds, ActiveLockVolumeName) {
		t.Fatalf("legacy handoff lock volume is missing: volumes=%#v mounts=%#v", ds.Spec.Template.Spec.Volumes, ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
	if got := ds.Annotations[LegacyHandoffStateAnnotation]; got != LegacyHandoffStateActive {
		t.Fatalf("legacy handoff state = %q, want %q", got, LegacyHandoffStateActive)
	}
	container := ds.Spec.Template.Spec.Containers[0]
	if container.LivenessProbe == nil || container.ReadinessProbe == nil ||
		container.LivenessProbe.HTTPGet == nil || container.ReadinessProbe.HTTPGet == nil ||
		container.LivenessProbe.HTTPGet.Port.StrVal != "health" || container.ReadinessProbe.HTTPGet.Port.StrVal != "health" {
		t.Fatalf("legacy active probes do not target the named health port: %#v %#v", container.LivenessProbe, container.ReadinessProbe)
	}

	return ds
}

func TestPrepareLegacyStandbyPreservesFallbackUntilReady(t *testing.T) {
	ctx := context.Background()
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)
	if err := reconciler.PrepareLegacyHandoff(ctx, "ghcr.io/sandbox0-ai/sandbox0", "v2", compiled); err != nil {
		t.Fatalf("PrepareLegacyHandoff() error = %v", err)
	}
	key := types.NamespacedName{Name: infra.Name + "-netd", Namespace: infra.Namespace}
	active := &appsv1.DaemonSet{}
	if err := client.Get(ctx, key, active); err != nil {
		t.Fatalf("get validated active daemonset: %v", err)
	}
	activeImage := active.Spec.Template.Spec.Containers[0].Image
	activeConfigMap := netdConfigMapName(t, active)
	if err := reconciler.PrepareLegacyStandby(ctx, compiled); err != nil {
		t.Fatalf("PrepareLegacyStandby() error = %v", err)
	}

	ds := &appsv1.DaemonSet{}
	if err := client.Get(ctx, key, ds); err != nil {
		t.Fatalf("get standby daemonset: %v", err)
	}
	if !legacyDaemonSetIsStandby(ds) {
		t.Fatalf("legacy daemonset is not a delayed-lock standby: %#v", ds.Spec.Template.Spec.Containers[0])
	}
	if got := ds.Spec.Template.Annotations[LegacyHandoffStateAnnotation]; got != LegacyHandoffStateStandby {
		t.Fatalf("standby pod state annotation = %q", got)
	}
	if got := ds.Spec.Template.Spec.Containers[0].Image; got != activeImage {
		t.Fatalf("standby image = %q, want validated active image %q", got, activeImage)
	}
	if got := netdConfigMapName(t, ds); got != activeConfigMap {
		t.Fatalf("standby config = %q, want validated active config %q", got, activeConfigMap)
	}
	rolling := ds.Spec.UpdateStrategy.RollingUpdate
	if ds.Spec.UpdateStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType || rolling == nil ||
		rolling.MaxSurge == nil || rolling.MaxSurge.IntValue() != 1 ||
		rolling.MaxUnavailable == nil || rolling.MaxUnavailable.IntValue() != 0 {
		t.Fatalf("standby rollout does not start before stopping active: %#v", ds.Spec.UpdateStrategy)
	}
	ds.Status.ObservedGeneration = ds.Generation
	ds.Status.DesiredNumberScheduled = 1
	ds.Status.CurrentNumberScheduled = 1
	ds.Status.UpdatedNumberScheduled = 0
	ds.Status.NumberReady = 1
	ds.Status.NumberAvailable = 1
	if err := client.Status().Update(ctx, ds); err != nil {
		t.Fatalf("update pending standby rollout status: %v", err)
	}
	if ready, err := reconciler.LegacyStandbyReady(ctx, compiled); err != nil || ready {
		t.Fatalf("LegacyStandbyReady() before rollout = %v, %v; want false, nil", ready, err)
	}

	// The next full reconcile must not reactivate the fallback while embedded
	// netd is still proving readiness.
	if err := reconciler.PrepareLegacyHandoff(ctx, "ghcr.io/sandbox0-ai/sandbox0", "v2", compiled); err != nil {
		t.Fatalf("repeated PrepareLegacyHandoff() error = %v", err)
	}
	if err := client.Get(ctx, key, ds); err != nil {
		t.Fatalf("get standby after repeated handoff: %v", err)
	}
	if !legacyDaemonSetIsStandby(ds) {
		t.Fatal("repeated handoff reactivated the legacy daemonset")
	}

	ds.Status.ObservedGeneration = ds.Generation
	ds.Status.DesiredNumberScheduled = 1
	ds.Status.UpdatedNumberScheduled = 1
	ds.Status.CurrentNumberScheduled = 1
	ds.Status.NumberReady = 1
	ds.Status.NumberAvailable = 1
	ds.Status.NumberUnavailable = 0
	if err := client.Status().Update(ctx, ds); err != nil {
		t.Fatalf("update standby rollout status: %v", err)
	}
	if ready, err := reconciler.LegacyStandbyReady(ctx, compiled); err != nil || !ready {
		t.Fatalf("LegacyStandbyReady() = %v, %v; want true, nil", ready, err)
	}
}

func TestReconcileDoesNotCreateLegacyDaemonSetOnFreshInstall(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClientWithLegacy(t, false, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	ds := &appsv1.DaemonSet{}
	err := client.Get(context.Background(), types.NamespacedName{Name: infra.Name + "-netd", Namespace: infra.Namespace}, ds)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("fresh reconcile created legacy netd: %v", err)
	}
}

func TestScopedActiveLockPathIsolatesInfraInstances(t *testing.T) {
	first := ScopedActiveLockPath("sandbox0-system", "first")
	second := ScopedActiveLockPath("sandbox0-system", "second")
	otherNamespace := ScopedActiveLockPath("tenant-system", "first")
	if first == second || first == otherNamespace || second == otherNamespace {
		t.Fatalf("active lock paths are not scope-specific: %q %q %q", first, second, otherNamespace)
	}
}

func TestCleanupLegacyDaemonSetWaitsForPodsToDisappear(t *testing.T) {
	infra := newNetdTestInfra()
	labels := common.GetServiceLabels(infra.Name, "netd")
	legacyPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-netd-pod", Namespace: infra.Namespace, Labels: labels}}
	client, scheme := newNetdTestClient(t, infra.DeepCopy(), legacyPod)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)

	if err := reconciler.CleanupLegacyDaemonSet(context.Background(), compiled); err == nil {
		t.Fatal("CleanupLegacyDaemonSet() succeeded while a legacy pod remained")
	}
	if err := client.Delete(context.Background(), legacyPod); err != nil {
		t.Fatalf("delete legacy pod: %v", err)
	}
	if err := reconciler.CleanupLegacyDaemonSet(context.Background(), compiled); err != nil {
		t.Fatalf("CleanupLegacyDaemonSet() after pod deletion = %v", err)
	}
}

func containerEnvValue(container corev1.Container, name string) string {
	for i := range container.Env {
		if container.Env[i].Name == name {
			return container.Env[i].Value
		}
	}
	return ""
}

func TestReconcileUsesCompiledPlanForEgressAuthResolverURL(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)
	compiled.Netd.EgressAuthResolverURL = "http://planned-manager.sandbox0-system.svc.cluster.local:19090"

	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", compiled); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset: %v", err)
	}
	configMapName := netdConfigMapName(t, ds)

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      configMapName,
		Namespace: infra.Namespace,
	}, configMap); err != nil {
		t.Fatalf("expected configmap to be created: %v", err)
	}

	cfg := &apiconfig.NetdConfig{}
	if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), cfg); err != nil {
		t.Fatalf("failed to parse netd config: %v", err)
	}
	if got := cfg.EgressAuthResolverURL; got != compiled.Netd.EgressAuthResolverURL {
		t.Fatalf("expected resolver URL %q, got %q", compiled.Netd.EgressAuthResolverURL, got)
	}
}

func TestReconcileInjectsSandboxObservabilityIngestURL(t *testing.T) {
	infra := newNetdTestInfra()
	infra.Spec.Services.ClusterGateway = &infrav1alpha1.ClusterGatewayServiceConfig{
		WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
			EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
		},
	}
	infra.Spec.SandboxObservability = &infrav1alpha1.SandboxObservabilityConfig{
		Type: infrav1alpha1.SandboxObservabilityTypeExternal,
		Audit: &infrav1alpha1.SandboxObservabilityAuditConfig{
			Enabled:      true,
			DeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
		},
		External: &infrav1alpha1.ExternalSandboxObservabilityConfig{
			ClickHouse: infrav1alpha1.ExternalSandboxObservabilityClickHouseConfig{
				DSNSecret: infrav1alpha1.SandboxObservabilityClickHouseDSNSecretRef{
					Name: "clickhouse-dsn",
					Key:  "dsn",
				},
			},
		},
		Ingest: infrav1alpha1.SandboxObservabilityIngestConfig{
			QueueSize: 2048,
		},
	}

	client, scheme := newNetdTestClient(t,
		infra.DeepCopy(),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "clickhouse-dsn",
				Namespace: infra.Namespace,
			},
			Data: map[string][]byte{
				"dsn": []byte("clickhouse://secret@clickhouse:9000/default"),
			},
		},
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)
	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", compiled); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	cfg := getReconciledNetdConfig(t, client, infra)
	want := compiled.Services.ClusterGateway.URL + "/internal/v1/sandbox-observability/events"
	if cfg.SandboxObservabilityIngestURL != want {
		t.Fatalf("sandbox observability ingest url = %q, want %q", cfg.SandboxObservabilityIngestURL, want)
	}
	if cfg.SandboxObservabilityIngestQueueSize != 2048 {
		t.Fatalf("sandbox observability ingest queue size = %d, want 2048", cfg.SandboxObservabilityIngestQueueSize)
	}
	if cfg.SandboxObservabilityAuditDeliveryMode != sandboxobservability.AuditDeliveryModeCanonicalSync {
		t.Fatalf("sandbox observability audit delivery mode = %q, want canonical_sync", cfg.SandboxObservabilityAuditDeliveryMode)
	}
	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: infra.Name + "-netd", Namespace: infra.Namespace}, ds); err != nil {
		t.Fatalf("get netd daemonset: %v", err)
	}
	if !netdHasVolume(ds, "audit-spool") || !netdHasVolume(ds, "audit-jwt-private-key") || !netdHasMount(ds, "audit-spool") || !netdHasMount(ds, "audit-jwt-private-key") {
		t.Fatalf("audit volumes or mounts missing: volumes=%#v mounts=%#v", ds.Spec.Template.Spec.Volumes, ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
}

func TestReconcileOmitsAuditHostAccessWhenAuditDisabled(t *testing.T) {
	ds := reconcileNetdDaemonSet(t, newNetdTestInfra())
	for _, name := range []string{"audit-spool", "audit-jwt-private-key"} {
		if netdHasVolume(ds, name) || netdHasMount(ds, name) {
			t.Fatalf("audit-disabled netd unexpectedly has %q volume or mount", name)
		}
	}
}

func netdHasVolume(ds *appsv1.DaemonSet, name string) bool {
	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func netdHasMount(ds *appsv1.DaemonSet, name string) bool {
	for _, mount := range ds.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func TestReconcileInjectsRedisConfigForTeamBandwidth(t *testing.T) {
	failOpen := true
	infra := newNetdTestInfra()
	infra.Spec.Redis = &infrav1alpha1.RedisConfig{
		Type:             infrav1alpha1.RedisTypeBuiltin,
		KeyPrefix:        "custom",
		OperationTimeout: metav1.Duration{Duration: 250 * time.Millisecond},
		FailOpen:         &failOpen,
	}
	infra.Spec.Services.Netd.Config.TeamEgressBandwidthBytesPerSecond = 1024
	infra.Spec.Services.Netd.Config.TeamIngressBandwidthBytesPerSecond = 2048
	infra.Spec.Services.Netd.Config.TeamBandwidthBurstBytes = 4096

	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), "ghcr.io/sandbox0-ai/sandbox0", "latest", infraplan.Compile(infra)); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	cfg := getReconciledNetdConfig(t, client, infra)
	if cfg.RedisURL != "redis://demo-redis.sandbox0-system.svc:6379/0" {
		t.Fatalf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.RedisKeyPrefix != "custom" {
		t.Fatalf("RedisKeyPrefix = %q", cfg.RedisKeyPrefix)
	}
	if cfg.RedisTimeout.Duration != 250*time.Millisecond {
		t.Fatalf("RedisTimeout = %s", cfg.RedisTimeout.Duration)
	}
	if !cfg.RedisFailOpen {
		t.Fatal("RedisFailOpen = false, want true")
	}
	if cfg.TeamEgressBandwidthBytesPerSecond != 1024 ||
		cfg.TeamIngressBandwidthBytesPerSecond != 2048 ||
		cfg.TeamBandwidthBurstBytes != 4096 {
		t.Fatalf("team bandwidth limits were not preserved: %#v", cfg)
	}
}

func getReconciledNetdConfig(t *testing.T, client ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra) *apiconfig.NetdConfig {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset: %v", err)
	}
	configMapName := netdConfigMapName(t, ds)
	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      configMapName,
		Namespace: infra.Namespace,
	}, configMap); err != nil {
		t.Fatalf("expected configmap to be created: %v", err)
	}
	cfg := &apiconfig.NetdConfig{}
	if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), cfg); err != nil {
		t.Fatalf("failed to parse netd config: %v", err)
	}
	return cfg
}

func newNetdTestClient(t *testing.T, objects ...ctrlclient.Object) (ctrlclient.Client, *runtime.Scheme) {
	return newNetdTestClientWithLegacy(t, true, objects...)
}

func newNetdTestClientWithLegacy(t *testing.T, includeLegacy bool, objects ...ctrlclient.Object) (ctrlclient.Client, *runtime.Scheme) {
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

	objects = append(objects, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-dns",
			Namespace: "kube-system",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.96.0.10",
		},
	})
	if includeLegacy {
		for _, object := range objects {
			infra, ok := object.(*infrav1alpha1.Sandbox0Infra)
			if !ok {
				continue
			}
			labels := common.GetServiceLabels(infra.Name, "netd")
			objects = append(objects, &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: infra.Name + "-netd", Namespace: infra.Namespace},
				Spec: appsv1.DaemonSetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: labels},
					Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}},
				},
			})
			break
		}
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
	return client, scheme
}

func assertNetdMITMSecretMounted(t *testing.T, client ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, secretName string) {
	t.Helper()

	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset: %v", err)
	}
	foundVolume := false
	foundMount := false
	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == "mitm-ca" && volume.Secret != nil && volume.Secret.SecretName == secretName {
			foundVolume = true
		}
	}
	for _, mount := range ds.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "mitm-ca" && mount.MountPath == "/tls" && mount.ReadOnly {
			foundMount = true
		}
	}
	if !foundVolume || !foundMount {
		t.Fatalf("expected mitm-ca volume and mount, got volumes=%#v mounts=%#v", ds.Spec.Template.Spec.Volumes, ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
	configMapName := netdConfigMapName(t, ds)

	cm := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      configMapName,
		Namespace: infra.Namespace,
	}, cm); err != nil {
		t.Fatalf("expected configmap: %v", err)
	}
	if got := cm.Data["config.yaml"]; !containsMITMPaths(got) {
		t.Fatalf("expected mitm ca paths in config, got %q", got)
	}
}

func netdConfigMapName(t *testing.T, ds *appsv1.DaemonSet) string {
	t.Helper()
	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == ConfigVolumeName && volume.ConfigMap != nil {
			return volume.ConfigMap.Name
		}
	}
	t.Fatalf("expected config volume, got %#v", ds.Spec.Template.Spec.Volumes)
	return ""
}

func assertValidManagedMITMCASecret(t *testing.T, secret *corev1.Secret) {
	t.Helper()

	if err := validateMITMCASecret(secret); err != nil {
		t.Fatalf("expected valid managed MITM CA secret: %v", err)
	}
	if _, err := tls.X509KeyPair(secret.Data[mitmCACertKey], secret.Data[mitmCAKeyKey]); err != nil {
		t.Fatalf("expected managed secret keypair to load: %v", err)
	}

	block, _ := pem.Decode(secret.Data[mitmCACertKey])
	if block == nil {
		t.Fatalf("expected managed secret certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse managed secret certificate: %v", err)
	}
	if !cert.IsCA {
		t.Fatalf("expected managed certificate to be a CA")
	}
}

func newExplicitMITMCASecret(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, name string) *corev1.Secret {
	t.Helper()

	certPEM, keyPEM, err := generateManagedMITMCA(infra.Name)
	if err != nil {
		t.Fatalf("generate explicit mitm ca secret: %v", err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			mitmCACertKey: certPEM,
			mitmCAKeyKey:  keyPEM,
		},
	}
}

func containsMITMPaths(config string) bool {
	return strings.Contains(config, "mitm_ca_cert_path: /tls/ca.crt") &&
		strings.Contains(config, "mitm_ca_key_path: /tls/ca.key")
}

func newNetdTestInfra() *infrav1alpha1.Sandbox0Infra {
	runtimeClass := "shared"
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
						Enabled: true,
					},
					RuntimeClassName: &runtimeClass,
					Config:           &infrav1alpha1.NetdConfig{},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}
}
