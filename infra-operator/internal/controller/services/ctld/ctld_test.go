package ctld

import (
	"context"
	"path/filepath"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	netdsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
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

func TestReconcileEmbedsNetdRuntimeAssetsInBothHASlots(t *testing.T) {
	infra := newCtldTestInfra()
	runtimeClass := "runc"
	infra.Spec.Services.Netd.Enabled = true
	infra.Spec.Services.Netd.RuntimeClassName = &runtimeClass

	primary, client := reconcileCtldResources(t, infra, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10"},
	})
	standby := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: infra.Name + "-ctld-b", Namespace: infra.Namespace}, standby); err != nil {
		t.Fatalf("get standby ctld: %v", err)
	}
	wantLockPath := filepath.Join(netdsvc.ActiveLockMountDirectory, infra.Namespace, infra.Name, "netd.lock")
	for _, ds := range []*appsv1.DaemonSet{primary, standby} {
		container := ds.Spec.Template.Spec.Containers[0]
		if ds.Spec.Template.Annotations[ctldRolloutRevisionAnnotation] == "" {
			t.Fatalf("%s is missing the staged rollout revision", ds.Name)
		}
		assertContainerEnv(t, container.Env, "NETD_CONFIG_PATH", netdsvc.ConfigPath)
		assertContainerEnv(t, container.Env, netdsvc.ActiveLockEnv, wantLockPath)
		if ds.Spec.Template.Spec.RuntimeClassName == nil || *ds.Spec.Template.Spec.RuntimeClassName != runtimeClass {
			t.Fatalf("%s runtimeClassName = %#v, want %q", ds.Name, ds.Spec.Template.Spec.RuntimeClassName, runtimeClass)
		}
		assertContainerVolumeMount(t, container.VolumeMounts, netdsvc.ConfigVolumeName, netdsvc.ConfigPath)
		assertContainerVolumeMount(t, container.VolumeMounts, "bpf-fs", "/sys/fs/bpf")
		assertContainerVolumeMount(t, container.VolumeMounts, "modules", "/lib/modules")
		assertContainerVolumeMount(t, container.VolumeMounts, "internal-jwt-private-key", "/secrets/internal_jwt_private.key")
		assertContainerVolumeMount(t, container.VolumeMounts, "mitm-ca", "/tls")
		assertContainerVolumeMount(t, container.VolumeMounts, netdsvc.ActiveLockVolumeName, netdsvc.ActiveLockMountDirectory)
		if got := container.Resources.Requests[corev1.ResourceCPU]; got.Cmp(resource.MustParse("350m")) != 0 {
			t.Fatalf("%s embedded cpu request = %s, want 350m", ds.Name, got.String())
		}
		if got := container.Resources.Requests[corev1.ResourceMemory]; got.Cmp(resource.MustParse("384Mi")) != 0 {
			t.Fatalf("%s embedded memory request = %s, want 384Mi", ds.Name, got.String())
		}
	}
	if !hasContainerPort(primary.Spec.Template.Spec.Containers[0].Ports, "metrics", 9091) {
		t.Fatalf("slot A netd metrics discovery port = %#v", primary.Spec.Template.Spec.Containers[0].Ports)
	}
	if len(standby.Spec.Template.Spec.Containers[0].Ports) != 0 {
		t.Fatalf("slot B duplicates the node-local metrics target: %#v", standby.Spec.Template.Spec.Containers[0].Ports)
	}
	legacy := &appsv1.DaemonSet{}
	err := client.Get(context.Background(), types.NamespacedName{Name: infra.Name + "-netd", Namespace: infra.Namespace}, legacy)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("ctld reconcile created a standalone netd daemonset: %v", err)
	}
}

func TestCtldTerminationGraceCoversStaticShutdownBudget(t *testing.T) {
	const (
		shutdownBudgetSeconds = int64(5 + 7 + 25)
		shutdownMarginSeconds = int64(5)
	)

	assert.LessOrEqual(t, shutdownBudgetSeconds+shutdownMarginSeconds, ctldTerminationGraceSeconds)
}

func TestReconcileStagesHASlotRolloutBThenA(t *testing.T) {
	ctx := context.Background()
	infra := newCtldTestInfra()
	infra.Spec.Services.Netd.Enabled = true
	primary, client := reconcileCtldResources(t, infra, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10"},
	})
	standby := &appsv1.DaemonSet{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: infra.Name + "-ctld-b", Namespace: infra.Namespace}, standby))
	markCtldDaemonSetReady(t, ctx, client, primary)
	markCtldDaemonSetReady(t, ctx, client, standby)
	primaryPod := readyCtldPodForDaemonSet(primary, "ctld-a-old", "node-a")
	standbyPod := readyCtldPodForDaemonSet(standby, "ctld-b-old", "node-a")
	require.NoError(t, client.Create(ctx, primaryPod))
	require.NoError(t, client.Create(ctx, standbyPod))

	reconciler := NewReconciler(common.NewResourceManager(client, newCtldTestScheme(t), nil, common.LocalDevConfig{}))
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	primary = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotA)
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", primary.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:next", standby.Spec.Template.Spec.Containers[0].Image)
	assertCtldRollingUpdate(t, primary, 1, 0)
	assertCtldRollingUpdate(t, standby, 1, 0)

	// An observed DaemonSet status is not enough while its live predecessor is
	// still present; slot A must remain untouched.
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	primary = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotA)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", primary.Spec.Template.Spec.Containers[0].Image)

	require.NoError(t, client.Delete(ctx, standbyPod))
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	markCtldDaemonSetReady(t, ctx, client, standby)
	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(standby, "ctld-b-next", "node-a")))
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	primary = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotA)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:next", primary.Spec.Template.Spec.Containers[0].Image)
}

func TestReconcileRepairsMissingSlotBeforeRollingPeer(t *testing.T) {
	ctx := context.Background()
	infra := newCtldTestInfra()
	primary, client := reconcileCtldResources(t, infra)
	standby := getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	markCtldDaemonSetReady(t, ctx, client, standby)
	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(standby, "ctld-b-old", "node-a")))
	require.NoError(t, client.Delete(ctx, primary))

	reconciler := NewReconciler(common.NewResourceManager(client, newCtldTestScheme(t), nil, common.LocalDevConfig{}))
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	primary = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotA)
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:next", primary.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", standby.Spec.Template.Spec.Containers[0].Image)
	// The fake client does not assign metadata.generation on create. Mirror the
	// API server so an unobserved replacement cannot look vacuously ready.
	primary.Generation = 1
	require.NoError(t, client.Update(ctx, primary))

	// The surviving peer remains unchanged until the repaired slot is actually
	// ready on its desired nodes.
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", standby.Spec.Template.Spec.Containers[0].Image)
	markCtldDaemonSetReady(t, ctx, client, primary)
	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(primary, "ctld-a-next", "node-a")))
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:next", standby.Spec.Template.Spec.Containers[0].Image)
}

func TestReconcileDoesNotMutateEitherDegradedPeer(t *testing.T) {
	ctx := context.Background()
	infra := newCtldTestInfra()
	primary, client := reconcileCtldResources(t, infra)
	standby := getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	markCtldDaemonSetNotReady(t, ctx, client, primary)
	markCtldDaemonSetNotReady(t, ctx, client, standby)

	reconciler := NewReconciler(common.NewResourceManager(client, newCtldTestScheme(t), nil, common.LocalDevConfig{}))
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	primary = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotA)
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", primary.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", standby.Spec.Template.Spec.Containers[0].Image)

	// Once B is a verified peer, A may be recovered first without risking a
	// simultaneous restart.
	markCtldDaemonSetReady(t, ctx, client, standby)
	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(standby, "ctld-b-old", "node-a")))
	require.NoError(t, reconciler.Reconcile(ctx, infra, "ghcr.io/sandbox0-ai/sandbox0", "next", "http://demo-cluster-gateway:8443"))
	primary = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotA)
	standby = getCtldDaemonSet(t, ctx, client, infra, dataplane.CtldHASlotB)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:next", primary.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, "ghcr.io/sandbox0-ai/sandbox0:latest", standby.Spec.Template.Spec.Containers[0].Image)
}

func TestDaemonSetReadyRequiresObservedFullyReadyRollout(t *testing.T) {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Status: appsv1.DaemonSetStatus{
			ObservedGeneration:     2,
			DesiredNumberScheduled: 3,
			UpdatedNumberScheduled: 3,
			NumberReady:            3,
		},
	}
	if !daemonSetReady(ds) {
		t.Fatal("fully rolled daemonset is not ready")
	}
	ds.Status.NumberReady = 2
	ds.Status.NumberUnavailable = 1
	if daemonSetReady(ds) {
		t.Fatal("partially ready daemonset is ready")
	}
}

func TestReadyRejectsLiveSurgePredecessor(t *testing.T) {
	ctx := context.Background()
	infra := newCtldTestInfra()
	primary, client := reconcileCtldResources(t, infra, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10"},
	})
	sets := []*appsv1.DaemonSet{primary, &appsv1.DaemonSet{}}
	if err := client.Get(ctx, types.NamespacedName{Name: infra.Name + "-ctld-b", Namespace: infra.Namespace}, sets[1]); err != nil {
		t.Fatalf("get slot B daemonset: %v", err)
	}
	for _, ds := range sets {
		ds.Status.ObservedGeneration = ds.Generation
		ds.Status.DesiredNumberScheduled = 1
		ds.Status.CurrentNumberScheduled = 1
		ds.Status.UpdatedNumberScheduled = 1
		ds.Status.NumberReady = 1
		ds.Status.NumberAvailable = 1
		if err := client.Status().Update(ctx, ds); err != nil {
			t.Fatalf("update %s status: %v", ds.Name, err)
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        ds.Name + "-current",
				Namespace:   ds.Namespace,
				Labels:      common.CloneStringMap(ds.Spec.Template.Labels),
				Annotations: common.CloneStringMap(ds.Spec.Template.Annotations),
			},
			Spec: *ds.Spec.Template.Spec.DeepCopy(),
			Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
				ContainerStatuses: []corev1.ContainerStatus{{Name: "ctld", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			},
		}
		pod.Spec.NodeName = "node-a"
		if err := client.Create(ctx, pod); err != nil {
			t.Fatalf("create current %s pod: %v", ds.Name, err)
		}
	}
	reconciler := NewReconciler(common.NewResourceManager(client, nil, nil, common.LocalDevConfig{}))
	if ready, err := reconciler.Ready(ctx, infra); err != nil || !ready {
		t.Fatalf("Ready() before predecessor = %v, %v; want true, nil", ready, err)
	}
	predecessor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      primary.Name + "-previous",
			Namespace: primary.Namespace,
			Labels:    common.CloneStringMap(primary.Spec.Template.Labels),
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-a",
			Containers: []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:previous"}},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "ctld", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
	if err := client.Create(ctx, predecessor); err != nil {
		t.Fatalf("create live predecessor: %v", err)
	}
	if ready, err := reconciler.Ready(ctx, infra); err != nil || ready {
		t.Fatalf("Ready() with live predecessor = %v, %v; want false, nil", ready, err)
	}
}

func TestCurrentTemplatePodsReadyRequiresUniqueNonTerminatingNodes(t *testing.T) {
	ctx := context.Background()
	infra := newCtldTestInfra()
	primary, client := reconcileCtldResources(t, infra)
	primary.Status.ObservedGeneration = primary.Generation
	primary.Status.DesiredNumberScheduled = 2
	primary.Status.CurrentNumberScheduled = 2
	primary.Status.UpdatedNumberScheduled = 2
	primary.Status.NumberReady = 2
	primary.Status.NumberAvailable = 2
	require.NoError(t, client.Status().Update(ctx, primary))
	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(primary, "ctld-a-duplicate-1", "node-a")))
	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(primary, "ctld-a-duplicate-2", "node-a")))

	reconciler := NewReconciler(common.NewResourceManager(client, newCtldTestScheme(t), nil, common.LocalDevConfig{}))
	ready, err := reconciler.currentTemplatePodsReady(ctx, primary)
	require.NoError(t, err)
	assert.False(t, ready, "duplicate pods on one node must not satisfy a two-node rollout")

	terminating := readyCtldPodForDaemonSet(primary, "ctld-a-terminating", "node-b")
	terminating.Finalizers = []string{"test.sandbox0.ai/hold"}
	require.NoError(t, client.Create(ctx, terminating))
	require.NoError(t, client.Delete(ctx, terminating))
	terminating = &corev1.Pod{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: "ctld-a-terminating", Namespace: primary.Namespace}, terminating))
	require.False(t, terminating.DeletionTimestamp.IsZero())
	ready, err = reconciler.currentTemplatePodsReady(ctx, primary)
	require.NoError(t, err)
	assert.False(t, ready, "a terminating pod must not satisfy the node readiness gate")

	require.NoError(t, client.Create(ctx, readyCtldPodForDaemonSet(primary, "ctld-a-node-b", "node-b")))
	ready, err = reconciler.currentTemplatePodsReady(ctx, primary)
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestReconcileRemovesLegacySingleDaemonSet(t *testing.T) {
	infra := newCtldTestInfra()
	legacy := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: infra.Name + "-ctld", Namespace: infra.Namespace}}
	_, client := reconcileCtldResources(t, infra, legacy)
	got := &appsv1.DaemonSet{}
	err := client.Get(context.Background(), types.NamespacedName{Name: legacy.Name, Namespace: legacy.Namespace}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("legacy daemonset still exists: %v", err)
	}
}

func reconcileCtldDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()
	ds, _ := reconcileCtldResources(t, infra)
	return ds
}

func reconcileCtldResources(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, extraObjects ...ctrlclient.Object) (*appsv1.DaemonSet, ctrlclient.Client) {
	t.Helper()

	scheme := newCtldTestScheme(t)

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

	ds := getCtldDaemonSet(t, context.Background(), client, infra, dataplane.CtldHASlotA)
	standby := getCtldDaemonSet(t, context.Background(), client, infra, dataplane.CtldHASlotB)

	if got := ds.Spec.Template.Spec.Containers[0].Env[0].Value; got != "ctld" {
		t.Fatalf("expected SERVICE=ctld, got %q", got)
	}
	if ds.Spec.Template.Spec.Containers[0].ReadinessProbe == nil || ds.Spec.Template.Spec.Containers[0].LivenessProbe == nil {
		t.Fatal("expected ctld probes to be configured")
	}
	assertCtldProbe(t, "liveness", ds.Spec.Template.Spec.Containers[0].LivenessProbe, "live", 10)
	assertCtldProbe(t, "readiness", ds.Spec.Template.Spec.Containers[0].ReadinessProbe, "ready", 5)
	if ds.Spec.Template.Spec.ServiceAccountName != "demo-ctld" {
		t.Fatalf("expected service account demo-ctld, got %q", ds.Spec.Template.Spec.ServiceAccountName)
	}
	if ds.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *ds.Spec.Template.Spec.TerminationGracePeriodSeconds != ctldTerminationGraceSeconds {
		t.Fatalf("expected ctld termination grace %ds, got %#v", ctldTerminationGraceSeconds, ds.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-cri-endpoint=/host-run/containerd/containerd.sock")
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-containerd-data-root=/host-var-lib/containerd")
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-kubelet-pods-root=/var/lib/kubelet/pods")
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-kubelet-registration-socket="+ctldKubeletRegistrationSocket)
	assertContainsArg(t, ds.Spec.Template.Spec.Containers[0].Args, "-kubelet-registration-endpoint="+ctldKubeletCSIEndpoint)
	if ds.Spec.Template.Spec.Containers[0].SecurityContext == nil || ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged == nil || !*ds.Spec.Template.Spec.Containers[0].SecurityContext.Privileged {
		t.Fatal("expected ctld container to run privileged")
	}
	wantCPU := resource.MustParse(ctldCPURequest)
	wantMemory := resource.MustParse(ctldMemoryRequest)
	if infraplan.Compile(infra).Netd.Enabled {
		wantCPU.Add(resource.MustParse(embeddedNetdCPURequest))
		wantMemory.Add(resource.MustParse(embeddedNetdMemoryRequest))
	}
	if got := ds.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; got.Cmp(wantCPU) != 0 {
		t.Fatalf("expected ctld cpu request %s, got %s", wantCPU.String(), got.String())
	}
	if got := ds.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantMemory) != 0 {
		t.Fatalf("expected ctld memory request %s, got %s", wantMemory.String(), got.String())
	}
	if len(ds.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected slot A to run only ctld, got %#v", ds.Spec.Template.Spec.Containers)
	}
	if len(standby.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected slot B to run only ctld, got %#v", standby.Spec.Template.Spec.Containers)
	}
	if ds.Spec.Template.Labels[dataplane.CtldHASlotLabel] != dataplane.CtldHASlotA || standby.Spec.Template.Labels[dataplane.CtldHASlotLabel] != dataplane.CtldHASlotB {
		t.Fatalf("unexpected ctld HA slot labels: slot A=%q slot B=%q", ds.Spec.Template.Labels[dataplane.CtldHASlotLabel], standby.Spec.Template.Labels[dataplane.CtldHASlotLabel])
	}
	for _, workload := range []*appsv1.DaemonSet{ds, standby} {
		if !infrav1alpha1.IsNetdEnabled(infra) && len(workload.Spec.Template.Spec.Containers[0].Ports) != 0 {
			t.Fatalf("ctld hostNetwork pod %s reserves node ports: %#v", workload.Name, workload.Spec.Template.Spec.Containers[0].Ports)
		}
	}
	assertCtldRollingUpdate(t, ds, 1, 0)
	assertCtldRollingUpdate(t, standby, 1, 0)
	if len(ds.Spec.Template.Spec.Containers[0].VolumeMounts) < 7 {
		t.Fatalf("expected ctld config, csi, kubelet, data, containerd socket, and containerd data mounts, got %#v", ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}
	assertContainerVolumeMount(t, ds.Spec.Template.Spec.Containers[0].VolumeMounts, "containerd-data", "/host-var-lib/containerd")
	assertNoPodVolume(t, ds.Spec.Template.Spec.Volumes, "plugin-registration")
	driver := &storagev1.CSIDriver{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "volume.sandbox0.ai"}, driver); err != nil {
		t.Fatalf("expected csi driver to be created: %v", err)
	}
	if driver.Spec.PodInfoOnMount == nil || !*driver.Spec.PodInfoOnMount {
		t.Fatal("expected csi driver podInfoOnMount=true")
	}

	return ds, client
}

func newCtldTestScheme(t *testing.T) *runtime.Scheme {
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
	return scheme
}

func getCtldDaemonSet(t *testing.T, ctx context.Context, client ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, slot string) *appsv1.DaemonSet {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: infra.Name + "-ctld-" + slot, Namespace: infra.Namespace}, ds))
	return ds
}

func markCtldDaemonSetReady(t *testing.T, ctx context.Context, client ctrlclient.Client, ds *appsv1.DaemonSet) {
	t.Helper()
	ds.Status.ObservedGeneration = ds.Generation
	ds.Status.DesiredNumberScheduled = 1
	ds.Status.CurrentNumberScheduled = 1
	ds.Status.UpdatedNumberScheduled = 1
	ds.Status.NumberReady = 1
	ds.Status.NumberAvailable = 1
	ds.Status.NumberUnavailable = 0
	require.NoError(t, client.Status().Update(ctx, ds))
}

func markCtldDaemonSetNotReady(t *testing.T, ctx context.Context, client ctrlclient.Client, ds *appsv1.DaemonSet) {
	t.Helper()
	ds.Status.ObservedGeneration = ds.Generation
	ds.Status.DesiredNumberScheduled = 1
	ds.Status.CurrentNumberScheduled = 1
	ds.Status.UpdatedNumberScheduled = 0
	ds.Status.NumberReady = 0
	ds.Status.NumberAvailable = 0
	ds.Status.NumberUnavailable = 1
	require.NoError(t, client.Status().Update(ctx, ds))
}

func readyCtldPodForDaemonSet(ds *appsv1.DaemonSet, name, node string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   ds.Namespace,
			Labels:      common.CloneStringMap(ds.Spec.Template.Labels),
			Annotations: common.CloneStringMap(ds.Spec.Template.Annotations),
		},
		Spec: *ds.Spec.Template.Spec.DeepCopy(),
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "ctld", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
	pod.Spec.NodeName = node
	return pod
}

func assertCtldRollingUpdate(t *testing.T, daemonSet *appsv1.DaemonSet, maxUnavailable, maxSurge int) {
	t.Helper()

	rollingUpdate := daemonSet.Spec.UpdateStrategy.RollingUpdate
	if rollingUpdate == nil || rollingUpdate.MaxUnavailable == nil || rollingUpdate.MaxSurge == nil {
		t.Fatalf("expected ctld rolling update strategy, got %#v", daemonSet.Spec.UpdateStrategy)
	}
	if got := rollingUpdate.MaxUnavailable.IntValue(); got != maxUnavailable {
		t.Fatalf("expected %s maxUnavailable=%d, got %d", daemonSet.Name, maxUnavailable, got)
	}
	if got := rollingUpdate.MaxSurge.IntValue(); got != maxSurge {
		t.Fatalf("expected %s maxSurge=%d, got %d", daemonSet.Name, maxSurge, got)
	}
}

func assertCtldProbe(t *testing.T, name string, probe *corev1.Probe, kind string, periodSeconds int32) {
	t.Helper()

	if probe.Exec == nil {
		t.Fatalf("expected ctld %s probe to use exec", name)
	}
	wantArg := "-ha-probe=" + kind
	found := false
	for _, arg := range probe.Exec.Command {
		if arg == wantArg {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected ctld %s probe command to contain %q, got %#v", name, wantArg, probe.Exec.Command)
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

func assertContainerEnv(t *testing.T, env []corev1.EnvVar, name, value string) {
	t.Helper()
	for i := range env {
		if env[i].Name != name {
			continue
		}
		if env[i].Value != value {
			t.Fatalf("environment %q = %q, want %q", name, env[i].Value, value)
		}
		return
	}
	t.Fatalf("expected environment %q, got %#v", name, env)
}

func hasContainerPort(ports []corev1.ContainerPort, name string, port int32) bool {
	for i := range ports {
		if ports[i].Name == name && ports[i].ContainerPort == port {
			return true
		}
	}
	return false
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

func assertNoPodVolume(t *testing.T, volumes []corev1.Volume, name string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			t.Fatalf("expected volume %q to be absent, got %#v", name, volumes)
		}
	}
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
