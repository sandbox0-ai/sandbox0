package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	rbacsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/rbac"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
)

func TestCleanupLegacyStorageProxyWaitsForManagerAliasAndPods(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	legacyLabels := common.GetServiceLabels(infra.Name, "storage-proxy")
	managerLabels := common.GetServiceLabels(infra.Name, "manager")
	name := infra.Name + "-storage-proxy"
	reconciler, client, _ := newCleanupTestReconciler(t,
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
			Spec:       corev1.ServiceSpec{Selector: legacyLabels},
		},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-storage-proxy", Namespace: infra.Namespace, Labels: legacyLabels}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "manager-0", Namespace: infra.Namespace, Labels: managerLabels}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:        name + "-config-old",
			Namespace:   infra.Namespace,
			Annotations: map[string]string{common.ServiceConfigBaseNameAnnotation: name},
		}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:        infra.Name + "-manager-storage-config-current",
			Namespace:   infra.Namespace,
			Annotations: map[string]string{common.ServiceConfigBaseNameAnnotation: infra.Name + "-manager-storage"},
		}},
	)

	if err := reconciler.cleanupLegacyStorageProxy(ctx, infra); err == nil {
		t.Fatal("cleanup succeeded before the compatibility service selected manager")
	}
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &appsv1.Deployment{})

	alias := &corev1.Service{}
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, alias); err != nil {
		t.Fatalf("get compatibility service: %v", err)
	}
	alias.Spec.Selector = managerLabels
	if err := client.Update(ctx, alias); err != nil {
		t.Fatalf("update compatibility service: %v", err)
	}

	if err := reconciler.cleanupLegacyStorageProxy(ctx, infra); err == nil {
		t.Fatal("cleanup succeeded before compatibility endpoints converged")
	}
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &appsv1.Deployment{})

	ready := true
	if err := client.Create(ctx, &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-manager",
			Namespace: infra.Namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: name},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.10"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: infra.Namespace, Name: "manager-0"},
		}},
	}); err != nil {
		t.Fatalf("create manager endpoint slice: %v", err)
	}

	if err := reconciler.cleanupLegacyStorageProxy(ctx, infra); err == nil {
		t.Fatal("cleanup succeeded while a legacy storage-proxy pod remained")
	}
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ServiceAccount{})
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ConfigMap{})

	legacyPod := &corev1.Pod{}
	if err := client.Get(ctx, types.NamespacedName{Name: "legacy-storage-proxy", Namespace: infra.Namespace}, legacyPod); err != nil {
		t.Fatalf("get legacy storage-proxy pod: %v", err)
	}
	if err := client.Delete(ctx, legacyPod); err != nil {
		t.Fatalf("delete legacy storage-proxy pod: %v", err)
	}
	if err := reconciler.cleanupLegacyStorageProxy(ctx, infra); err != nil {
		t.Fatalf("cleanup after legacy pod removal: %v", err)
	}

	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &appsv1.Deployment{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ServiceAccount{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ConfigMap{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name + "-config-old", Namespace: infra.Namespace}, &corev1.ConfigMap{})
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.Service{})
	assertClientObjectPresent(t, client, types.NamespacedName{Name: infra.Name + "-manager-storage-config-current", Namespace: infra.Namespace}, &corev1.ConfigMap{})
}

func TestCleanupLegacyStorageProxyUsesCanonicalManagerEndpointWithoutAlias(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{Storage: &infrav1alpha1.StorageConfig{
			Runtime: &infrav1alpha1.StorageProxyConfig{},
		}},
	}
	legacyName := infra.Name + "-storage-proxy"
	managerName := infra.Name + "-manager"
	managerLabels := common.GetServiceLabels(infra.Name, "manager")
	ready := true
	reconciler, client, _ := newCleanupTestReconciler(t,
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: managerName, Namespace: infra.Namespace},
			Spec: corev1.ServiceSpec{
				Selector: managerLabels,
				Ports:    []corev1.ServicePort{{Name: "storage-http", Port: 8081}},
			},
		},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "manager-0", Namespace: infra.Namespace, Labels: managerLabels}},
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      managerName + "-endpoint",
				Namespace: infra.Namespace,
				Labels:    map[string]string{discoveryv1.LabelServiceName: managerName},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  []string{"10.0.0.10"},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: infra.Namespace, Name: "manager-0"},
			}},
		},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: legacyName, Namespace: infra.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: legacyName, Namespace: infra.Namespace}},
	)

	if err := reconciler.cleanupLegacyStorageProxy(ctx, infra); err != nil {
		t.Fatalf("canonical cleanup: %v", err)
	}

	assertClientObjectMissing(t, client, types.NamespacedName{Name: legacyName, Namespace: infra.Namespace}, &appsv1.Deployment{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: legacyName, Namespace: infra.Namespace}, &corev1.ServiceAccount{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: legacyName, Namespace: infra.Namespace}, &corev1.Service{})
	assertClientObjectPresent(t, client, types.NamespacedName{Name: managerName, Namespace: infra.Namespace}, &corev1.Service{})
}

func TestCtldHandoffCandidatesRequireCurrentTemplateAndReadySlot(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	ctldLabels := common.GetServiceLabels(infra.Name, "ctld")
	ctldDaemonSets := make(map[string]*appsv1.DaemonSet, 2)
	objects := make([]ctrlclient.Object, 0, 4)
	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		labels := common.CloneStringMap(ctldLabels)
		labels[dataplane.CtldHASlotLabel] = slot
		ds := readyDaemonSet(infra.Name+"-ctld-"+slot, infra.Namespace, labels)
		ds.Spec.Template.Spec.Containers = []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:v2"}}
		configureCtldEmbeddedNetd(ds, infra)
		ctldDaemonSets[slot] = ds
		objects = append(objects, ds, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "old-ctld-" + slot, Namespace: infra.Namespace, Labels: labels, Finalizers: []string{"test.sandbox0.ai/hold"}},
			Spec:       corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:v1"}}},
			Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "ctld", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			},
		})
	}
	reconciler, client, _ := newCleanupTestReconciler(t, objects...)

	ready, err := reconciler.ctldHandoffCandidatesRunning(ctx, infra)
	if err != nil {
		t.Fatalf("check predecessor candidates: %v", err)
	}
	if ready {
		t.Fatal("surge predecessor ctld pods satisfied the current-template handoff gate")
	}

	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		ds := ctldDaemonSets[slot]
		conditions := []corev1.PodCondition(nil)
		if slot == dataplane.CtldHASlotB {
			conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
		if err := client.Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "new-ctld-" + slot,
				Namespace:   infra.Namespace,
				Labels:      ds.Spec.Template.Labels,
				Annotations: ds.Spec.Template.Annotations,
			},
			Spec: func() corev1.PodSpec {
				spec := *ds.Spec.Template.Spec.DeepCopy()
				spec.NodeName = "node-a"
				return spec
			}(),
			Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				Conditions:        conditions,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "ctld", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			},
		}); err != nil {
			t.Fatalf("create updated ctld %s pod: %v", slot, err)
		}
	}

	ready, err = reconciler.ctldHandoffCandidatesRunning(ctx, infra)
	if err != nil {
		t.Fatalf("check current candidates: %v", err)
	}
	if ready {
		t.Fatal("live surge predecessor satisfied the handoff gate after current candidates started")
	}
	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		oldPod := &corev1.Pod{}
		key := types.NamespacedName{Name: "old-ctld-" + slot, Namespace: infra.Namespace}
		if err := client.Get(ctx, key, oldPod); err != nil {
			t.Fatalf("get old ctld %s pod: %v", slot, err)
		}
		if err := client.Delete(ctx, oldPod); err != nil {
			t.Fatalf("delete old ctld %s pod: %v", slot, err)
		}
	}
	ready, err = reconciler.ctldHandoffCandidatesRunning(ctx, infra)
	if err != nil {
		t.Fatalf("check terminating predecessor candidates: %v", err)
	}
	if ready {
		t.Fatal("terminating but still-running surge predecessor satisfied the handoff gate")
	}
	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		oldPod := &corev1.Pod{}
		key := types.NamespacedName{Name: "old-ctld-" + slot, Namespace: infra.Namespace}
		if err := client.Get(ctx, key, oldPod); err != nil {
			t.Fatalf("get terminating old ctld %s pod: %v", slot, err)
		}
		oldPod.Status.ContainerStatuses = nil
		if err := client.Status().Update(ctx, oldPod); err != nil {
			t.Fatalf("mark old ctld %s process stopped: %v", slot, err)
		}
	}
	ready, err = reconciler.ctldHandoffCandidatesRunning(ctx, infra)
	if err != nil {
		t.Fatalf("check candidates after predecessors stopped: %v", err)
	}
	if !ready {
		t.Fatal("current-template ctld pods with one ready HA slot did not satisfy the handoff gate")
	}
}

func TestCtldHandoffCandidatesRejectSlotWithoutEmbeddedNetd(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	baseLabels := common.GetServiceLabels(infra.Name, "ctld")

	labelsA := common.CloneStringMap(baseLabels)
	labelsA[dataplane.CtldHASlotLabel] = dataplane.CtldHASlotA
	slotA := readyDaemonSet(infra.Name+"-ctld-a", infra.Namespace, labelsA)
	slotA.Spec.Template.Spec.Containers = []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:v1"}}
	podA := currentCtldHandoffPod(slotA, "old-ready-ctld-a", "node-a", true)

	labelsB := common.CloneStringMap(baseLabels)
	labelsB[dataplane.CtldHASlotLabel] = dataplane.CtldHASlotB
	slotB := readyDaemonSet(infra.Name+"-ctld-b", infra.Namespace, labelsB)
	slotB.Spec.Template.Spec.Containers = []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:v2"}}
	configureCtldEmbeddedNetd(slotB, infra)
	slotB.Status.NumberReady = 0
	slotB.Status.NumberAvailable = 0
	slotB.Status.NumberUnavailable = 1
	podB := currentCtldHandoffPod(slotB, "new-running-ctld-b", "node-a", false)

	reconciler, _, _ := newCleanupTestReconciler(t, slotA, podA, slotB, podB)
	ready, err := reconciler.ctldHandoffCandidatesRunning(ctx, infra)
	if err != nil {
		t.Fatalf("check mixed-runtime handoff candidates: %v", err)
	}
	if ready {
		t.Fatal("handoff accepted a slot that did not embed guarded netd")
	}
}

func TestCtldHandoffCandidatesAllowReadyAWithWaitingB(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	baseLabels := common.GetServiceLabels(infra.Name, "ctld")

	labelsA := common.CloneStringMap(baseLabels)
	labelsA[dataplane.CtldHASlotLabel] = dataplane.CtldHASlotA
	slotA := readyDaemonSet(infra.Name+"-ctld-a", infra.Namespace, labelsA)
	slotA.Spec.Template.Spec.Containers = []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:v2"}}
	configureCtldEmbeddedNetd(slotA, infra)
	podA := currentCtldHandoffPod(slotA, "new-ready-ctld-a", "node-a", true)

	labelsB := common.CloneStringMap(baseLabels)
	labelsB[dataplane.CtldHASlotLabel] = dataplane.CtldHASlotB
	slotB := readyDaemonSet(infra.Name+"-ctld-b", infra.Namespace, labelsB)
	slotB.Spec.Template.Spec.Containers = []corev1.Container{{Name: "ctld", Image: "sandbox0ai/infra:v2"}}
	configureCtldEmbeddedNetd(slotB, infra)
	slotB.Status.NumberReady = 0
	slotB.Status.NumberAvailable = 0
	slotB.Status.NumberUnavailable = 1
	podB := currentCtldHandoffPod(slotB, "new-ctld-b-waiting-for-netd-lock", "node-a", false)

	reconciler, _, _ := newCleanupTestReconciler(t, slotA, podA, slotB, podB)
	ready, err := reconciler.ctldHandoffCandidatesRunning(ctx, infra)
	if err != nil {
		t.Fatalf("check guarded handoff candidates: %v", err)
	}
	if !ready {
		t.Fatal("ready embedded slot A plus running embedded slot B did not satisfy the handoff gate")
	}
}

func configureCtldEmbeddedNetd(ds *appsv1.DaemonSet, infra *infrav1alpha1.Sandbox0Infra) {
	ds.Spec.Template.Annotations = common.CloneStringMap(ds.Spec.Template.Annotations)
	if ds.Spec.Template.Annotations == nil {
		ds.Spec.Template.Annotations = map[string]string{}
	}
	ds.Spec.Template.Annotations[netd.ConfigHashAnnotation] = "netd-config-v2"
	container := &ds.Spec.Template.Spec.Containers[0]
	container.Env = append(container.Env,
		corev1.EnvVar{Name: "NETD_CONFIG_PATH", Value: netd.ConfigPath},
		corev1.EnvVar{Name: netd.ActiveLockEnv, Value: netd.ScopedActiveLockPath(infra.Namespace, infra.Name)},
	)
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{Name: netd.ConfigVolumeName, MountPath: netd.ConfigPath},
		corev1.VolumeMount{Name: netd.ActiveLockVolumeName, MountPath: netd.ActiveLockMountDirectory},
	)
}

func currentCtldHandoffPod(ds *appsv1.DaemonSet, name, node string, ready bool) *corev1.Pod {
	conditions := []corev1.PodCondition(nil)
	if ready {
		conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
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
			Conditions:        conditions,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "ctld", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		},
	}
	pod.Spec.NodeName = node
	return pod
}

func TestFreshLegacyNetdHandoffDoesNotChurnRBAC(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	reconciler, client, scheme := newCleanupTestReconciler(t)
	resources := common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})
	netdReconciler := netd.NewReconciler(resources)
	rbacReconciler := rbacsvc.NewReconciler(resources)
	compiled := infraplan.Compile(infra)

	for i := 0; i < 2; i++ {
		if err := reconciler.prepareLegacyNetdHandoff(ctx, infra, "sandbox0ai/infra", "v2", compiled, netdReconciler, rbacReconciler); err != nil {
			t.Fatalf("fresh handoff reconcile %d: %v", i+1, err)
		}
	}
	name := infra.Name + "-netd"
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ServiceAccount{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
}

func TestCleanupLegacyNetdWaitsForPodsBeforeRemovingAccess(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	name := infra.Name + "-netd"
	legacyLabels := common.GetServiceLabels(infra.Name, "netd")
	legacyPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-netd", Namespace: infra.Namespace, Labels: legacyLabels}}
	reconciler, client, scheme := newCleanupTestReconciler(t,
		readyDaemonSet(name, infra.Namespace, legacyLabels),
		legacyPod,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}},
	)
	netdReconciler := netd.NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)

	if err := reconciler.cleanupLegacyNetd(ctx, infra, compiled, netdReconciler); err == nil {
		t.Fatal("cleanup succeeded while a legacy netd pod remained")
	}
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &appsv1.DaemonSet{})
	assertClientObjectPresent(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})

	if err := client.Delete(ctx, legacyPod); err != nil {
		t.Fatalf("delete legacy netd pod: %v", err)
	}
	if err := reconciler.cleanupLegacyNetd(ctx, infra, compiled, netdReconciler); err != nil {
		t.Fatalf("cleanup after legacy netd pod removal: %v", err)
	}
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &appsv1.DaemonSet{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ServiceAccount{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
}

func TestCleanupLegacyNetdFreshInstallDoesNotWaitForCtldCandidates(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"}}
	name := infra.Name + "-netd"
	reconciler, client, scheme := newCleanupTestReconciler(t,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}},
	)
	netdReconciler := netd.NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.cleanupLegacyNetd(ctx, infra, infraplan.Compile(infra), netdReconciler); err != nil {
		t.Fatalf("fresh-install cleanup: %v", err)
	}
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &appsv1.DaemonSet{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name, Namespace: infra.Namespace}, &corev1.ServiceAccount{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertClientObjectMissing(t, client, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
}

func readyDaemonSet(name, namespace string, labels map[string]string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Generation: 1},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}},
		},
		Status: appsv1.DaemonSetStatus{
			ObservedGeneration:     1,
			DesiredNumberScheduled: 1,
			CurrentNumberScheduled: 1,
			UpdatedNumberScheduled: 1,
			NumberReady:            1,
			NumberAvailable:        1,
		},
	}
}
