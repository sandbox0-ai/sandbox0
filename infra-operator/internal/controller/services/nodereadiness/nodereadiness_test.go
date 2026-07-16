package nodereadiness

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
)

func TestReconcileLabelsNodesFromComponentReadiness(t *testing.T) {
	infra := newNodeReadinessInfra()
	nodeA := newNodeReadinessNode("node-a", map[string]string{"sandbox0.ai/node-role": "sandbox"})
	nodeB := newNodeReadinessNode("node-b", map[string]string{"sandbox0.ai/node-role": "sandbox"})
	nodeSystem := newNodeReadinessNode("node-system", map[string]string{"sandbox0.ai/node-role": "system"})
	client, scheme := newNodeReadinessClient(t,
		nodeA,
		nodeB,
		nodeSystem,
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotA),
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotB),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-a-a", "node-a", dataplane.CtldHASlotA, true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-a-b", "node-a", dataplane.CtldHASlotB, true),
		newNodeReadinessCSINode("node-a", true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-b-a", "node-b", dataplane.CtldHASlotA, true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-b-b", "node-b", dataplane.CtldHASlotB, false),
		newNodeReadinessCSINode("node-b", true),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, infraplan.Compile(infra)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	gotNodeA := getNodeReadinessNode(t, client, "node-a")
	assertNodeReadinessLabels(t, gotNodeA, dataplane.ReadyLabelValue, dataplane.ReadyLabelValue)

	gotNodeB := getNodeReadinessNode(t, client, "node-b")
	assertNodeReadinessLabels(t, gotNodeB, dataplane.NotReadyLabelValue, dataplane.NotReadyLabelValue)

	gotSystem := getNodeReadinessNode(t, client, "node-system")
	if _, ok := gotSystem.Labels[dataplane.NodeDataPlaneReadyLabel]; ok {
		t.Fatalf("system node data-plane label = %q, want absent", gotSystem.Labels[dataplane.NodeDataPlaneReadyLabel])
	}
}

func TestReconcileReturnsErrorAndClearsReadinessWhenNoNodeReady(t *testing.T) {
	infra := newNodeReadinessInfra()
	node := newNodeReadinessNode("node-a", map[string]string{"sandbox0.ai/node-role": "sandbox"})
	client, scheme := newNodeReadinessClient(t,
		node,
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotA),
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotB),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	err := reconciler.Reconcile(context.Background(), infra, infraplan.Compile(infra))
	if err == nil {
		t.Fatal("Reconcile() error = nil, want data-plane readiness error")
	}
	if !strings.Contains(err.Error(), "0/1 ready") {
		t.Fatalf("Reconcile() error = %v, want 0/1 ready", err)
	}

	gotNode := getNodeReadinessNode(t, client, "node-a")
	assertNodeReadinessLabels(t, gotNode, dataplane.NotReadyLabelValue, dataplane.NotReadyLabelValue)
}

func TestReconcileDeletesSupersededNetworkReadinessLabel(t *testing.T) {
	infra := newNodeReadinessInfra()
	infra.Spec.Network = nil
	node := newNodeReadinessNode("node-a", map[string]string{
		"sandbox0.ai/node-role":           "sandbox",
		dataplane.NodeDataPlaneReadyLabel: dataplane.NotReadyLabelValue,
		dataplane.NodeNetdReadyLabel:      dataplane.ReadyLabelValue,
		dataplane.NodeCtldReadyLabel:      dataplane.NotReadyLabelValue,
	})
	client, scheme := newNodeReadinessClient(t,
		node,
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotA),
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotB),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-a-a", "node-a", dataplane.CtldHASlotA, true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-a-b", "node-a", dataplane.CtldHASlotB, true),
		newNodeReadinessCSINode("node-a", true),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, infraplan.Compile(infra)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	gotNode := getNodeReadinessNode(t, client, "node-a")
	if got := gotNode.Labels[dataplane.NodeDataPlaneReadyLabel]; got != dataplane.ReadyLabelValue {
		t.Fatalf("data-plane label = %q, want %q", got, dataplane.ReadyLabelValue)
	}
	if _, ok := gotNode.Labels[dataplane.NodeNetdReadyLabel]; ok {
		t.Fatalf("superseded network label = %q, want absent", gotNode.Labels[dataplane.NodeNetdReadyLabel])
	}
	if got := gotNode.Labels[dataplane.NodeCtldReadyLabel]; got != dataplane.ReadyLabelValue {
		t.Fatalf("ctld label = %q, want %q", got, dataplane.ReadyLabelValue)
	}
}

func TestReconcileRequiresKubeletCSIRegistration(t *testing.T) {
	infra := newNodeReadinessInfra()
	node := newNodeReadinessNode("node-a", map[string]string{"sandbox0.ai/node-role": "sandbox"})
	client, scheme := newNodeReadinessClient(t,
		node,
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotA),
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotB),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-a", "node-a", dataplane.CtldHASlotA, true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "ctld-b", "node-a", dataplane.CtldHASlotB, true),
		newNodeReadinessCSINode("node-a", false),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, infraplan.Compile(infra)); err == nil {
		t.Fatal("Reconcile() error = nil before kubelet CSI registration")
	}
	gotNode := getNodeReadinessNode(t, client, "node-a")
	assertNodeReadinessLabels(t, gotNode, dataplane.NotReadyLabelValue, dataplane.NotReadyLabelValue)
}

func TestRefreshRejectsReadySurgePredecessorsWithoutGating(t *testing.T) {
	infra := newNodeReadinessInfra()
	node := newNodeReadinessNode("node-a", map[string]string{
		"sandbox0.ai/node-role":           "sandbox",
		dataplane.NodeDataPlaneReadyLabel: dataplane.ReadyLabelValue,
		dataplane.NodeNetdReadyLabel:      dataplane.ReadyLabelValue,
		dataplane.NodeCtldReadyLabel:      dataplane.ReadyLabelValue,
	})
	oldSlotA := newNodeReadinessCtldPod(infra.Namespace, infra.Name, "old-ctld-a", node.Name, dataplane.CtldHASlotA, true)
	oldSlotA.Spec.Containers[0].Image = "ctld:previous"
	oldSlotB := newNodeReadinessCtldPod(infra.Namespace, infra.Name, "old-ctld-b", node.Name, dataplane.CtldHASlotB, true)
	oldSlotB.Spec.Containers[0].Image = "ctld:previous"
	client, scheme := newNodeReadinessClient(t,
		node,
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotA),
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotB),
		oldSlotA,
		oldSlotB,
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "current-ctld-a", node.Name, dataplane.CtldHASlotA, true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "current-ctld-b", node.Name, dataplane.CtldHASlotB, true),
		newNodeReadinessCSINode(node.Name, true),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	summary, err := reconciler.Refresh(context.Background(), infra, infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if summary.MatchedNodes != 1 || summary.ReadyNodes != 0 {
		t.Fatalf("Refresh() summary = %+v, want 1 matched and 0 ready", summary)
	}
	gotNode := getNodeReadinessNode(t, client, node.Name)
	assertNodeReadinessLabels(t, gotNode, dataplane.NotReadyLabelValue, dataplane.NotReadyLabelValue)
}

func TestRefreshRejectsTerminatingRunningSurgePredecessor(t *testing.T) {
	infra := newNodeReadinessInfra()
	node := newNodeReadinessNode("node-a", map[string]string{
		"sandbox0.ai/node-role":           "sandbox",
		dataplane.NodeDataPlaneReadyLabel: dataplane.ReadyLabelValue,
		dataplane.NodeNetdReadyLabel:      dataplane.ReadyLabelValue,
		dataplane.NodeCtldReadyLabel:      dataplane.ReadyLabelValue,
	})
	predecessor := newNodeReadinessCtldPod(infra.Namespace, infra.Name, "old-ctld-a", node.Name, dataplane.CtldHASlotA, true)
	predecessor.Spec.Containers[0].Image = "ctld:previous"
	deletionTimestamp := metav1.Now()
	predecessor.DeletionTimestamp = &deletionTimestamp
	predecessor.Finalizers = []string{"test.sandbox0.ai/hold-termination"}
	client, scheme := newNodeReadinessClient(t,
		node,
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotA),
		newNodeReadinessCtldDaemonSet(infra.Namespace, infra.Name, dataplane.CtldHASlotB),
		predecessor,
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "current-ctld-a", node.Name, dataplane.CtldHASlotA, true),
		newNodeReadinessCtldPod(infra.Namespace, infra.Name, "current-ctld-b", node.Name, dataplane.CtldHASlotB, true),
		newNodeReadinessCSINode(node.Name, true),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	summary, err := reconciler.Refresh(context.Background(), infra, infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if summary.MatchedNodes != 1 || summary.ReadyNodes != 0 {
		t.Fatalf("Refresh() summary = %+v, want 1 matched and 0 ready", summary)
	}
	gotNode := getNodeReadinessNode(t, client, node.Name)
	assertNodeReadinessLabels(t, gotNode, dataplane.NotReadyLabelValue, dataplane.NotReadyLabelValue)
}

func TestReadyCtldPodsByNodeRequiresDistinctHASlots(t *testing.T) {
	pods := []corev1.Pod{
		*newNodeReadinessCtldPod("sandbox0-system", "demo", "ctld-a-1", "node-a", dataplane.CtldHASlotA, true),
		*newNodeReadinessCtldPod("sandbox0-system", "demo", "ctld-a-2", "node-a", dataplane.CtldHASlotA, true),
		*newNodeReadinessCtldPod("sandbox0-system", "demo", "ctld-b-a", "node-b", dataplane.CtldHASlotA, true),
		*newNodeReadinessCtldPod("sandbox0-system", "demo", "ctld-b-b", "node-b", dataplane.CtldHASlotB, true),
	}
	readyByNode := readyCtldPodsByNode(pods, map[string]*appsv1.DaemonSet{
		dataplane.CtldHASlotA: newNodeReadinessCtldDaemonSet("sandbox0-system", "demo", dataplane.CtldHASlotA),
		dataplane.CtldHASlotB: newNodeReadinessCtldDaemonSet("sandbox0-system", "demo", dataplane.CtldHASlotB),
	}, true)
	if readyByNode["node-a"] {
		t.Fatal("node-a is ready with two pods from the same ctld HA slot")
	}
	if !readyByNode["node-b"] {
		t.Fatal("node-b is not ready with one ready pod from each ctld HA slot")
	}
}

func TestReadyCtldPodsByNodeRequiresEmbeddedNetdTemplate(t *testing.T) {
	sets := map[string]*appsv1.DaemonSet{
		dataplane.CtldHASlotA: newNodeReadinessCtldDaemonSet("sandbox0-system", "demo", dataplane.CtldHASlotA),
		dataplane.CtldHASlotB: newNodeReadinessCtldDaemonSet("sandbox0-system", "demo", dataplane.CtldHASlotB),
	}
	pods := []corev1.Pod{
		*newNodeReadinessCtldPod("sandbox0-system", "demo", "ctld-a", "node-a", dataplane.CtldHASlotA, true),
		*newNodeReadinessCtldPod("sandbox0-system", "demo", "ctld-b", "node-a", dataplane.CtldHASlotB, true),
	}
	for _, ds := range sets {
		ds.Spec.Template.Spec.Containers[0].Env = nil
	}
	for i := range pods {
		pods[i].Spec.Containers[0].Env = nil
	}

	if readyCtldPodsByNode(pods, sets, true)["node-a"] {
		t.Fatal("node-a is netd-ready before the current ctld template embeds netd")
	}
	if !readyCtldPodsByNode(pods, sets, false)["node-a"] {
		t.Fatal("node-a is not ctld-ready when embedded netd is not required")
	}
}

func newNodeReadinessInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Network: &infrav1alpha1.NetworkConfig{Config: &infrav1alpha1.NetdConfig{}},
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{"sandbox0.ai/node-role": "sandbox"},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}
}

func newNodeReadinessClient(t *testing.T, objects ...ctrlclient.Object) (ctrlclient.Client, *runtime.Scheme) {
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
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return client, scheme
}

func newNodeReadinessNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func newNodeReadinessPod(namespace, instance, component, name, nodeName string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				labelManagedBy: "sandbox0infra-operator",
				labelInstance:  instance,
				labelComponent: component,
			},
		},
		Spec: corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: status,
			}},
		},
	}
}

func newNodeReadinessCtldPod(namespace, instance, name, nodeName, slot string, ready bool) *corev1.Pod {
	pod := newNodeReadinessPod(namespace, instance, "ctld", name, nodeName, ready)
	ds := newNodeReadinessCtldDaemonSet(namespace, instance, slot)
	pod.Labels = cloneNodeReadinessStrings(ds.Spec.Template.Labels)
	pod.Annotations = cloneNodeReadinessStrings(ds.Spec.Template.Annotations)
	pod.Spec = *ds.Spec.Template.Spec.DeepCopy()
	pod.Spec.NodeName = nodeName
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "ctld",
		Ready: ready,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	return pod
}

func newNodeReadinessCtldDaemonSet(namespace, instance, slot string) *appsv1.DaemonSet {
	labels := map[string]string{
		labelManagedBy:            "sandbox0infra-operator",
		labelInstance:             instance,
		labelComponent:            "ctld",
		dataplane.CtldHASlotLabel: slot,
	}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance + "-ctld-" + slot,
			Namespace: namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "ctld",
					Image: "ctld:test",
					Env:   []corev1.EnvVar{{Name: embeddedNetdConfigEnv, Value: "/config/netd.yaml"}},
				}}},
			},
		},
	}
}

func cloneNodeReadinessStrings(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func newNodeReadinessCSINode(nodeName string, registered bool) *storagev1.CSINode {
	csiNode := &storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
	if registered {
		csiNode.Spec.Drivers = []storagev1.CSINodeDriver{{
			Name:   volumeportal.DriverName,
			NodeID: nodeName,
		}}
	}
	return csiNode
}

func getNodeReadinessNode(t *testing.T, client ctrlclient.Client, name string) *corev1.Node {
	t.Helper()
	node := &corev1.Node{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: name}, node); err != nil {
		t.Fatalf("get node %s: %v", name, err)
	}
	return node
}

func assertNodeReadinessLabels(t *testing.T, node *corev1.Node, dataPlane, ctld string) {
	t.Helper()
	if got := node.Labels[dataplane.NodeDataPlaneReadyLabel]; got != dataPlane {
		t.Fatalf("node %s data-plane label = %q, want %q", node.Name, got, dataPlane)
	}
	if got, ok := node.Labels[dataplane.NodeNetdReadyLabel]; ok {
		t.Fatalf("node %s superseded network label = %q, want absent", node.Name, got)
	}
	if got := node.Labels[dataplane.NodeCtldReadyLabel]; got != ctld {
		t.Fatalf("node %s ctld label = %q, want %q", node.Name, got, ctld)
	}
}
