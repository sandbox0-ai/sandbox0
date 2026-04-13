package nodereadiness

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
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
		newNodeReadinessPod(infra.Namespace, infra.Name, "netd", "netd-a", "node-a", true),
		newNodeReadinessPod(infra.Namespace, infra.Name, "ctld", "ctld-a", "node-a", true),
		newNodeReadinessPod(infra.Namespace, infra.Name, "netd", "netd-b", "node-b", true),
		newNodeReadinessPod(infra.Namespace, infra.Name, "ctld", "ctld-b", "node-b", false),
	)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, infraplan.Compile(infra)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	gotNodeA := getNodeReadinessNode(t, client, "node-a")
	assertNodeReadinessLabels(t, gotNodeA, dataplane.ReadyLabelValue, dataplane.ReadyLabelValue, dataplane.ReadyLabelValue)

	gotNodeB := getNodeReadinessNode(t, client, "node-b")
	assertNodeReadinessLabels(t, gotNodeB, dataplane.NotReadyLabelValue, dataplane.ReadyLabelValue, dataplane.NotReadyLabelValue)

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
		newNodeReadinessPod(infra.Namespace, infra.Name, "netd", "netd-a", "node-a", true),
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
	assertNodeReadinessLabels(t, gotNode, dataplane.NotReadyLabelValue, dataplane.ReadyLabelValue, dataplane.NotReadyLabelValue)
}

func TestReconcileDeletesDisabledComponentReadinessLabels(t *testing.T) {
	infra := newNodeReadinessInfra()
	infra.Spec.Services.Netd.Enabled = false
	node := newNodeReadinessNode("node-a", map[string]string{
		"sandbox0.ai/node-role":           "sandbox",
		dataplane.NodeDataPlaneReadyLabel: dataplane.NotReadyLabelValue,
		dataplane.NodeNetdReadyLabel:      dataplane.ReadyLabelValue,
		dataplane.NodeCtldReadyLabel:      dataplane.NotReadyLabelValue,
	})
	client, scheme := newNodeReadinessClient(t,
		node,
		newNodeReadinessPod(infra.Namespace, infra.Name, "ctld", "ctld-a", "node-a", true),
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
		t.Fatalf("netd label = %q, want absent when netd is disabled", gotNode.Labels[dataplane.NodeNetdReadyLabel])
	}
	if got := gotNode.Labels[dataplane.NodeCtldReadyLabel]; got != dataplane.ReadyLabelValue {
		t.Fatalf("ctld label = %q, want %q", got, dataplane.ReadyLabelValue)
	}
}

func newNodeReadinessInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{"sandbox0.ai/node-role": "sandbox"},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
				},
			},
		},
	}
}

func newNodeReadinessClient(t *testing.T, objects ...ctrlclient.Object) (ctrlclient.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
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

func getNodeReadinessNode(t *testing.T, client ctrlclient.Client, name string) *corev1.Node {
	t.Helper()
	node := &corev1.Node{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: name}, node); err != nil {
		t.Fatalf("get node %s: %v", name, err)
	}
	return node
}

func assertNodeReadinessLabels(t *testing.T, node *corev1.Node, dataPlane, netd, ctld string) {
	t.Helper()
	if got := node.Labels[dataplane.NodeDataPlaneReadyLabel]; got != dataPlane {
		t.Fatalf("node %s data-plane label = %q, want %q", node.Name, got, dataPlane)
	}
	if got := node.Labels[dataplane.NodeNetdReadyLabel]; got != netd {
		t.Fatalf("node %s netd label = %q, want %q", node.Name, got, netd)
	}
	if got := node.Labels[dataplane.NodeCtldReadyLabel]; got != ctld {
		t.Fatalf("node %s ctld label = %q, want %q", node.Name, got, ctld)
	}
}
