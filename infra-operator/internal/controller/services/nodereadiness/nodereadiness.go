package nodereadiness

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
)

const (
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelInstance  = "app.kubernetes.io/instance"
	labelComponent = "app.kubernetes.io/component"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) error {
	if r == nil || r.Resources == nil || r.Resources.Client == nil {
		return fmt.Errorf("node readiness reconciler is not configured")
	}
	if infra == nil {
		return fmt.Errorf("infra is required")
	}
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	if !compiledPlan.Components.EnableManager {
		return nil
	}

	nodeSelector, _ := common.ResolveSandboxNodePlacement(infra)
	requireNetd := compiledPlan.Components.EnableNetd
	requireCtld := compiledPlan.Components.EnableFusePlugin

	podList := &corev1.PodList{}
	if err := r.Resources.Client.List(ctx, podList,
		client.InNamespace(compiledPlan.Scope.Namespace),
		client.MatchingLabels{
			labelManagedBy: "sandbox0infra-operator",
			labelInstance:  compiledPlan.Scope.Name,
		},
	); err != nil {
		return fmt.Errorf("list data-plane daemon pods: %w", err)
	}
	netdReadyByNode := readyPodsByNode(podList.Items, "netd")
	ctldReadyByNode := readyPodsByNode(podList.Items, "ctld")

	nodeList := &corev1.NodeList{}
	if err := r.Resources.Client.List(ctx, nodeList); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	matchedNodes := 0
	readyNodes := 0
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !nodeMatchesSelector(node, nodeSelector) {
			continue
		}
		matchedNodes++

		netdReady := !requireNetd || netdReadyByNode[node.Name]
		ctldReady := !requireCtld || ctldReadyByNode[node.Name]
		ready := netdReady && ctldReady
		if ready {
			readyNodes++
		}

		if err := r.patchNodeReadiness(ctx, node, ready, requireNetd, netdReady, requireCtld, ctldReady); err != nil {
			return err
		}
	}

	if matchedNodes == 0 {
		return fmt.Errorf("no nodes match sandbox placement")
	}
	if readyNodes == 0 {
		return fmt.Errorf("sandbox0 data-plane nodes are not ready: 0/%d ready", matchedNodes)
	}
	return nil
}

func (r *Reconciler) patchNodeReadiness(
	ctx context.Context,
	node *corev1.Node,
	dataPlaneReady bool,
	requireNetd bool,
	netdReady bool,
	requireCtld bool,
	ctldReady bool,
) error {
	if node == nil {
		return nil
	}
	original := node.DeepCopy()
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}

	setBoolLabel(node.Labels, dataplane.NodeDataPlaneReadyLabel, dataPlaneReady)
	setOptionalBoolLabel(node.Labels, dataplane.NodeNetdReadyLabel, requireNetd, netdReady)
	setOptionalBoolLabel(node.Labels, dataplane.NodeCtldReadyLabel, requireCtld, ctldReady)

	if labelsEqual(original.Labels, node.Labels) {
		return nil
	}
	if err := r.Resources.Client.Patch(ctx, node, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("patch node %s data-plane readiness: %w", node.Name, err)
	}
	return nil
}

func readyPodsByNode(pods []corev1.Pod, component string) map[string]bool {
	out := make(map[string]bool)
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == "" || pod.Labels[labelComponent] != component {
			continue
		}
		if podReady(pod) {
			out[pod.Spec.NodeName] = true
		}
	}
	return out
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func nodeMatchesSelector(node *corev1.Node, selector map[string]string) bool {
	if node == nil {
		return false
	}
	for key, value := range selector {
		if node.Labels[key] != value {
			return false
		}
	}
	return true
}

func setBoolLabel(labels map[string]string, key string, ready bool) {
	if ready {
		labels[key] = dataplane.ReadyLabelValue
		return
	}
	labels[key] = dataplane.NotReadyLabelValue
}

func setOptionalBoolLabel(labels map[string]string, key string, required bool, ready bool) {
	if !required {
		delete(labels, key)
		return
	}
	setBoolLabel(labels, key, ready)
}

func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, aValue := range a {
		if b[key] != aValue {
			return false
		}
	}
	return true
}
