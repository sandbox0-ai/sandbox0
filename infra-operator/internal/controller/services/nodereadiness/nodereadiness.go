package nodereadiness

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	ctldsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/ctld"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
)

const (
	labelManagedBy        = "app.kubernetes.io/managed-by"
	labelInstance         = "app.kubernetes.io/instance"
	labelComponent        = "app.kubernetes.io/component"
	embeddedNetdConfigEnv = "NETD_CONFIG_PATH"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Summary describes the latest node-scoped data-plane readiness observation.
type Summary struct {
	MatchedNodes int
	ReadyNodes   int
}

// Reconcile preserves the original gating behavior for existing callers.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) error {
	return r.Check(ctx, infra, compiledPlan)
}

// Check refreshes node labels and gates workflow progress on at least one
// ready data-plane node.
func (r *Reconciler) Check(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) error {
	summary, err := r.Refresh(ctx, infra, compiledPlan)
	if err != nil {
		return err
	}
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}
	if !compiledPlan.Components.EnableManager {
		return nil
	}
	if summary.MatchedNodes == 0 {
		return fmt.Errorf("no nodes match sandbox placement")
	}
	if summary.ReadyNodes == 0 {
		return fmt.Errorf("sandbox0 data-plane nodes are not ready: 0/%d ready", summary.MatchedNodes)
	}
	return nil
}

// Refresh patches node readiness labels from current runtime state without
// gating workflow progress. Controllers call it after every workflow attempt
// so an earlier component failure cannot leave stale ready labels behind.
func (r *Reconciler) Refresh(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) (Summary, error) {
	var summary Summary
	if r == nil || r.Resources == nil || r.Resources.Client == nil {
		return summary, fmt.Errorf("node readiness reconciler is not configured")
	}
	if infra == nil {
		return summary, fmt.Errorf("infra is required")
	}
	if compiledPlan == nil {
		compiledPlan = infraplan.Compile(infra)
	}

	nodeSelector, _ := common.ResolveSandboxNodePlacement(infra)
	managerEnabled := compiledPlan.Components.EnableManager
	requireNetwork := compiledPlan.Components.EnableNetwork
	requireCtld := compiledPlan.Components.EnableCtld

	ctldReadyByNode := make(map[string]bool)
	csiRegisteredByNode := make(map[string]bool)
	if managerEnabled && (requireCtld || requireNetwork) {
		podList := &corev1.PodList{}
		if err := r.Resources.Client.List(ctx, podList,
			client.InNamespace(compiledPlan.Scope.Namespace),
			client.MatchingLabels{
				labelManagedBy: "sandbox0infra-operator",
				labelInstance:  compiledPlan.Scope.Name,
			},
		); err != nil {
			return summary, fmt.Errorf("list data-plane daemon pods: %w", err)
		}
		ctldDaemonSets, err := r.currentCtldDaemonSets(ctx, compiledPlan)
		if err != nil {
			return summary, err
		}
		ctldReadyByNode = readyCtldPodsByNode(podList.Items, ctldDaemonSets, requireNetwork)
	}
	if managerEnabled && requireCtld {
		csiNodeList := &storagev1.CSINodeList{}
		if err := r.Resources.Client.List(ctx, csiNodeList); err != nil {
			return summary, fmt.Errorf("list CSI nodes: %w", err)
		}
		csiRegisteredByNode = registeredCSIDriverByNode(csiNodeList.Items, volumeportal.DriverName)
	}

	nodeList := &corev1.NodeList{}
	if err := r.Resources.Client.List(ctx, nodeList); err != nil {
		return summary, fmt.Errorf("list nodes: %w", err)
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !nodeMatchesSelector(node, nodeSelector) {
			continue
		}
		summary.MatchedNodes++

		if !managerEnabled {
			if err := r.clearNodeReadiness(ctx, node); err != nil {
				return summary, err
			}
			continue
		}

		ctldReady := !requireCtld || (ctldReadyByNode[node.Name] && csiRegisteredByNode[node.Name])
		// Active ctld readiness includes the network runtime, while standby
		// readiness proves that the HA peer can take over.
		networkReady := !requireNetwork || ctldReady
		ready := networkReady && ctldReady
		if ready {
			summary.ReadyNodes++
		}

		if err := r.patchNodeReadiness(ctx, node, ready, requireCtld, ctldReady); err != nil {
			return summary, err
		}
	}
	return summary, nil
}

func (r *Reconciler) currentCtldDaemonSets(ctx context.Context, compiledPlan *infraplan.InfraPlan) (map[string]*appsv1.DaemonSet, error) {
	sets := make(map[string]*appsv1.DaemonSet, 2)
	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		ds := &appsv1.DaemonSet{}
		key := types.NamespacedName{
			Name:      fmt.Sprintf("%s-ctld-%s", compiledPlan.Scope.Name, slot),
			Namespace: compiledPlan.Scope.Namespace,
		}
		if err := r.Resources.Client.Get(ctx, key, ds); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get ctld %s daemonset: %w", slot, err)
		}
		sets[slot] = ds
	}
	return sets, nil
}

func (r *Reconciler) clearNodeReadiness(ctx context.Context, node *corev1.Node) error {
	if node == nil {
		return nil
	}
	original := node.DeepCopy()
	delete(node.Labels, dataplane.NodeDataPlaneReadyLabel)
	// Remove the superseded per-engine label while converging nodes on the
	// data-plane and ctld readiness signals.
	delete(node.Labels, dataplane.NodeNetdReadyLabel)
	delete(node.Labels, dataplane.NodeCtldReadyLabel)
	if labelsEqual(original.Labels, node.Labels) {
		return nil
	}
	if err := r.Resources.Client.Patch(ctx, node, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("clear node %s data-plane readiness: %w", node.Name, err)
	}
	return nil
}

func (r *Reconciler) patchNodeReadiness(
	ctx context.Context,
	node *corev1.Node,
	dataPlaneReady bool,
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
	delete(node.Labels, dataplane.NodeNetdReadyLabel)
	setOptionalBoolLabel(node.Labels, dataplane.NodeCtldReadyLabel, requireCtld, ctldReady)

	if labelsEqual(original.Labels, node.Labels) {
		return nil
	}
	if err := r.Resources.Client.Patch(ctx, node, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("patch node %s data-plane readiness: %w", node.Name, err)
	}
	return nil
}

// readyCtldPodsByNode requires one ready pod from each HA slot. A synchronized
// standby can become ready before the active ctld has completed kubelet CSI
// registration, so a single ready ctld pod is not a sufficient node signal.
func readyCtldPodsByNode(pods []corev1.Pod, daemonSets map[string]*appsv1.DaemonSet, requireEmbeddedNetd bool) map[string]bool {
	readySlotsByNode := make(map[string]map[string]struct{})
	blockedByPredecessor := make(map[string]bool)
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == "" || pod.Labels[labelComponent] != "ctld" {
			continue
		}
		slot := pod.Labels[dataplane.CtldHASlotLabel]
		if slot != dataplane.CtldHASlotA && slot != dataplane.CtldHASlotB {
			continue
		}
		ds := daemonSets[slot]
		if requireEmbeddedNetd && !daemonSetEmbedsNetd(ds) {
			continue
		}
		if ctldsvc.CtldContainerRunning(pod) && !ctldsvc.PodMatchesCurrentTemplate(pod, ds) {
			blockedByPredecessor[pod.Spec.NodeName] = true
			continue
		}
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if !ctldsvc.PodReadyForCurrentTemplate(pod, ds) {
			continue
		}
		if readySlotsByNode[pod.Spec.NodeName] == nil {
			readySlotsByNode[pod.Spec.NodeName] = make(map[string]struct{}, 2)
		}
		readySlotsByNode[pod.Spec.NodeName][slot] = struct{}{}
	}
	readyByNode := make(map[string]bool, len(readySlotsByNode))
	for nodeName, slots := range readySlotsByNode {
		_, slotAReady := slots[dataplane.CtldHASlotA]
		_, slotBReady := slots[dataplane.CtldHASlotB]
		readyByNode[nodeName] = slotAReady && slotBReady && !blockedByPredecessor[nodeName]
	}
	return readyByNode
}

func daemonSetEmbedsNetd(ds *appsv1.DaemonSet) bool {
	if ds == nil {
		return false
	}
	for i := range ds.Spec.Template.Spec.Containers {
		container := &ds.Spec.Template.Spec.Containers[i]
		if container.Name != "ctld" {
			continue
		}
		for _, env := range container.Env {
			if env.Name == embeddedNetdConfigEnv && env.Value != "" {
				return true
			}
		}
		return false
	}
	return false
}

func registeredCSIDriverByNode(csiNodes []storagev1.CSINode, driverName string) map[string]bool {
	registeredByNode := make(map[string]bool)
	for i := range csiNodes {
		csiNode := &csiNodes[i]
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name == driverName {
				registeredByNode[csiNode.Name] = true
				break
			}
		}
	}
	return registeredByNode
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
