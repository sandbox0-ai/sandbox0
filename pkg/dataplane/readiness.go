package dataplane

import corev1 "k8s.io/api/core/v1"

const (
	NodeDataPlaneReadyLabel = "sandbox0.ai/data-plane-ready"
	NodeNetdReadyLabel      = "sandbox0.ai/netd-ready"
	NodeCtldReadyLabel      = "sandbox0.ai/ctld-ready"
	ReadyLabelValue         = "true"
	NotReadyLabelValue      = "false"
)

// DataPlaneReadyNodeSelector returns the scheduling selector consumed by
// sandbox pods that must run only on nodes with a ready sandbox0 data plane.
func DataPlaneReadyNodeSelector() map[string]string {
	return map[string]string{NodeDataPlaneReadyLabel: ReadyLabelValue}
}

// SelectorRequiresReadyNode reports whether a node selector is explicitly gated
// on the sandbox0 data-plane readiness signal.
func SelectorRequiresReadyNode(selector map[string]string) bool {
	return selector[NodeDataPlaneReadyLabel] == ReadyLabelValue
}

// NodeReady reports whether the node currently advertises sandbox0 data-plane
// readiness.
func NodeReady(node *corev1.Node) bool {
	return node != nil && node.Labels[NodeDataPlaneReadyLabel] == ReadyLabelValue
}
