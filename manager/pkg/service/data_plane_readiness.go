package service

import (
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	corev1 "k8s.io/api/core/v1"
)

func selectorRequiresReadyDataPlane(selector map[string]string) bool {
	return selector[dataplane.NodeDataPlaneReadyLabel] == dataplane.ReadyLabelValue
}

func nodeDataPlaneReady(node *corev1.Node) bool {
	return node != nil && node.Labels[dataplane.NodeDataPlaneReadyLabel] == dataplane.ReadyLabelValue
}
