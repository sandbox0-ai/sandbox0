package podmeta

import (
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
)

// IsReady reports whether Kubernetes and manager-specific sandbox readiness are true.
func IsReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if !conditionTrue(pod.Status.Conditions, corev1.PodReady) {
		return false
	}
	if sandboxpod.HasReadinessGate(pod) {
		if !conditionTrue(pod.Status.Conditions, sandboxpod.SandboxPodReadinessConditionType) {
			return false
		}
		live := findCondition(pod.Status.Conditions, sandboxpod.SandboxPodLivenessConditionType)
		return live == nil || live.Status != corev1.ConditionFalse
	}
	return true
}

func conditionTrue(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) bool {
	condition := findCondition(conditions, conditionType)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

func findCondition(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
