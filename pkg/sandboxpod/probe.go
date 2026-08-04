package sandboxpod

import corev1 "k8s.io/api/core/v1"

// HasReadinessGate reports whether the Pod uses the Sandbox0 readiness gate.
func HasReadinessGate(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, gate := range pod.Spec.ReadinessGates {
		if gate.ConditionType == SandboxPodReadinessConditionType {
			return true
		}
	}
	return false
}
