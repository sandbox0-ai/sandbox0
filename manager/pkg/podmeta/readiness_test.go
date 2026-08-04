package podmeta

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
)

func TestIsReadyHonorsSandboxConditions(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{{ConditionType: sandboxpod.SandboxPodReadinessConditionType}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: sandboxpod.SandboxPodReadinessConditionType, Status: corev1.ConditionTrue},
			},
		},
	}
	if !IsReady(pod) {
		t.Fatal("ready sandbox pod reported as unready")
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: sandboxpod.SandboxPodLivenessConditionType, Status: corev1.ConditionFalse})
	if IsReady(pod) {
		t.Fatal("failed liveness condition did not make sandbox unready")
	}
}
