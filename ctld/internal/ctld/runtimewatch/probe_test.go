package runtimewatch

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestEnsureProbeConditionsPreservesUnrelatedStatus(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-a", Namespace: "default"},
		Spec:       corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{{ConditionType: sandboxpod.SandboxPodReadinessConditionType}}},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	sink := NewPodStatusSink(fake.NewSimpleClientset(pod))
	startup := sandboxprobe.Passed(sandboxprobe.KindStartup, "Started", "runtime started", nil)
	readiness := sandboxprobe.Passed(sandboxprobe.KindReadiness, "Ready", "runtime ready", nil)
	liveness := sandboxprobe.Passed(sandboxprobe.KindLiveness, "Live", "runtime live", nil)

	updated, err := sink.ensureProbeConditions(context.Background(), pod, &startup, &readiness, &liveness)
	if err != nil {
		t.Fatalf("ensureProbeConditions() error = %v", err)
	}
	if findProbeCondition(updated.Status.Conditions, corev1.PodReady) == nil {
		t.Fatal("Kubernetes PodReady condition was removed")
	}
	for _, conditionType := range []corev1.PodConditionType{
		sandboxpod.SandboxPodStartupConditionType,
		sandboxpod.SandboxPodReadinessConditionType,
		sandboxpod.SandboxPodLivenessConditionType,
	} {
		condition := findProbeCondition(updated.Status.Conditions, conditionType)
		if condition == nil || condition.Status != corev1.ConditionTrue {
			t.Fatalf("condition %q = %#v", conditionType, condition)
		}
	}
}
