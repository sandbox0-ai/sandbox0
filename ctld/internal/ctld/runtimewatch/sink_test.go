package runtimewatch

import (
	"context"
	"testing"

	v1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodStatusSinkDesiredStateOnlyUpdatesReadiness(t *testing.T) {
	pod := testPod()
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	pod.Annotations = map[string]string{
		runtimecontrol.AnnotationObservedState:      string(runtimecontrol.ObservedReady),
		runtimecontrol.AnnotationObservedRevision:   "previous",
		runtimecontrol.AnnotationObservedGeneration: "1",
	}
	client := fake.NewSimpleClientset(pod)
	sink := NewPodStatusSink(client)

	if err := sink.Desired(context.Background(), pod, runtimecontrol.Snapshot{
		State: runtimecontrol.DesiredWaitingRootFS,
	}); err != nil {
		t.Fatalf("Desired() error = %v", err)
	}

	current, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Annotations[runtimecontrol.AnnotationObservedRevision] != "previous" {
		t.Fatalf("desired state rewrote observation annotations: %#v", current.Annotations)
	}
	if got := conditionStatus(current, v1alpha1.SandboxPodReadinessConditionType); got != corev1.ConditionFalse {
		t.Fatalf("readiness status = %q, want False", got)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" && action.GetSubresource() == "" {
			t.Fatalf("desired state performed a metadata update: %#v", action)
		}
	}
}

func TestPodStatusSinkDoesNotPersistTransientObservations(t *testing.T) {
	for _, state := range []runtimecontrol.ObservedState{
		runtimecontrol.ObservedWaitingRootFS,
		runtimecontrol.ObservedLoading,
		runtimecontrol.ObservedRecovering,
	} {
		t.Run(string(state), func(t *testing.T) {
			pod := testPod()
			client := fake.NewSimpleClientset(pod)
			sink := NewPodStatusSink(client)

			if err := sink.Observed(context.Background(), pod, runtimecontrol.Observation{State: state}); err != nil {
				t.Fatalf("Observed() error = %v", err)
			}
			if len(client.Actions()) != 0 {
				t.Fatalf("transient observation performed Kubernetes API actions: %#v", client.Actions())
			}
		})
	}
}

func TestPodStatusSinkMarksOnlyMatchingObservationReady(t *testing.T) {
	pod := testPod()
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	pod.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationRuntimeGeneration: "4",
	}
	assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
	pod.Annotations[runtimecontrol.AnnotationAssignmentReady] = revision

	client := fake.NewSimpleClientset(pod)
	sink := NewPodStatusSink(client)
	observation := runtimecontrol.Observation{
		State:             runtimecontrol.ObservedReady,
		Revision:          revision,
		RuntimeGeneration: assignment.RuntimeGeneration,
	}
	if err := sink.Observed(context.Background(), pod, observation); err != nil {
		t.Fatalf("Observed() error = %v", err)
	}

	current, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Annotations[runtimecontrol.AnnotationObservedState] != string(runtimecontrol.ObservedReady) ||
		current.Annotations[runtimecontrol.AnnotationObservedRevision] != revision ||
		current.Annotations[runtimecontrol.AnnotationObservedGeneration] != "4" {
		t.Fatalf("observed annotations = %#v", current.Annotations)
	}
	if !conditionTrue(current, v1alpha1.SandboxPodReadinessConditionType) {
		t.Fatalf("readiness conditions = %#v", current.Status.Conditions)
	}
}

func TestPodStatusSinkDisconnectRevokesReadiness(t *testing.T) {
	pod := testPod()
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	pod.Annotations = map[string]string{
		runtimecontrol.AnnotationObservedState:      string(runtimecontrol.ObservedReady),
		runtimecontrol.AnnotationObservedRevision:   "old",
		runtimecontrol.AnnotationObservedGeneration: "1",
	}
	client := fake.NewSimpleClientset(pod)
	sink := NewPodStatusSink(client)

	if err := sink.Disconnected(context.Background(), pod); err != nil {
		t.Fatalf("Disconnected() error = %v", err)
	}
	current, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Annotations[runtimecontrol.AnnotationObservedState] != string(runtimecontrol.ObservedDisconnected) {
		t.Fatalf("observed state = %q", current.Annotations[runtimecontrol.AnnotationObservedState])
	}
	if current.Annotations[runtimecontrol.AnnotationObservedRevision] != "" ||
		current.Annotations[runtimecontrol.AnnotationObservedGeneration] != "" {
		t.Fatalf("stale observation annotations = %#v", current.Annotations)
	}
	if conditionTrue(current, v1alpha1.SandboxPodReadinessConditionType) {
		t.Fatalf("readiness conditions = %#v", current.Status.Conditions)
	}
	if got := conditionStatus(current, v1alpha1.SandboxPodLivenessConditionType); got != corev1.ConditionUnknown {
		t.Fatalf("liveness status = %q, want Unknown", got)
	}
}

func TestPodStatusSinkActivationFailureDoesNotReportProcdDead(t *testing.T) {
	pod := testPod()
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{
		ConditionType: v1alpha1.SandboxPodReadinessConditionType,
	}}
	pod.Annotations = map[string]string{
		runtimecontrol.AnnotationSandboxID:         "sandbox-1",
		runtimecontrol.AnnotationRuntimeGeneration: "2",
	}
	assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
	pod.Annotations[runtimecontrol.AnnotationAssignmentReady] = revision
	client := fake.NewSimpleClientset(pod)
	sink := NewPodStatusSink(client)

	if err := sink.Observed(context.Background(), pod, runtimecontrol.Observation{
		State:             runtimecontrol.ObservedFailed,
		Revision:          revision,
		RuntimeGeneration: assignment.RuntimeGeneration,
		Reason:            "runtime state does not match",
	}); err != nil {
		t.Fatalf("Observed() error = %v", err)
	}
	current, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := conditionStatus(current, v1alpha1.SandboxPodReadinessConditionType); got != corev1.ConditionFalse {
		t.Fatalf("readiness status = %q, want False", got)
	}
	if got := conditionStatus(current, v1alpha1.SandboxPodLivenessConditionType); got != corev1.ConditionTrue {
		t.Fatalf("liveness status = %q, want True", got)
	}
}

func conditionTrue(pod *corev1.Pod, conditionType corev1.PodConditionType) bool {
	return conditionStatus(pod, conditionType) == corev1.ConditionTrue
}

func conditionStatus(pod *corev1.Pod, conditionType corev1.PodConditionType) corev1.ConditionStatus {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status
		}
	}
	return ""
}
