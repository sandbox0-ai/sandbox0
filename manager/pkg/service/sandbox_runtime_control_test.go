package service

import (
	"strconv"
	"testing"

	v1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestRuntimeAssignmentObservationRequiresExactPublishedReadyState(t *testing.T) {
	pod, revision := readyRuntimeControlTestPod(t)
	ready, failed, reason := runtimeAssignmentObservation(pod, revision)
	if !ready || failed || reason != "" {
		t.Fatalf("ready observation = (%t, %t, %q)", ready, failed, reason)
	}

	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
		failed bool
	}{
		{
			name: "assignment not published",
			mutate: func(pod *corev1.Pod) {
				delete(pod.Annotations, runtimecontrol.AnnotationAssignmentRevision)
			},
		},
		{
			name: "storage activation pending",
			mutate: func(pod *corev1.Pod) {
				delete(pod.Annotations, runtimecontrol.AnnotationAssignmentReady)
			},
		},
		{
			name: "observed revision stale",
			mutate: func(pod *corev1.Pod) {
				pod.Annotations[runtimecontrol.AnnotationObservedRevision] = "stale"
			},
		},
		{
			name: "observed generation stale",
			mutate: func(pod *corev1.Pod) {
				pod.Annotations[runtimecontrol.AnnotationObservedGeneration] = "1"
			},
		},
		{
			name: "runtime activation failed",
			mutate: func(pod *corev1.Pod) {
				pod.Annotations[runtimecontrol.AnnotationObservedState] = string(runtimecontrol.ObservedFailed)
				setRuntimeTestCondition(pod, v1alpha1.SandboxPodReadinessConditionType, corev1.ConditionFalse, "RuntimeFailed", "state ownership mismatch")
			},
			failed: true,
		},
		{
			name: "runtime failure condition pending",
			mutate: func(pod *corev1.Pod) {
				pod.Annotations[runtimecontrol.AnnotationObservedState] = string(runtimecontrol.ObservedFailed)
				setRuntimeTestCondition(pod, v1alpha1.SandboxPodReadinessConditionType, corev1.ConditionFalse, "RuntimeControlDisconnected", "runtime control stream is disconnected")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := pod.DeepCopy()
			tt.mutate(candidate)
			ready, failed, _ := runtimeAssignmentObservation(candidate, revision)
			if ready {
				t.Fatal("mismatched runtime observation was reported ready")
			}
			if failed != tt.failed {
				t.Fatalf("failed = %t, want %t", failed, tt.failed)
			}
		})
	}
}

func TestRuntimeAssignmentObservationDoesNotWaitForKubeletPhasePropagation(t *testing.T) {
	pod, revision := readyRuntimeControlTestPod(t)
	pod.Status.Phase = corev1.PodPending

	ready, failed, reason := runtimeAssignmentObservation(pod, revision)
	if !ready || failed || reason != "" {
		t.Fatalf("pending-phase ready observation = (%t, %t, %q)", ready, failed, reason)
	}
	if status := (&SandboxService{}).podToSandboxStatus(pod); status != managerapi.SandboxStatusRunning {
		t.Fatalf("sandbox status = %q, want running", status)
	}
}

func TestPodToSandboxHidesAddressWhileRuntimeObservationIsStale(t *testing.T) {
	pod, _ := readyRuntimeControlTestPod(t)
	pod.Status.PodIP = "10.0.0.10"
	pod.Annotations[runtimecontrol.AnnotationObservedRevision] = "stale"

	sandbox := (&SandboxService{config: SandboxServiceConfig{ProcdPort: 49983}}).podToSandbox(pod, "sandbox-1")
	if sandbox.Status != managerapi.SandboxStatusStarting {
		t.Fatalf("status = %q, want starting", sandbox.Status)
	}
	if sandbox.InternalAddr != "" {
		t.Fatalf("internal address = %q, want empty", sandbox.InternalAddr)
	}
}

func TestDeactivateRuntimeAssignmentClearsStaleObservation(t *testing.T) {
	pod, _ := readyRuntimeControlTestPod(t)

	revision, err := deactivateRuntimeAssignment(pod)
	if err != nil {
		t.Fatalf("deactivateRuntimeAssignment() error = %v", err)
	}
	if revision == "" {
		t.Fatal("deactivateRuntimeAssignment() returned an empty revision")
	}
	if pod.Annotations[runtimecontrol.AnnotationAssignmentReady] != "" {
		t.Fatal("runtime assignment remained activation-ready")
	}
	for _, key := range []string{
		runtimecontrol.AnnotationObservedState,
		runtimecontrol.AnnotationObservedRevision,
		runtimecontrol.AnnotationObservedGeneration,
	} {
		if _, ok := pod.Annotations[key]; ok {
			t.Fatalf("stale observation annotation %q remained", key)
		}
	}
}

func installRuntimeObservationReactor(
	t *testing.T,
	client interface {
		PrependReactor(string, string, k8stesting.ReactionFunc)
	},
	indexer cache.Indexer,
	observedState runtimecontrol.ObservedState,
	onActivation func(*corev1.Pod),
) {
	t.Helper()
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := update.GetObject().(*corev1.Pod)
		if !ok || pod == nil {
			return false, nil, nil
		}
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		readyRevision := pod.Annotations[runtimecontrol.AnnotationAssignmentReady]
		if readyRevision != "" {
			assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
			if err != nil {
				return true, nil, err
			}
			if assignment != nil && revision == readyRevision {
				pod.Annotations[runtimecontrol.AnnotationObservedRevision] = revision
				pod.Annotations[runtimecontrol.AnnotationObservedGeneration] = formatRuntimeGeneration(assignment.RuntimeGeneration)
				pod.Annotations[runtimecontrol.AnnotationObservedState] = string(observedState)
				status := corev1.ConditionTrue
				reason := "RuntimeReady"
				message := "runtime assignment is ready"
				if observedState == runtimecontrol.ObservedFailed {
					status = corev1.ConditionFalse
					reason = "RuntimeFailed"
					message = "runtime activation failed"
				}
				setRuntimeTestCondition(pod, corev1.PodReady, status, reason, message)
				setRuntimeTestCondition(pod, v1alpha1.SandboxPodReadinessConditionType, status, reason, message)
				if onActivation != nil {
					onActivation(pod)
				}
			}
		}
		if err := indexer.Update(pod.DeepCopy()); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
}

func setRuntimeTestCondition(pod *corev1.Pod, conditionType corev1.PodConditionType, status corev1.ConditionStatus, reason, message string) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == conditionType {
			pod.Status.Conditions[i].Status = status
			pod.Status.Conditions[i].Reason = reason
			pod.Status.Conditions[i].Message = message
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

func formatRuntimeGeneration(generation int64) string {
	return strconv.FormatInt(generation, 10)
}

func readyRuntimeControlTestPod(t *testing.T) (*corev1.Pod, string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-pod",
			Namespace: "sandbox-system",
			Annotations: map[string]string{
				runtimecontrol.AnnotationSandboxID:         "sandbox-1",
				runtimecontrol.AnnotationRuntimeGeneration: "2",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	assignment, revision, err := runtimecontrol.AssignmentFromPod(pod)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[runtimecontrol.AnnotationAssignmentRevision] = revision
	pod.Annotations[runtimecontrol.AnnotationAssignmentReady] = revision
	pod.Annotations[runtimecontrol.AnnotationObservedRevision] = revision
	pod.Annotations[runtimecontrol.AnnotationObservedGeneration] = formatRuntimeGeneration(assignment.RuntimeGeneration)
	pod.Annotations[runtimecontrol.AnnotationObservedState] = string(runtimecontrol.ObservedReady)
	setRuntimeTestCondition(pod, v1alpha1.SandboxPodReadinessConditionType, corev1.ConditionTrue, "RuntimeReady", "runtime assignment is ready")
	return pod, revision
}
