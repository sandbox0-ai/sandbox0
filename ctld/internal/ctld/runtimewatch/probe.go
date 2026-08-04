package runtimewatch

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

// ensureProbeConditions updates Sandbox0 probe conditions without changing
// unrelated Pod status.
func (s *PodStatusSink) ensureProbeConditions(ctx context.Context, pod *corev1.Pod, startup, readiness, liveness *sandboxprobe.Response) (*corev1.Pod, error) {
	if s == nil || s.client == nil || pod == nil || !sandboxpod.HasReadinessGate(pod) {
		return pod, nil
	}

	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := s.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !sandboxpod.HasReadinessGate(current) {
			updated = current
			return nil
		}

		current = current.DeepCopy()
		changed := false
		apply := func(condition corev1.PodCondition) {
			existing := findProbeCondition(current.Status.Conditions, condition.Type)
			if existing != nil && existing.Status == condition.Status && existing.Reason == condition.Reason && existing.Message == condition.Message {
				return
			}
			setProbeCondition(&current.Status.Conditions, condition)
			changed = true
		}

		if startup != nil {
			apply(probeConditionFromResponse(sandboxpod.SandboxPodStartupConditionType, *startup, corev1.ConditionFalse))
		}
		if liveness != nil {
			apply(probeConditionFromResponse(sandboxpod.SandboxPodLivenessConditionType, *liveness, corev1.ConditionUnknown))
		}
		if readiness != nil {
			readyCondition := probeConditionFromResponse(sandboxpod.SandboxPodReadinessConditionType, *readiness, corev1.ConditionFalse)
			if startup != nil && startup.Status != sandboxprobe.StatusPassed {
				readyCondition.Status = corev1.ConditionFalse
				readyCondition.Reason = "SandboxStartupProbeFailed"
				readyCondition.Message = startup.Message
			}
			if liveness != nil && liveness.Status == sandboxprobe.StatusFailed {
				readyCondition.Status = corev1.ConditionFalse
				readyCondition.Reason = "SandboxLivenessProbeFailed"
				readyCondition.Message = liveness.Message
			}
			apply(readyCondition)
		}

		if !changed {
			updated = current
			return nil
		}
		updated, err = s.client.CoreV1().Pods(current.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func probeConditionFromResponse(conditionType corev1.PodConditionType, result sandboxprobe.Response, suspendedStatus corev1.ConditionStatus) corev1.PodCondition {
	status := corev1.ConditionFalse
	switch result.Status {
	case sandboxprobe.StatusPassed:
		status = corev1.ConditionTrue
	case sandboxprobe.StatusSuspended:
		status = suspendedStatus
	}
	reason := result.Reason
	if reason == "" {
		reason = "SandboxProbe" + string(result.Status)
	}
	return corev1.PodCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            result.Message,
	}
}

func findProbeCondition(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func setProbeCondition(conditions *[]corev1.PodCondition, condition corev1.PodCondition) {
	if conditions == nil {
		return
	}
	for i := range *conditions {
		if (*conditions)[i].Type != condition.Type {
			continue
		}
		if (*conditions)[i].Status == condition.Status {
			condition.LastTransitionTime = (*conditions)[i].LastTransitionTime
		}
		(*conditions)[i] = condition
		return
	}
	*conditions = append(*conditions, condition)
}
