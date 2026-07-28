package service

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

const (
	defaultSandboxRuntimeUnhealthyAfter      = 90 * time.Second
	defaultSandboxUnhealthyCheckpointTimeout = 30 * time.Second
)

func sandboxRuntimeLivenessCondition(pod *corev1.Pod) *corev1.PodCondition {
	if pod == nil {
		return nil
	}
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == v1alpha1.SandboxPodLivenessConditionType {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

func sandboxRuntimeLivenessFailureSustained(condition *corev1.PodCondition, now time.Time, after time.Duration) bool {
	if condition == nil || condition.Status != corev1.ConditionFalse {
		return false
	}
	if after <= 0 {
		return true
	}
	transitionedAt := condition.LastTransitionTime.Time
	return !transitionedAt.IsZero() && !transitionedAt.Add(after).After(now)
}
