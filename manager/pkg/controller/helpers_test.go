package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestIsPodReady(t *testing.T) {
	t.Run("returns false for nil pods", func(t *testing.T) {
		if IsPodReady(nil) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns true only for running pods with ready condition", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if !IsPodReady(pod) {
			t.Fatal("IsPodReady() = false, want true")
		}
	})

	t.Run("returns false for running but not-ready pods", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionFalse,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns false for pending pods", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns false when ready condition is missing", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.ContainersReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns false for unknown ready condition", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionUnknown,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})
}
