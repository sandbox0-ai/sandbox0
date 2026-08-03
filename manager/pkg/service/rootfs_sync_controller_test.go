package service

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRootFSSyncControllerReconcilesExistingActivePod(t *testing.T) {
	pod := rootFSSyncTestPod()
	c := NewRootFSSyncController(newTestPodLister(t, pod), nil, nil)
	var bound *corev1.Pod
	c.bind = func(_ context.Context, candidate *corev1.Pod) error {
		bound = candidate
		return nil
	}

	require.NoError(t, c.reconcile(context.Background(), pod.Namespace+"/"+pod.Name))
	require.NotNil(t, bound)
	assert.Equal(t, pod.UID, bound.UID)
}

func TestRootFSSyncCandidateRequiresActiveRunningProcd(t *testing.T) {
	base := rootFSSyncTestPod()
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "idle", mutate: func(pod *corev1.Pod) { pod.Labels[controller.LabelPoolType] = controller.PoolTypeIdle }},
		{name: "no team", mutate: func(pod *corev1.Pod) { delete(pod.Annotations, controller.AnnotationTeamID) }},
		{name: "not scheduled", mutate: func(pod *corev1.Pod) { pod.Spec.NodeName = "" }},
		{name: "not running", mutate: func(pod *corev1.Pod) { pod.Status.ContainerStatuses[0].State = corev1.ContainerState{} }},
		{name: "no container id", mutate: func(pod *corev1.Pod) { pod.Status.ContainerStatuses[0].ContainerID = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := base.DeepCopy()
			tt.mutate(pod)
			assert.False(t, rootFSSyncCandidate(pod))
		})
	}
	assert.True(t, rootFSSyncCandidate(base))
}

func rootFSSyncTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "sandbox-pod",
			UID:       "pod-uid",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
				controller.LabelPoolType:  controller.PoolTypeActive,
			},
			Annotations: map[string]string{
				controller.AnnotationSandboxID: "sandbox-1",
				controller.AnnotationTeamID:    "team-1",
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:        sandboxRootFSContainerName,
				ContainerID: "containerd://procd-id",
				State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}
