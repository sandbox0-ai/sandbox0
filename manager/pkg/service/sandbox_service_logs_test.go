package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestGetSandboxLogsUsesBoundedPodLogOptions(t *testing.T) {
	pod := newSandboxLogsTestPod("sandbox-1", "team-1", "procd")
	client := fake.NewSimpleClientset(pod)
	svc := &SandboxService{
		k8sClient: client,
		podLister: newTestPodLister(t, pod),
		logger:    zap.NewNop(),
	}
	sinceSeconds := int64(60)

	resp, err := svc.GetSandboxLogs(context.Background(), "sandbox-1", "team-1", &SandboxLogsOptions{
		Container:    "procd",
		TailLines:    25,
		LimitBytes:   1024,
		Previous:     true,
		Timestamps:   true,
		SinceSeconds: &sinceSeconds,
	})
	require.NoError(t, err)
	assert.Equal(t, "sandbox-1", resp.SandboxID)
	assert.Equal(t, "sandbox-1", resp.PodName)
	assert.Equal(t, "procd", resp.Container)
	assert.True(t, resp.Previous)
	assert.Equal(t, "fake logs", resp.Logs)

	logOptions := findPodLogOptions(t, client.Actions())
	require.NotNil(t, logOptions)
	assert.Equal(t, "procd", logOptions.Container)
	assert.True(t, logOptions.Previous)
	assert.True(t, logOptions.Timestamps)
	require.NotNil(t, logOptions.TailLines)
	assert.EqualValues(t, 25, *logOptions.TailLines)
	require.NotNil(t, logOptions.LimitBytes)
	assert.EqualValues(t, 1024, *logOptions.LimitBytes)
	require.NotNil(t, logOptions.SinceSeconds)
	assert.EqualValues(t, 60, *logOptions.SinceSeconds)
}

func TestGetSandboxLogsRejectsDifferentTeam(t *testing.T) {
	pod := newSandboxLogsTestPod("sandbox-1", "team-1", "procd")
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		logger:    zap.NewNop(),
	}

	_, err := svc.GetSandboxLogs(context.Background(), "sandbox-1", "team-2", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSandboxTeamMismatch))
}

func TestGetSandboxLogsRejectsUnknownContainer(t *testing.T) {
	pod := newSandboxLogsTestPod("sandbox-1", "team-1", "procd")
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod),
		podLister: newTestPodLister(t, pod),
		logger:    zap.NewNop(),
	}

	_, err := svc.GetSandboxLogs(context.Background(), "sandbox-1", "team-1", &SandboxLogsOptions{Container: "sidecar"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrSandboxLogContainerNotFound))
}

func newSandboxLogsTestPod(sandboxID, teamID, container string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxID,
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: sandboxID,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID: teamID,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: container}},
		},
	}
}

func findPodLogOptions(t *testing.T, actions []k8stesting.Action) *corev1.PodLogOptions {
	t.Helper()
	for _, action := range actions {
		if !action.Matches("get", "pods/log") {
			continue
		}
		generic, ok := action.(k8stesting.GenericAction)
		require.True(t, ok, "pods/log action should be generic")
		options, ok := generic.GetValue().(*corev1.PodLogOptions)
		require.True(t, ok, "pods/log action value should be PodLogOptions")
		return options
	}
	t.Fatalf("expected pods/log get action")
	return nil
}
