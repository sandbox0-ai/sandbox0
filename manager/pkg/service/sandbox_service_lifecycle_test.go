package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetSandboxReturnsNotFoundWhenRecordAndPodAreMissing(t *testing.T) {
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(),
		podLister:    newTestPodLister(t),
		sandboxStore: &memorySandboxStore{records: map[string]*SandboxRecord{}},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	sandbox, err := svc.GetSandbox(context.Background(), "missing-sandbox")

	require.Nil(t, sandbox)
	require.Error(t, err)
	require.True(t, k8serrors.IsNotFound(err), "err = %v", err)
}

func TestGetSandboxFallsBackToPodWhenRecordIsMissing(t *testing.T) {
	pod := rootFSTestPod("pod-1", "sandbox-1", "team-1")
	pod.Status.PodIP = "10.0.0.10"
	svc := &SandboxService{
		k8sClient:    fake.NewSimpleClientset(pod),
		podLister:    newTestPodLister(t, pod),
		sandboxStore: &memorySandboxStore{records: map[string]*SandboxRecord{}},
		config:       SandboxServiceConfig{ProcdPort: 49983},
		clock:        systemTime{},
		logger:       zap.NewNop(),
	}

	sandbox, err := svc.GetSandbox(context.Background(), "sandbox-1")

	require.NoError(t, err)
	require.NotNil(t, sandbox)
	assert.Equal(t, "sandbox-1", sandbox.ID)
	assert.Equal(t, "team-1", sandbox.TeamID)
	assert.Equal(t, "pod-1", sandbox.PodName)
}
