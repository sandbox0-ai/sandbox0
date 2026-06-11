package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodToSandboxDoesNotExposeLegacyPausedAnnotation(t *testing.T) {
	svc := &SandboxService{
		config: SandboxServiceConfig{ProcdPort: 49983},
		logger: zap.NewNop(),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sandbox-1",
			Labels: map[string]string{
				controller.LabelTemplateID:        "t-team-template-1",
				controller.LabelTemplateLogicalID: "template-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:            "team-1",
				controller.AnnotationUserID:            "user-1",
				controller.AnnotationPaused:            "true",
				controller.AnnotationRuntimeGeneration: "9",
			},
			CreationTimestamp: metav1.NewTime(time.Unix(1700000000, 0).UTC()),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
		},
	}

	sandbox := svc.podToSandbox(context.Background(), pod, pod.Name)

	assert.False(t, sandbox.Paused)
	assert.Equal(t, "template-1", sandbox.TemplateID)
	assert.Equal(t, int64(9), sandbox.RuntimeGeneration)
}

type staticTokenGenerator struct{}

func (staticTokenGenerator) GenerateToken(_, _, _ string) (string, error) {
	return "token", nil
}
