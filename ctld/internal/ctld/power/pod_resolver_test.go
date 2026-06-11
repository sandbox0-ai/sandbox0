package power

import (
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodResolverResolvesSandboxPodWithoutCgroup(t *testing.T) {
	runtimeClass := "gvisor"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			UID:       types.UID("uid-1"),
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "node-a",
			RuntimeClassName: &runtimeClass,
			Containers: []corev1.Container{{
				Name: "procd",
				Ports: []corev1.ContainerPort{{
					Name:          "http",
					ContainerPort: 49984,
				}},
			}},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.12"},
	}
	resolver := NewPodResolver(fake.NewSimpleClientset(pod), "node-a")

	target, err := resolver.Resolve(httptest.NewRequest("GET", "/", nil), "sandbox-1")

	require.NoError(t, err)
	assert.Equal(t, "sandbox-1", target.SandboxID)
	assert.Equal(t, "gvisor", target.Runtime)
	assert.Equal(t, "default", target.PodNamespace)
	assert.Equal(t, "pod-1", target.PodName)
	assert.Equal(t, "uid-1", target.PodUID)
	assert.Equal(t, "10.0.0.12", target.PodIP)
	assert.Equal(t, int32(49984), target.ProcdPort)
}

func TestPodResolverRejectsPodOnDifferentNode(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-1"},
		},
		Spec: corev1.PodSpec{NodeName: "node-b"},
	}
	resolver := NewPodResolver(fake.NewSimpleClientset(pod), "node-a")

	_, err := resolver.Resolve(httptest.NewRequest("GET", "/", nil), "sandbox-1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not node-a")
}
