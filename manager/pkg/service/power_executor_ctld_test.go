package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type rewriteTransport struct {
	base   http.RoundTripper
	target *url.URL
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	clone.Host = t.target.Host
	return t.base.RoundTrip(clone)
}

func TestCtldAddressForSandboxUsesNodeInternalIP(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-1", Namespace: "default", Labels: map[string]string{"sandbox0.ai/sandbox-id": "sandbox-1"}},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}},
	}
	svc := &SandboxService{
		k8sClient: fake.NewSimpleClientset(pod, node),
		config:    SandboxServiceConfig{CtldPort: 8095},
		logger:    zap.NewNop(),
	}

	addr, err := svc.ctldAddressForSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, "http://10.0.0.12:8095", addr)
}

func TestNewSandboxServiceUsesCtldExecutorWhenEnabled(t *testing.T) {
	svc := NewSandboxService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, SandboxServiceConfig{CtldEnabled: true}, zap.NewNop(), nil)
	_, ok := svc.powerExecutor.(*ctldSandboxPowerExecutor)
	assert.True(t, ok)
	assert.Equal(t, 8095, svc.config.CtldPort)
}

func TestCtldPowerExecutorCallsCtldPause(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/pause", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"paused":true,"resource_usage":{"container_memory_working_set":456}}`))
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-1", Namespace: "default", Labels: map[string]string{"sandbox0.ai/sandbox-id": "sandbox-1"}},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}},
	}
	transport := &rewriteTransport{base: server.Client().Transport, target: target}

	svc := &SandboxService{
		k8sClient:  fake.NewSimpleClientset(pod, node),
		ctldClient: NewCtldClientWithHTTPClient(&http.Client{Transport: transport}),
		config:     SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095},
		logger:     zap.NewNop(),
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.Equal(t, int64(456), resp.ResourceUsage.ContainerMemoryWorkingSet)
}
