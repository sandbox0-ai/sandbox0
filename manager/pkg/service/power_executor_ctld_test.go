package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
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
		config:     SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095, PauseMinCPU: "10m", PauseMemoryBufferRatio: 1.1},
		logger:     zap.NewNop(),
		clock:      systemTime{},
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	resp, err := svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Paused)
	assert.Equal(t, int64(456), resp.ResourceUsage.ContainerMemoryWorkingSet)

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "true", updated.Annotations[controller.AnnotationPaused])
	assert.NotEmpty(t, updated.Annotations[controller.AnnotationPausedState])
	assert.Equal(t, SandboxPowerStatePaused, updated.Annotations[controller.AnnotationPowerStateObserved])
}

func TestCtldPowerExecutorCallsCtldResumeAfterRestoringState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/resume", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resumed":true}`))
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels:    map[string]string{"sandbox0.ai/sandbox-id": "sandbox-1"},
			Annotations: map[string]string{
				controller.AnnotationPaused:      "true",
				controller.AnnotationPausedAt:    time.Now().UTC().Format(time.RFC3339),
				controller.AnnotationPausedState: `{"resources":{"procd":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"200m","memory":"256Mi"}}}}`,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "procd",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("96Mi")},
				},
			}},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}},
	}
	transport := &rewriteTransport{base: server.Client().Transport, target: target}

	svc := &SandboxService{
		k8sClient:  fake.NewSimpleClientset(pod, node),
		ctldClient: NewCtldClientWithHTTPClient(&http.Client{Transport: transport}),
		config:     SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095, PauseMinCPU: "10m", PauseMemoryBufferRatio: 1.1},
		logger:     zap.NewNop(),
		clock:      systemTime{},
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	resp, err := svc.ResumeSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	assert.True(t, resp.Resumed)

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, updated.Annotations[controller.AnnotationPaused])
	assert.Empty(t, updated.Annotations[controller.AnnotationPausedState])
	assert.Equal(t, SandboxPowerStateActive, updated.Annotations[controller.AnnotationPowerStateObserved])
}

func TestCtldPowerExecutorResumeFailureKeepsPausedStateAuthoritative(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/resume", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resumed":false,"error":"thaw failed"}`))
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels:    map[string]string{"sandbox0.ai/sandbox-id": "sandbox-1"},
			Annotations: map[string]string{
				controller.AnnotationPaused:                      "true",
				controller.AnnotationPausedAt:                    time.Now().UTC().Format(time.RFC3339),
				controller.AnnotationPausedState:                 `{"resources":{"procd":{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"200m","memory":"256Mi"}}}}`,
				controller.AnnotationPowerStateDesired:           SandboxPowerStateActive,
				controller.AnnotationPowerStateDesiredGeneration: "4",
				controller.AnnotationPowerStateObserved:          SandboxPowerStatePaused,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "procd",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("96Mi")},
				},
			}},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}},
	}
	transport := &rewriteTransport{base: server.Client().Transport, target: target}

	svc := &SandboxService{
		k8sClient:  fake.NewSimpleClientset(pod, node),
		ctldClient: NewCtldClientWithHTTPClient(&http.Client{Transport: transport}),
		config:     SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095, PauseMinCPU: "10m", PauseMemoryBufferRatio: 1.1},
		logger:     zap.NewNop(),
		clock:      systemTime{},
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	_, err = svc.ResumeSandbox(context.Background(), "sandbox-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ctld resume failed")

	updated, err := svc.k8sClient.CoreV1().Pods("default").Get(context.Background(), "sandbox-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "true", updated.Annotations[controller.AnnotationPaused])
	assert.NotEmpty(t, updated.Annotations[controller.AnnotationPausedState])
	assert.Equal(t, SandboxPowerStatePaused, updated.Annotations[controller.AnnotationPowerStateObserved])
}

func TestCtldPowerExecutorDistributesPauseResizeAcrossContainers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sandboxes/sandbox-1/pause", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"paused":true,"resource_usage":{"container_memory_working_set":524288000}}`))
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	require.NoError(t, err)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox-1", Namespace: "default", Labels: map[string]string{"sandbox0.ai/sandbox-id": "sandbox-1"}},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "procd",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("900m"), corev1.ResourceMemory: resource.MustParse("900Mi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("900m"), corev1.ResourceMemory: resource.MustParse("900Mi")},
					},
				},
				{
					Name: "helper",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("100Mi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("100Mi")},
					},
				},
			},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status:     corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.12"}}},
	}
	client := fake.NewSimpleClientset(pod, node)
	var resizePod *corev1.Pod
	client.PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "resize" {
			return false, nil, nil
		}
		resizePod = action.(ktesting.UpdateAction).GetObject().(*corev1.Pod).DeepCopy()
		return false, nil, nil
	})
	transport := &rewriteTransport{base: server.Client().Transport, target: target}

	svc := &SandboxService{
		k8sClient:  client,
		ctldClient: NewCtldClientWithHTTPClient(&http.Client{Transport: transport}),
		config:     SandboxServiceConfig{CtldEnabled: true, CtldPort: 8095, PauseMinCPU: "10m", PauseMemoryBufferRatio: 1.1},
		logger:     zap.NewNop(),
		clock:      systemTime{},
	}
	svc.SetPowerExecutor(&ctldSandboxPowerExecutor{service: svc})

	_, err = svc.PauseSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, resizePod)
	require.Len(t, resizePod.Spec.Containers, 2)

	assert.Equal(t, mustValue("450Mi"), quantityValue(resizePod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]))
	assert.Equal(t, mustValue("495Mi"), quantityValue(resizePod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]))
	assert.Equal(t, mustMilliValue("9m"), quantityMilliValue(resizePod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU]))
	assert.Equal(t, mustValue("50Mi"), quantityValue(resizePod.Spec.Containers[1].Resources.Requests[corev1.ResourceMemory]))
	assert.Equal(t, mustValue("55Mi"), quantityValue(resizePod.Spec.Containers[1].Resources.Limits[corev1.ResourceMemory]))
	assert.Equal(t, mustMilliValue("1m"), quantityMilliValue(resizePod.Spec.Containers[1].Resources.Limits[corev1.ResourceCPU]))
}

func mustValue(raw string) int64 {
	quantity := resource.MustParse(raw)
	return quantity.Value()
}

func mustMilliValue(raw string) int64 {
	quantity := resource.MustParse(raw)
	return quantity.MilliValue()
}

func quantityValue(quantity resource.Quantity) int64 {
	return quantity.Value()
}

func quantityMilliValue(quantity resource.Quantity) int64 {
	return quantity.MilliValue()
}
