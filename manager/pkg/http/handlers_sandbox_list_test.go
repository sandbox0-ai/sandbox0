package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestListSandboxesReturnsOK(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sandbox-1",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Labels: map[string]string{
				controller.LabelTemplateID: "default",
				controller.LabelPoolType:   controller.PoolTypeActive,
				controller.LabelSandboxID:  "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID:    "team-1",
				controller.AnnotationExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339),
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	sandboxService := service.NewSandboxService(
		fake.NewSimpleClientset(pod),
		newHTTPTestPodLister(t, pod),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		service.SandboxServiceConfig{},
		zap.NewNop(),
		nil,
	)

	server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil)
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
	ctx.Request = request

	server.listSandboxes(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !response.Success {
		t.Fatalf("success = false, want true")
	}

	data, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatalf("marshal response data: %v", err)
	}

	var payload service.ListSandboxesResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Count != 1 {
		t.Fatalf("count = %d, want 1", payload.Count)
	}
	if len(payload.Sandboxes) != 1 || payload.Sandboxes[0].ID != "sandbox-1" {
		t.Fatalf("unexpected sandboxes payload: %+v", payload.Sandboxes)
	}
}

func TestListSandboxesRejectsNegativeOffset(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes?offset=-1", nil)
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
	ctx.Request = request

	server.listSandboxes(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Success || response.Error == nil || response.Error.Code != spec.CodeBadRequest {
		t.Fatalf("response = %+v, want bad_request error", response)
	}
}

func TestListSandboxesRejectsInvalidStatusAndLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		path string
	}{
		{name: "invalid status", path: "/api/v1/sandboxes?status=not-a-status"},
		{name: "zero limit", path: "/api/v1/sandboxes?limit=0"},
		{name: "negative limit", path: "/api/v1/sandboxes?limit=-1"},
		{name: "limit above maximum", path: "/api/v1/sandboxes?limit=201"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodGet, tt.path, nil)
			request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
			ctx.Request = request

			server.listSandboxes(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}

			var response spec.Response
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if response.Success || response.Error == nil || response.Error.Code != spec.CodeBadRequest {
				t.Fatalf("response = %+v, want bad_request error", response)
			}
		})
	}
}

func TestClaimSandboxReturnsUnavailableWhenDataPlaneNotReady(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withHTTPTestManagerConfig(t, `sandbox_pod_placement:
  node_selector:
    sandbox0.ai/data-plane-ready: "true"
`)

	templateNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	if err != nil {
		t.Fatalf("template namespace: %v", err)
	}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: templateNamespace},
		Spec:       v1alpha1.SandboxTemplateSpec{MainContainer: v1alpha1.ContainerSpec{Image: "busybox"}},
	}
	sandboxService := service.NewSandboxService(
		fake.NewSimpleClientset(),
		newHTTPTestPodLister(t),
		newHTTPTestNodeLister(t),
		nil,
		nil,
		staticHTTPTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		nil,
		nil,
		nil,
		nil,
		service.SandboxServiceConfig{},
		zap.NewNop(),
		nil,
	)

	server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", strings.NewReader(`{"template":"default"}`))
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	ctx.Request = request

	server.claimSandbox(ctx)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

func TestClaimSandboxReturnsTooManyRequestsWhenClaimStartThrottled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	withHTTPTestPublicKey(t)
	withHTTPTestManagerConfig(t, `sandbox_pod_placement:
  node_selector:
    sandbox0.ai/data-plane-ready: "true"
`)

	templateNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	if err != nil {
		t.Fatalf("template namespace: %v", err)
	}
	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: templateNamespace},
		Spec:       v1alpha1.SandboxTemplateSpec{MainContainer: v1alpha1.ContainerSpec{Image: "busybox"}},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sandbox-node",
			Labels: map[string]string{
				"sandbox0.ai/data-plane-ready": "true",
			},
		},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:   corev1.NodeReady,
			Status: corev1.ConditionTrue,
		}}},
	}
	startingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-starting",
			Namespace: templateNamespace,
			Labels: map[string]string{
				controller.LabelTemplateID: template.Name,
				controller.LabelPoolType:   controller.PoolTypeActive,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	k8sClient := fake.NewSimpleClientset(node, startingPod)
	claimStartLimiter, err := startlimiter.New(context.Background(), startlimiter.Config{
		K8sClient:      k8sClient,
		PerSandboxNode: 1,
		MaxLimit:       1,
		SandboxNodeSelector: map[string]string{
			"sandbox0.ai/data-plane-ready": "true",
		},
	})
	if err != nil {
		t.Fatalf("create claim start limiter: %v", err)
	}
	sandboxService := service.NewSandboxService(
		k8sClient,
		newHTTPTestPodLister(t, startingPod),
		newHTTPTestNodeLister(t, node),
		nil,
		newHTTPTestSecretLister(t),
		staticHTTPTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		nil,
		nil,
		nil,
		nil,
		service.SandboxServiceConfig{},
		zap.NewNop(),
		nil,
	)
	sandboxService.SetClaimStartLimiter(claimStartLimiter)
	sandboxService.SetTeamQuotaStore(&permissiveTeamQuotaCapacityStore{})
	sandboxService.SetTeamQuotaRateLimiter(permissiveTeamQuotaRateLimiter{})

	server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", strings.NewReader(`{"template":"default"}`))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	ctx.Request = request

	server.claimSandbox(ctx)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusTooManyRequests, recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Success || response.Error == nil || response.Error.Code != spec.CodeClaimStartThrottled {
		t.Fatalf("response = %+v, want claim_start_throttled error", response)
	}
}

func TestClaimSandboxReturnsNotFoundForMissingTemplate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sandboxService := service.NewSandboxService(
		fake.NewSimpleClientset(),
		newHTTPTestPodLister(t),
		nil,
		nil,
		nil,
		staticHTTPTemplateLister{},
		nil,
		nil,
		nil,
		nil,
		service.SandboxServiceConfig{},
		zap.NewNop(),
		nil,
	)

	server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", strings.NewReader(`{"template":"missing"}`))
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
	ctx.Request = request

	server.claimSandbox(ctx)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}

	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Success || response.Error == nil || response.Error.Code != spec.CodeNotFound {
		t.Fatalf("response = %+v, want not_found error", response)
	}
}

func TestRefreshSandboxRejectsMalformedJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		body string
	}{
		{name: "wrong duration type", body: `{"duration":"soon"}`},
		{name: "malformed json", body: `{`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sandbox-1",
					Namespace: "default",
					Labels: map[string]string{
						controller.LabelSandboxID: "sandbox-1",
						controller.LabelPoolType:  controller.PoolTypeActive,
					},
					Annotations: map[string]string{
						controller.AnnotationTeamID: "team-1",
					},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.10"},
			}
			sandboxService := service.NewSandboxService(
				fake.NewSimpleClientset(pod),
				newHTTPTestPodLister(t, pod),
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				service.SandboxServiceConfig{},
				zap.NewNop(),
				nil,
			)

			server := &Server{sandboxService: sandboxService, logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/refresh", strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
			ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}
			ctx.Request = request

			server.refreshSandbox(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}

			var response spec.Response
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if response.Success || response.Error == nil || response.Error.Code != spec.CodeBadRequest {
				t.Fatalf("response = %+v, want bad_request error", response)
			}
		})
	}
}

func newHTTPTestPodLister(t *testing.T, pods ...*corev1.Pod) corelisters.PodLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, pod := range pods {
		if err := indexer.Add(pod); err != nil {
			t.Fatalf("add pod: %v", err)
		}
	}
	return corelisters.NewPodLister(indexer)
}

func newHTTPTestNodeLister(t *testing.T, nodes ...*corev1.Node) corelisters.NodeLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, node := range nodes {
		if err := indexer.Add(node); err != nil {
			t.Fatalf("add node: %v", err)
		}
	}
	return corelisters.NewNodeLister(indexer)
}

func newHTTPTestSecretLister(t *testing.T, secrets ...*corev1.Secret) corelisters.SecretLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, secret := range secrets {
		if err := indexer.Add(secret); err != nil {
			t.Fatalf("add secret: %v", err)
		}
	}
	return corelisters.NewSecretLister(indexer)
}

type staticHTTPTemplateLister struct {
	templates []*v1alpha1.SandboxTemplate
}

func (l staticHTTPTemplateLister) List() ([]*v1alpha1.SandboxTemplate, error) {
	return l.templates, nil
}

func (l staticHTTPTemplateLister) Get(namespace, name string) (*v1alpha1.SandboxTemplate, error) {
	for _, template := range l.templates {
		if template.Namespace == namespace && template.Name == name {
			return template, nil
		}
	}
	return nil, apierrors.NewNotFound(v1alpha1.Resource("sandboxtemplate"), name)
}

func withHTTPTestManagerConfig(t *testing.T, content string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manager-config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}
	previousPath, hadPath := os.LookupEnv("CONFIG_PATH")
	if err := os.Setenv("CONFIG_PATH", path); err != nil {
		t.Fatalf("set CONFIG_PATH: %v", err)
	}
	t.Cleanup(func() {
		if hadPath {
			_ = os.Setenv("CONFIG_PATH", previousPath)
			return
		}
		_ = os.Unsetenv("CONFIG_PATH")
	})
}

func withHTTPTestPublicKey(t *testing.T) {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "internal_jwt_public.key")
	if err := os.WriteFile(keyPath, []byte("test-public-key"), 0o600); err != nil {
		t.Fatalf("write public key: %v", err)
	}
	previousPath := internalauth.DefaultInternalJWTPublicKeyPath
	internalauth.DefaultInternalJWTPublicKeyPath = keyPath
	t.Cleanup(func() {
		internalauth.DefaultInternalJWTPublicKeyPath = previousPath
	})
}
