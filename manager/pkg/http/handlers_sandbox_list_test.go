package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
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
