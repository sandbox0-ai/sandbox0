package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type recordingSandboxTerminator struct {
	sandboxIDs []string
}

func (r *recordingSandboxTerminator) TerminateSandbox(_ context.Context, sandboxID string) error {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
	return nil
}

func TestTerminateSandboxUsesSelectedRuntimeBackend(t *testing.T) {
	terminator := &recordingSandboxTerminator{}
	server, ctx, recorder := newTerminateSandboxHandlerFixture(t, terminator)

	server.terminateSandbox(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if len(terminator.sandboxIDs) != 1 || terminator.sandboxIDs[0] != "sandbox-1" {
		t.Fatalf("termination calls = %v", terminator.sandboxIDs)
	}
}

func TestTerminateSandboxFailsClosedWithoutRuntimeBackend(t *testing.T) {
	server, ctx, recorder := newTerminateSandboxHandlerFixture(t, nil)

	server.terminateSandbox(ctx)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
}

func newTerminateSandboxHandlerFixture(
	t *testing.T,
	terminator service.SandboxTerminator,
) (*Server, *gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sandbox-1", Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1", controller.LabelTemplateID: "default",
				controller.LabelPoolType: controller.PoolTypeActive,
			},
			Annotations: map[string]string{controller.AnnotationTeamID: "team-1"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	sandboxService := service.NewSandboxServiceWithDependencies(service.SandboxServiceDependencies{
		PodLister: newHTTPTestPodLister(t, pod), Config: service.SandboxServiceConfig{}, Logger: zap.NewNop(),
	})
	server := newHTTPTestServerWithSandboxService(sandboxService)
	server.sandboxTerminator = terminator
	server.logger = zap.NewNop()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/sandboxes/sandbox-1", nil)
	ctx.Request = request.WithContext(internalauth.WithClaims(
		request.Context(), &internalauth.Claims{TeamID: "team-1"},
	))
	return server, ctx, recorder
}
