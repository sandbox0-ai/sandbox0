package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type recordingSandboxPauser struct {
	sandboxIDs []string
	err        error
}

func (r *recordingSandboxPauser) PauseSandboxAndWait(_ context.Context, sandboxID string) (*service.PauseSandboxResponse, error) {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
	if r.err != nil {
		return nil, r.err
	}
	return &service.PauseSandboxResponse{SandboxID: sandboxID, Paused: true, Status: managerapi.SandboxStatusPaused}, nil
}

type recordingSandboxResumer struct {
	sandboxIDs []string
}

func (r *recordingSandboxResumer) ResumeSandboxAndWait(_ context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
	return &managerapi.ResumeSandboxResponse{SandboxID: sandboxID, Resumed: true}, nil
}

func TestPauseAndResumeUseSelectedRuntimeBackend(t *testing.T) {
	pauser := &recordingSandboxPauser{}
	resumer := &recordingSandboxResumer{}
	server, pauseContext, pauseRecorder := newSandboxLifecycleHandlerFixture(t, pauser, resumer, http.MethodPost)

	server.pauseSandbox(pauseContext)
	if pauseRecorder.Code != http.StatusOK || len(pauser.sandboxIDs) != 1 || pauser.sandboxIDs[0] != "sandbox-1" {
		t.Fatalf("pause status=%d calls=%v body=%s", pauseRecorder.Code, pauser.sandboxIDs, pauseRecorder.Body.String())
	}

	server, resumeContext, resumeRecorder := newSandboxLifecycleHandlerFixture(t, pauser, resumer, http.MethodPost)
	server.resumeSandbox(resumeContext)
	if resumeRecorder.Code != http.StatusOK || len(resumer.sandboxIDs) != 1 || resumer.sandboxIDs[0] != "sandbox-1" {
		t.Fatalf("resume status=%d calls=%v body=%s", resumeRecorder.Code, resumer.sandboxIDs, resumeRecorder.Body.String())
	}
}

func TestPauseMapsUnavailableRuntimeBackend(t *testing.T) {
	pauser := &recordingSandboxPauser{err: fmt.Errorf("%w: test backend", service.ErrSandboxLifecycleUnavailable)}
	server, ctx, recorder := newSandboxLifecycleHandlerFixture(t, pauser, nil, http.MethodPost)

	server.pauseSandbox(ctx)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want %d; body=%s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
}

func newSandboxLifecycleHandlerFixture(
	t *testing.T,
	pauser service.SandboxPauser,
	resumer service.SandboxResumer,
	method string,
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
	server.sandboxPauser = pauser
	server.sandboxResumer = resumer
	server.logger = zap.NewNop()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}
	request := httptest.NewRequest(method, "/api/v1/sandboxes/sandbox-1/lifecycle", nil)
	ctx.Request = request.WithContext(internalauth.WithClaims(
		request.Context(), &internalauth.Claims{TeamID: "team-1"},
	))
	return server, ctx, recorder
}
