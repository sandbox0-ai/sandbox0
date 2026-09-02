package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"go.uber.org/zap"
)

type recordingSandboxPauser struct {
	sandboxIDs []string
	err        error
	response   *service.PauseSandboxResponse
}

func (r *recordingSandboxPauser) PauseSandboxAndWait(_ context.Context, sandboxID string) (*service.PauseSandboxResponse, error) {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
	if r.err != nil {
		return nil, r.err
	}
	if r.response != nil {
		return r.response, nil
	}
	return &service.PauseSandboxResponse{SandboxID: sandboxID, Paused: true, Status: managerapi.SandboxStatusPaused}, nil
}

func TestPauseReturnsAcceptedWhileCheckpointIsPending(t *testing.T) {
	pauser := &recordingSandboxPauser{response: &service.PauseSandboxResponse{
		SandboxID: "sandbox-1", Paused: false, Status: managerapi.SandboxStatusStarting,
	}}
	server, pauseContext, pauseRecorder := newSandboxLifecycleHandlerFixture(t, pauser, nil, http.MethodPost)

	server.pauseSandbox(pauseContext)
	if pauseRecorder.Code != http.StatusAccepted {
		t.Fatalf("pause status=%d, want %d body=%s", pauseRecorder.Code, http.StatusAccepted, pauseRecorder.Body.String())
	}
}

type recordingSandboxResumer struct {
	sandboxIDs []string
}

func (r *recordingSandboxResumer) ResumeSandboxAndWait(_ context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
	return &managerapi.ResumeSandboxResponse{SandboxID: sandboxID, Resumed: true}, nil
}

func TestPauseAndResumeUseRuntime(t *testing.T) {
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

func TestPauseMapsUnavailableRuntime(t *testing.T) {
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
	server := &Server{sandboxReader: staticSandboxReader{sandbox: &managerapi.Sandbox{
		ID: "sandbox-1", TeamID: "team-1", Status: managerapi.SandboxStatusRunning,
	}}}
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
