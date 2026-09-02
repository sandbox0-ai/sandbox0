package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"go.uber.org/zap"
)

type recordingSandboxTerminator struct {
	sandboxIDs []string
}

func (r *recordingSandboxTerminator) TerminateSandbox(_ context.Context, sandboxID string) error {
	r.sandboxIDs = append(r.sandboxIDs, sandboxID)
	return nil
}

func TestTerminateSandboxUsesRuntime(t *testing.T) {
	terminator := &recordingSandboxTerminator{}
	server, ctx, recorder := newTerminateSandboxHandlerFixture(t, terminator)

	server.terminateSandbox(ctx)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if len(terminator.sandboxIDs) != 1 || terminator.sandboxIDs[0] != "sandbox-1" {
		t.Fatalf("termination calls = %v", terminator.sandboxIDs)
	}
}

func TestTerminateSandboxFailsClosedWithoutRuntime(t *testing.T) {
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
	server := &Server{sandboxReader: staticSandboxReader{sandbox: &managerapi.Sandbox{
		ID: "sandbox-1", TeamID: "team-1", Status: managerapi.SandboxStatusRunning,
	}}}
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
