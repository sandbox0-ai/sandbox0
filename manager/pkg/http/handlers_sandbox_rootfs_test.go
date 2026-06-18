package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestRestoreSandboxRootFSRejectsMissingSnapshotID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cases := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{}`},
		{name: "blank", body: `{"snapshot_id":"   "}`},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := &Server{}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-1/rootfs/restore", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
			ctx.Request = request
			ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}

			server.restoreSandboxRootFS(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
}

func TestWriteSandboxRootFSErrorMapsExpiredSnapshotToBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	server.writeSandboxRootFSError(ctx, "create rootfs snapshot", "sandbox-1", service.ErrRootFSSnapshotExpired)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}
