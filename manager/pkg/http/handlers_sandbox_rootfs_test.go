package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
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

func TestWriteSandboxRootFSErrorMapsTeamQuotaFailures(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantRetry  string
	}{
		{
			name: "capacity exceeded",
			err: &teamquota.ExceededError{
				Key:       teamquota.KeyRootFSStorageBytes,
				Requested: 1,
				Limit:     10,
			},
			wantStatus: http.StatusTooManyRequests,
		},
		{
			name: "admission unavailable",
			err: &teamquota.UnavailableError{
				Operation: "publish rootfs object",
				Err:       context.DeadlineExceeded,
			},
			wantStatus: http.StatusServiceUnavailable,
			wantRetry:  "1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)

			server.writeSandboxRootFSError(
				ctx,
				"create rootfs snapshot",
				"sandbox-1",
				test.err,
			)

			if recorder.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d: %s",
					recorder.Code,
					test.wantStatus,
					recorder.Body.String(),
				)
			}
			if got := recorder.Header().Get("Retry-After"); got != test.wantRetry {
				t.Fatalf("Retry-After = %q, want %q", got, test.wantRetry)
			}
		})
	}
}
