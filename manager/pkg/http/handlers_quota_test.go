package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
)

func TestPutTeamQuotaRejectsMissingLimitValue(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{}`},
		{name: "null", body: `{"limit_value":null}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{quotaRepo: &quota.Repository{}, logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodPut, "/api/v1/quotas/active_sandboxes", strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{TeamID: "team-1"}))
			ctx.Request = request
			ctx.Params = gin.Params{{Key: "dimension", Value: string(quota.DimensionActiveSandboxes)}}

			server.putTeamQuota(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
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
