package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func TestUpdateNetworkPolicyRejectsInvalidMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		body string
	}{
		{name: "missing mode", body: `{"egress":{"trafficRules":[{"name":"allow-https","action":"allow"}]}}`},
		{name: "unsupported mode", body: `{"mode":"not-a-mode"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{logger: zap.NewNop()}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodPut, "/api/v1/sandboxes/sandbox-1/network", strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			ctx.Request = request
			ctx.Params = gin.Params{{Key: "id", Value: "sandbox-1"}}

			server.updateNetworkPolicy(ctx)

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
