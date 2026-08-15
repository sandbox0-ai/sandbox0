package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func TestRejectSandboxVolumeMutation(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/sandboxvolumes", nil)

	server.rejectSandboxVolumeMutation(ctx)

	if recorder.Code != http.StatusGone {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGone)
	}
	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Success || response.Error == nil || response.Error.Code != spec.CodeGone {
		t.Fatalf("response = %+v, want gone error", response)
	}
}
