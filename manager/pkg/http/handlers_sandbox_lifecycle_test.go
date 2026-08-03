package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func TestWriteSandboxLifecycleTransitionErrorMapsMetadataHeadMigrationToConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	server.writeSandboxLifecycleTransitionError(
		ctx,
		"resume",
		"sandbox-1",
		fmt.Errorf("%w: legacy head", service.ErrRootFSHeadMigrationRequired),
	)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Success || response.Error == nil || response.Error.Code != codeRootFSHeadMigrationRequired {
		t.Fatalf("response = %#v, want %s", response, codeRootFSHeadMigrationRequired)
	}
}
