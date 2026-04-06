package http

import (
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	registryprovider "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// getRegistryCredentials returns short-lived registry credentials for uploads.
func (s *Server) getRegistryCredentials(c *gin.Context) {
	if s.registry == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "registry provider is not configured")
		return
	}

	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var reqBody struct {
		TargetImage string `json:"targetImage"`
	}
	if err := c.ShouldBindJSON(&reqBody); err != nil && !errors.Is(err, io.EOF) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid registry credentials request")
		return
	}

	creds, err := s.registry.GetPushCredentials(c.Request.Context(), registryprovider.PushCredentialsRequest{
		TeamID:      authCtx.TeamID,
		TargetImage: reqBody.TargetImage,
	})
	if err != nil {
		s.logger.Error("Failed to get registry credentials", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get registry credentials")
		return
	}
	if creds == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "registry provider returned no credentials")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, creds)
}
