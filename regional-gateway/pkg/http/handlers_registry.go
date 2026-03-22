package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
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

	creds, err := s.registry.GetPushCredentials(c.Request.Context(), authCtx.TeamID)
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
