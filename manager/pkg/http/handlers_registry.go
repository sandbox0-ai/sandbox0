package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/service"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// getRegistryCredentials returns short-lived registry credentials for uploads.
func (s *Server) getRegistryCredentials(c *gin.Context) {
	if s.registryService == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "registry provider is not configured")
		return
	}
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	teamID := ""
	if claims != nil {
		teamID = claims.TeamID
	}

	creds, err := s.registryService.GetPushCredentials(c.Request.Context(), teamID)
	if err != nil {
		s.logger.Error("Failed to get registry credentials", zap.Error(err))
		if errors.Is(err, service.ErrRegistryProviderNotConfigured) {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "registry provider is not configured")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get registry credentials")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, creds)
}
