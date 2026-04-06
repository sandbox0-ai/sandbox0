package http

import (
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type registryCredentialsRequest struct {
	TargetImage string `json:"targetImage"`
}

// getRegistryCredentials returns short-lived registry credentials for uploads.
func (s *Server) getRegistryCredentials(c *gin.Context) {
	var reqBody registryCredentialsRequest
	if err := c.ShouldBindJSON(&reqBody); err != nil && !errors.Is(err, io.EOF) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid registry credentials request")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	teamID := ""
	if claims != nil {
		teamID = claims.TeamID
	}

	creds, err := s.registryService.GetPushCredentials(c.Request.Context(), registry.PushCredentialsRequest{
		TeamID:      teamID,
		TargetImage: reqBody.TargetImage,
	})
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
