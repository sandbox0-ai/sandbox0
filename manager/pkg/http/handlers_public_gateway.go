package http

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func (s *Server) getPublicGateway(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Error("Failed to get sandbox public gateway",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}

	cfg := sandbox.PublicGateway
	if cfg == nil {
		cfg = &service.PublicGatewayConfig{}
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"sandbox_id":      sandboxID,
		"public_gateway":  cfg,
		"exposure_domain": s.getExposureDomain(),
	})
}

func (s *Server) updatePublicGateway(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var req service.PublicGatewayConfig
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("invalid request: %v", err))
		return
	}

	sandbox, err := s.sandboxService.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, fmt.Sprintf("sandbox not found: %v", err))
		return
	}
	if sandbox.TeamID != claims.TeamID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "sandbox belongs to a different team")
		return
	}
	if !sandbox.AutoResume && service.PublicGatewayHasResumeRoute(&req) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest,
			"cannot set resume=true on public gateway routes when sandbox auto_resume is disabled")
		return
	}

	updated, err := s.sandboxService.UpdateSandbox(c.Request.Context(), sandboxID, &service.SandboxUpdateConfig{
		PublicGateway: &req,
	})
	if err != nil {
		s.logger.Error("Failed to update sandbox public gateway",
			zap.String("sandboxID", sandboxID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, fmt.Sprintf("failed to update public gateway: %v", err))
		return
	}

	cfg := updated.PublicGateway
	if cfg == nil {
		cfg = &service.PublicGatewayConfig{}
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"sandbox_id":      sandboxID,
		"public_gateway":  cfg,
		"exposure_domain": s.getExposureDomain(),
	})
}
