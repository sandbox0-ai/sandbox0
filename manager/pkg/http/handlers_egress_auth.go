package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func (s *Server) resolveEgressAuth(c *gin.Context) {
	if s.egressAuthService == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "egress auth runtime resolver is unavailable")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.Caller != "netd" {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "caller is not allowed to resolve egress auth")
		return
	}

	var req egressauth.ResolveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if req.AuthRef == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "authRef is required")
		return
	}

	s.logger.Info("Runtime egress auth resolve request received",
		zap.String("caller", claims.Caller),
		zap.String("sandbox_id", req.SandboxID),
		zap.String("team_id", req.TeamID),
		zap.String("auth_ref", req.AuthRef),
		zap.String("destination", req.Destination),
		zap.String("protocol", req.Protocol),
	)
	resp, err := s.egressAuthService.Resolve(c.Request.Context(), &req)
	if err != nil {
		statusCode, code, message := service.MapEgressAuthResolveError(err)
		spec.JSONError(c, statusCode, code, message)
		return
	}

	spec.JSONSuccess(c, http.StatusOK, resp)
}
