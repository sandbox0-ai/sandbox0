package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	"go.uber.org/zap"
)

func (s *Server) getSandboxDetail(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	targetURL, err := s.resolveClusterGatewayTarget(c, sandboxID)
	if err != nil {
		s.logger.Warn("Failed to resolve cluster-gateway for sandbox detail",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "cluster gateway unavailable")
		return
	}

	sandbox, statusCode, err := s.getSandboxFromClusterGateway(c.Request.Context(), targetURL, sandboxID)
	if err != nil {
		s.logger.Warn("Failed to fetch sandbox detail from cluster-gateway",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", authCtx.TeamID),
			zap.String("cluster_gateway_url", targetURL),
			zap.Error(err),
		)
		switch statusCode {
		case http.StatusNotFound:
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
		default:
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox unavailable")
		}
		return
	}

	if sandbox.TeamID != authCtx.TeamID {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
		return
	}

	payload := sharedssh.SandboxToAPI(sandbox, sharedssh.BuildConnectionInfo(s.cfg.SSHEndpointHost, s.cfg.SSHEndpointPort, sandbox.ID))
	spec.JSONSuccess(c, http.StatusOK, payload)
}
