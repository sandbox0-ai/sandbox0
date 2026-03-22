package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
)

func (s *Server) proxySandbox(c *gin.Context) {
	if s.schedulerRouter == nil {
		s.proxyToDefaultClusterGateway(c)
		return
	}

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

	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid sandbox_id")
		return
	}

	targetURL, err := s.getClusterGatewayURLForCluster(c.Request.Context(), parsed.ClusterID, authCtx)
	if err != nil || targetURL == "" {
		s.logger.Warn("Failed to resolve sandbox cluster, falling back to scheduler",
			zap.String("sandbox_id", sandboxID),
			zap.String("cluster_id", parsed.ClusterID),
			zap.Error(err),
		)
		s.proxyToScheduler(c, authCtx)
		return
	}

	router, err := s.getClusterGatewayProxy(targetURL)
	if err != nil {
		s.logger.Warn("Failed to create cluster-gateway proxy, falling back to scheduler",
			zap.String("sandbox_id", sandboxID),
			zap.String("cluster_id", parsed.ClusterID),
			zap.String("cluster_gateway_url", targetURL),
			zap.Error(err),
		)
		s.proxyToScheduler(c, authCtx)
		return
	}

	token, err := s.generateInternalToken(c, authCtx, "cluster-gateway")
	if err != nil {
		s.logger.Error("Failed to generate internal token for cluster-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	s.applyInternalHeaders(c, token, authCtx)

	router.ProxyToTarget(c)
}

func (s *Server) proxyToScheduler(c *gin.Context, authCtx *authn.AuthContext) {
	if s.schedulerRouter == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "scheduler not available")
		return
	}

	token, err := s.generateInternalToken(c, authCtx, "scheduler")
	if err != nil {
		s.logger.Error("Failed to generate internal token for scheduler", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	s.applyInternalHeaders(c, token, authCtx)
	s.schedulerRouter.ProxyToTarget(c)
}

func (s *Server) proxyToDefaultClusterGateway(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	token, err := s.generateInternalToken(c, authCtx, "cluster-gateway")
	if err != nil {
		s.logger.Error("Failed to generate internal token for cluster-gateway", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	s.applyInternalHeaders(c, token, authCtx)
	s.clusterGatewayRouter.ProxyToTarget(c)
}

func (s *Server) applyInternalHeaders(c *gin.Context, token string, authCtx *authn.AuthContext) {
	c.Request.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	if authCtx.UserID != "" {
		c.Request.Header.Set("X-User-ID", authCtx.UserID)
	}
	c.Request.Header.Set("X-Auth-Method", string(authCtx.AuthMethod))
}
