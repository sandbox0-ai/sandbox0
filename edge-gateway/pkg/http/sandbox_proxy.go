package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/auth"
	"github.com/sandbox0-ai/infra/pkg/gateway/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"go.uber.org/zap"
)

func (s *Server) proxySandbox(c *gin.Context) {
	if s.schedulerRouter == nil {
		s.proxyToDefaultInternalGateway(c)
		return
	}

	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authentication"})
		return
	}

	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	parsed, err := naming.ParseSandboxName(sandboxID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid sandbox_id"})
		return
	}

	targetURL, err := s.getInternalGatewayURLForCluster(c.Request.Context(), parsed.ClusterID, authCtx, middleware.GetRequestID(c))
	if err != nil || targetURL == "" {
		s.logger.Warn("Failed to resolve sandbox cluster, falling back to scheduler",
			zap.String("sandbox_id", sandboxID),
			zap.String("cluster_id", parsed.ClusterID),
			zap.Error(err),
		)
		s.proxyToScheduler(c, authCtx)
		return
	}

	router, err := s.getInternalGatewayProxy(targetURL)
	if err != nil {
		s.logger.Warn("Failed to create internal-gateway proxy, falling back to scheduler",
			zap.String("sandbox_id", sandboxID),
			zap.String("cluster_id", parsed.ClusterID),
			zap.String("internal_gateway_url", targetURL),
			zap.Error(err),
		)
		s.proxyToScheduler(c, authCtx)
		return
	}

	token, err := s.generateInternalToken(c, authCtx, "internal-gateway")
	if err != nil {
		s.logger.Error("Failed to generate internal token for internal-gateway", zap.Error(err))
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	s.applyInternalHeaders(c, token, authCtx)

	router.ProxyToTarget(c)
}

func (s *Server) proxyToScheduler(c *gin.Context, authCtx *auth.AuthContext) {
	if s.schedulerRouter == nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "scheduler not available"})
		return
	}

	token, err := s.generateInternalToken(c, authCtx, "scheduler")
	if err != nil {
		s.logger.Error("Failed to generate internal token for scheduler", zap.Error(err))
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	s.applyInternalHeaders(c, token, authCtx)
	s.schedulerRouter.ProxyToTarget(c)
}

func (s *Server) proxyToDefaultInternalGateway(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authentication"})
		return
	}

	token, err := s.generateInternalToken(c, authCtx, "internal-gateway")
	if err != nil {
		s.logger.Error("Failed to generate internal token for internal-gateway", zap.Error(err))
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	s.applyInternalHeaders(c, token, authCtx)
	s.igRouter.ProxyToTarget(c)
}

func (s *Server) applyInternalHeaders(c *gin.Context, token string, authCtx *auth.AuthContext) {
	c.Request.Header.Set(internalauth.DefaultTokenHeader, token)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	if authCtx.UserID != "" {
		c.Request.Header.Set("X-User-ID", authCtx.UserID)
	}
	c.Request.Header.Set("X-Auth-Method", string(authCtx.AuthMethod))
}
