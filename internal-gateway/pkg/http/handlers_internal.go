package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// === Internal API Handlers (for scheduler) ===

// getClusterSummary proxies cluster summary request to manager
func (s *Server) getClusterSummary(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for manager
	internalToken, err := s.internalAuthGen.Generate("manager", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	// Set headers
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	c.Request.Header.Set("X-Internal-Token", internalToken)

	// Rewrite path for manager
	c.Request.URL.Path = "/api/v1/cluster/summary"

	// Forward to manager
	s.proxy2Mgr.ProxyToTarget()(c)
}

// getTemplateStats proxies template stats request to manager
func (s *Server) getTemplateStats(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for manager
	internalToken, err := s.internalAuthGen.Generate("manager", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	// Set headers
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	c.Request.Header.Set("X-Internal-Token", internalToken)

	// Rewrite path for manager
	c.Request.URL.Path = "/api/v1/templates/stats"

	// Forward to manager
	s.proxy2Mgr.ProxyToTarget()(c)
}
