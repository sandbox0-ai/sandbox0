package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// === Sandbox Management Handlers (→ Manager) ===

// proxyToManager proxies a request to manager with internal authentication
func (s *Server) proxyToManager(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for manager
	internalToken, err := s.internalAuthGen.Generate("manager", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{})
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

	// Forward to manager
	s.router_proxy.ProxyToManager()(c)
}

// createSandbox creates a new sandbox
func (s *Server) createSandbox(c *gin.Context) {
	// Rewrite path for manager
	c.Request.URL.Path = "/api/v1/sandboxes/claim"

	s.proxyToManager(c)
}

// listSandboxes lists sandboxes for the authenticated team
func (s *Server) listSandboxes(c *gin.Context) {
	s.proxyToManager(c)
}

// getSandbox gets a sandbox by ID
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	// Proxy to manager - manager will handle team ownership verification
	s.proxyToManager(c)
}

// getSandboxStatus gets sandbox status
func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/status"

	s.proxyToManager(c)
}

// updateSandbox updates sandbox configuration
func (s *Server) updateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.proxyToManager(c)
}

// deleteSandbox deletes a sandbox
func (s *Server) deleteSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.proxyToManager(c)
}

// pauseSandbox pauses a sandbox
func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.proxyToManager(c)
}

// resumeSandbox resumes a paused sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.proxyToManager(c)
}

// refreshSandbox refreshes sandbox TTL
func (s *Server) refreshSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.proxyToManager(c)
}
