package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"go.uber.org/zap"
)

// === Sandbox Management Handlers (→ Manager) ===

// createSandbox creates a new sandbox
func (s *Server) createSandbox(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	// Forward to manager with team context
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	// Rewrite path for manager
	c.Request.URL.Path = "/api/v1/sandboxes/claim"

	s.router_proxy.ProxyToManager()(c)
}

// listSandboxes lists sandboxes for the authenticated team
func (s *Server) listSandboxes(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	s.router_proxy.ProxyToManager()(c)
}

// getSandbox gets a sandbox by ID
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	// Verify the sandbox belongs to the team
	authCtx := middleware.GetAuthContext(c)
	sandbox, err := s.repo.GetSandbox(c.Request.Context(), sandboxID)
	if err != nil {
		s.logger.Warn("Sandbox not found",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{"error": "sandbox not found"})
		return
	}

	// Check team ownership
	if sandbox.TeamID != authCtx.TeamID {
		c.JSON(http.StatusForbidden, gin.H{"error": "sandbox belongs to a different team"})
		return
	}

	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	s.router_proxy.ProxyToManager()(c)
}

// getSandboxStatus gets sandbox status
func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/status"

	s.router_proxy.ProxyToManager()(c)
}

// updateSandbox updates sandbox configuration
func (s *Server) updateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	s.router_proxy.ProxyToManager()(c)
}

// deleteSandbox deletes a sandbox
func (s *Server) deleteSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	s.router_proxy.ProxyToManager()(c)
}

// pauseSandbox pauses a sandbox
func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	s.router_proxy.ProxyToManager()(c)
}

// resumeSandbox resumes a paused sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	s.router_proxy.ProxyToManager()(c)
}

// refreshSandbox refreshes sandbox TTL
func (s *Server) refreshSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	s.router_proxy.ProxyToManager()(c)
}
