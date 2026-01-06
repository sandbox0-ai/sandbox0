package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
)

// === Template Management Handlers (→ Manager) ===

// listTemplates lists available templates
func (s *Server) listTemplates(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	// Forward to manager
	c.Request.URL.Path = "/api/v1/templates"
	s.router_proxy.ProxyToManager()(c)
}

// getTemplate gets a template by ID
func (s *Server) getTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/templates/" + templateID
	s.router_proxy.ProxyToManager()(c)
}

// createTemplate creates a new template
func (s *Server) createTemplate(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/templates"
	s.router_proxy.ProxyToManager()(c)
}

// updateTemplate updates an existing template
func (s *Server) updateTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/templates/" + templateID
	s.router_proxy.ProxyToManager()(c)
}

// deleteTemplate deletes a template
func (s *Server) deleteTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/templates/" + templateID
	s.router_proxy.ProxyToManager()(c)
}

// warmPool warms the pool for a template
func (s *Server) warmPool(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/templates/" + templateID + "/pool/warm"
	s.router_proxy.ProxyToManager()(c)
}
