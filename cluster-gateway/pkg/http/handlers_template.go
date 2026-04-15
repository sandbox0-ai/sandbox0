package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// === Template Management Handlers (→ Manager) ===

// listTemplates lists available templates
func (s *Server) listTemplates(c *gin.Context) {
	// Forward to manager
	c.Request.URL.Path = "/api/v1/templates"
	s.proxyTemplateToManager(c)
}

// getTemplate gets a template by ID
func (s *Server) getTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	c.Request.URL.Path = "/api/v1/templates/" + templateID
	s.proxyTemplateToManager(c)
}

// createTemplate creates a new template
func (s *Server) createTemplate(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/templates"
	s.proxyTemplateToManager(c)
}

// updateTemplate updates an existing template
func (s *Server) updateTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	c.Request.URL.Path = "/api/v1/templates/" + templateID
	s.proxyTemplateToManager(c)
}

// deleteTemplate deletes a template
func (s *Server) deleteTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	c.Request.URL.Path = "/api/v1/templates/" + templateID
	s.proxyTemplateToManager(c)
}
