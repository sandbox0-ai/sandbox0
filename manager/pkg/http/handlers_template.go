package http

import "github.com/gin-gonic/gin"

// listTemplates lists all templates.
func (s *Server) listTemplates(c *gin.Context) {
	s.templateHandler.ListTemplates(c)
}

// getTemplate gets a template by ID.
func (s *Server) getTemplate(c *gin.Context) {
	s.templateHandler.GetTemplate(c)
}

// createTemplate creates a new template.
func (s *Server) createTemplate(c *gin.Context) {
	s.templateHandler.CreateTemplate(c)
}

// createTemplateFromSandbox creates a template from a sandbox rootfs.
func (s *Server) createTemplateFromSandbox(c *gin.Context) {
	s.templateHandler.CreateTemplateFromSandbox(c)
}

// updateTemplate updates an existing template.
func (s *Server) updateTemplate(c *gin.Context) {
	s.templateHandler.UpdateTemplate(c)
}

// deleteTemplate deletes a template.
func (s *Server) deleteTemplate(c *gin.Context) {
	s.templateHandler.DeleteTemplate(c)
}
