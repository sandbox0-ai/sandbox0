package http

import "github.com/gin-gonic/gin"

// === Registry Handlers (→ Manager) ===

// getRegistryCredentials returns short-lived registry credentials for uploads.
func (s *Server) getRegistryCredentials(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/registry/credentials"
	s.proxyToManager(c)
}
