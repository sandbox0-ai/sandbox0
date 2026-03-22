package http

import "github.com/gin-gonic/gin"

// === Credential Source Handlers (→ Manager) ===

func (s *Server) listCredentialSources(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/credential-sources"
	s.proxyToManager(c)
}

func (s *Server) createCredentialSource(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/credential-sources"
	s.proxyToManager(c)
}

func (s *Server) getCredentialSource(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/credential-sources/" + c.Param("name")
	s.proxyToManager(c)
}

func (s *Server) updateCredentialSource(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/credential-sources/" + c.Param("name")
	s.proxyToManager(c)
}

func (s *Server) deleteCredentialSource(c *gin.Context) {
	c.Request.URL.Path = "/api/v1/credential-sources/" + c.Param("name")
	s.proxyToManager(c)
}
