package http

import "github.com/gin-gonic/gin"

func (s *Server) listSandboxServices(c *gin.Context) {
	sandboxID := c.Param("id")
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/services"
	s.proxyToManager(c)
}

func (s *Server) updateSandboxServices(c *gin.Context) {
	sandboxID := c.Param("id")
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/services"
	s.proxyToManager(c)
}
