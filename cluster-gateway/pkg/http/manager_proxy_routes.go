package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func (s *Server) proxyManagerPath(path string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.URL.Path = path
		s.proxyToManager(c)
	}
}

func (s *Server) proxyManagerPathParam(prefix, paramName, description string) gin.HandlerFunc {
	return func(c *gin.Context) {
		value := c.Param(paramName)
		if value == "" {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, description+" is required")
			return
		}
		c.Request.URL.Path = prefix + value
		s.proxyToManager(c)
	}
}

func (s *Server) proxySandboxManagerSubresource(subresource string) gin.HandlerFunc {
	return func(c *gin.Context) {
		sandboxID := c.Param("id")
		if sandboxID == "" {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
			return
		}
		if s.logger != nil {
			s.logger.Debug("Proxying sandbox subresource to manager",
				zap.String("sandboxID", sandboxID),
				zap.String("subresource", subresource),
			)
		}
		c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/" + subresource
		if c.Request.Method == http.MethodPut && subresource == "services" {
			s.proxyToManagerAndInvalidateSandbox(c, sandboxID)
			return
		}
		s.proxyToManager(c)
	}
}
