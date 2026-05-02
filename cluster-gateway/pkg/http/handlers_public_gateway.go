package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

func (s *Server) getPublicGateway(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Getting public gateway",
		zap.String("sandboxID", sandboxID),
	)
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/public-gateway"
	s.proxyToManager(c)
}

func (s *Server) updatePublicGateway(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Updating public gateway",
		zap.String("sandboxID", sandboxID),
	)
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/public-gateway"
	s.proxyToManager(c)
}
