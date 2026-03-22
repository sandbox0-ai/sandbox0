package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// === Network Policy Handlers (→ Manager) ===

// getNetworkPolicy gets the network policy for a sandbox
func (s *Server) getNetworkPolicy(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Getting network policy",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/network"

	s.proxyToManager(c)
}

// updateNetworkPolicy updates the network policy for a sandbox
func (s *Server) updateNetworkPolicy(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Updating network policy",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/network"

	s.proxyToManager(c)
}
