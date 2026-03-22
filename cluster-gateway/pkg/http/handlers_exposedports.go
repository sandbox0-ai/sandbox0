package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// === Exposed Ports Handlers (→ Manager) ===

// getExposedPorts gets the exposed ports for a sandbox
func (s *Server) getExposedPorts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Getting exposed ports",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/exposed-ports"

	s.proxyToManager(c)
}

// updateExposedPorts updates the exposed ports for a sandbox
func (s *Server) updateExposedPorts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Updating exposed ports",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/exposed-ports"

	s.proxyToManager(c)
}

// clearExposedPorts clears all exposed ports for a sandbox
func (s *Server) clearExposedPorts(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.logger.Debug("Clearing exposed ports",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/exposed-ports"

	s.proxyToManager(c)
}

// deleteExposedPort deletes a specific exposed port for a sandbox
func (s *Server) deleteExposedPort(c *gin.Context) {
	sandboxID := c.Param("id")
	port := c.Param("port")
	if sandboxID == "" || port == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id and port are required")
		return
	}

	s.logger.Debug("Deleting exposed port",
		zap.String("sandboxID", sandboxID),
		zap.String("port", port),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/exposed-ports/" + port

	s.proxyToManager(c)
}
