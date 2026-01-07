package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// === Network Policy Handlers (→ Manager) ===

// getNetworkPolicy gets the network policy for a sandbox
func (s *Server) getNetworkPolicy(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.logger.Debug("Updating network policy",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/network"

	s.proxyToManager(c)
}

// === Bandwidth Policy Handlers (→ Manager) ===

// getBandwidthPolicy gets the bandwidth policy for a sandbox
func (s *Server) getBandwidthPolicy(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.logger.Debug("Getting bandwidth policy",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/bandwidth"

	s.proxyToManager(c)
}

// updateBandwidthPolicy updates the bandwidth policy for a sandbox
func (s *Server) updateBandwidthPolicy(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox_id is required"})
		return
	}

	s.logger.Debug("Updating bandwidth policy",
		zap.String("sandboxID", sandboxID),
	)

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/bandwidth"

	s.proxyToManager(c)
}
