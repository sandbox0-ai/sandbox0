package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"go.uber.org/zap"
)

// === Sandbox Volume Management Handlers (→ Storage Proxy) ===

// proxyToStorageProxy proxies a request to storage-proxy with internal authentication
func (s *Server) proxyToStorageProxy(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for storage-proxy
	internalToken, err := s.internalAuthGen.Generate("storage-proxy", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{})
	if err != nil {
		s.logger.Error("Failed to generate internal token for storage-proxy",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal authentication failed"})
		return
	}

	// Set headers
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)
	c.Request.Header.Set("X-Internal-Token", internalToken)

	// Forward to storage-proxy
	s.router_proxy.ProxyToStorageProxy()(c)
}

// createSandboxVolume creates a new sandbox volume
func (s *Server) createSandboxVolume(c *gin.Context) {
	c.Request.URL.Path = "/sandboxvolumes"
	s.proxyToStorageProxy(c)
}

// listSandboxVolumes lists sandbox volumes for the authenticated team
func (s *Server) listSandboxVolumes(c *gin.Context) {
	c.Request.URL.Path = "/sandboxvolumes"
	s.proxyToStorageProxy(c)
}

// getSandboxVolume gets a sandbox volume by ID
func (s *Server) getSandboxVolume(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required"})
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToStorageProxy(c)
}
