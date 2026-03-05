package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// === Sandbox Management Handlers (→ Manager) ===

// proxyToManager proxies a request to manager with internal authentication
func (s *Server) proxyToManager(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	// Generate internal token for manager
	internalToken, err := s.internalAuthGen.Generate("manager", authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{})
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	// Set headers
	c.Request.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)

	// Forward to manager
	s.proxy2Mgr.ProxyToTarget(c)
}

// createSandbox creates a new sandbox
func (s *Server) createSandbox(c *gin.Context) {
	// Rewrite path for manager
	c.Request.URL.Path = "/api/v1/sandboxes"

	s.proxyToManager(c)
}

// listSandboxes lists all sandboxes for the authenticated team
func (s *Server) listSandboxes(c *gin.Context) {
	s.proxyToManager(c)
}

// getSandbox gets a sandbox by ID
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Proxy to manager - manager will handle team ownership verification
	s.proxyToManager(c)
}

// getSandboxStatus gets sandbox status
func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/status"

	s.proxyToManager(c)
}

// updateSandbox updates sandbox configuration
func (s *Server) updateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)

	// Invalidate cache after update to ensure fresh data on next access
	s.sandboxAddrCache.Delete(sandboxID)
	s.logger.Debug("Invalidated sandbox cache after update",
		zap.String("sandbox_id", sandboxID),
	)
}

// deleteSandbox deletes a sandbox
func (s *Server) deleteSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)

	// Invalidate cache after deletion
	s.sandboxAddrCache.Delete(sandboxID)
	s.logger.Debug("Invalidated sandbox cache after deletion",
		zap.String("sandbox_id", sandboxID),
	)
}

// pauseSandbox pauses a sandbox
func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)

	// Invalidate cache after state change
	s.sandboxAddrCache.Delete(sandboxID)
	s.logger.Debug("Invalidated sandbox cache after pause",
		zap.String("sandbox_id", sandboxID),
	)
}

// resumeSandbox resumes a paused sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)

	// Invalidate cache after state change
	s.sandboxAddrCache.Delete(sandboxID)
	s.logger.Debug("Invalidated sandbox cache after resume",
		zap.String("sandbox_id", sandboxID),
	)
}

// refreshSandbox refreshes sandbox TTL
func (s *Server) refreshSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)
}

// === Sandbox Volume Mount Handlers (→ Procd) ===

// mountSandboxVolume mounts a volume in the sandbox
func (s *Server) mountSandboxVolume(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/sandboxvolumes/mount"
	s.proxyToProcd(c, procdURL)
}

// unmountSandboxVolume unmounts a volume from the sandbox
func (s *Server) unmountSandboxVolume(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/sandboxvolumes/unmount"
	s.proxyToProcd(c, procdURL)
}

// getSandboxVolumeStatus gets the status of mounted volumes
func (s *Server) getSandboxVolumeStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/sandboxvolumes/status"
	s.proxyToProcd(c, procdURL)
}
