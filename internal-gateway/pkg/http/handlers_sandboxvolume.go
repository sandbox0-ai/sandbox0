package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/infra/internal-gateway/pkg/service"
	"go.uber.org/zap"
)

// === SandboxVolume Management Handlers ===
// Most operations proxy to Storage Proxy, except attach/detach which use coordination service

// createSandboxVolume creates a new sandboxvolume
func (s *Server) createSandboxVolume(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	// Forward to storage proxy
	c.Request.URL.Path = "/api/v1/sandboxvolumes"
	s.router_proxy.ProxyToStorageProxy()(c)
}

// listSandboxVolumes lists sandboxvolumes for the team
func (s *Server) listSandboxVolumes(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	// Add team filter to query
	query := c.Request.URL.Query()
	query.Set("team_id", authCtx.TeamID)
	c.Request.URL.RawQuery = query.Encode()

	c.Request.URL.Path = "/api/v1/sandboxvolumes"
	s.router_proxy.ProxyToStorageProxy()(c)
}

// getSandboxVolume gets a sandboxvolume by ID
func (s *Server) getSandboxVolume(c *gin.Context) {
	volumeID := c.Param("id")
	if volumeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandboxvolume_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/sandboxvolumes/" + volumeID
	s.router_proxy.ProxyToStorageProxy()(c)
}

// deleteSandboxVolume deletes a sandboxvolume
func (s *Server) deleteSandboxVolume(c *gin.Context) {
	volumeID := c.Param("id")
	if volumeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandboxvolume_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/sandboxvolumes/" + volumeID
	s.router_proxy.ProxyToStorageProxy()(c)
}

// attachSandboxVolume attaches a sandboxvolume to a sandbox
// This uses the coordination service to:
// 1. Call Storage Proxy to prepare mount (get token)
// 2. Call Procd to mount with token
func (s *Server) attachSandboxVolume(c *gin.Context) {
	volumeID := c.Param("id")
	if volumeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandboxvolume_id is required"})
		return
	}

	var req service.AttachRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// Verify sandbox belongs to team
	authCtx := middleware.GetAuthContext(c)
	sandbox, err := s.repo.GetSandbox(c.Request.Context(), req.SandboxID)
	if err != nil {
		s.logger.Warn("Sandbox not found for attach",
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)
		c.JSON(http.StatusNotFound, gin.H{"error": "sandbox not found"})
		return
	}

	if sandbox.TeamID != authCtx.TeamID {
		c.JSON(http.StatusForbidden, gin.H{"error": "sandbox belongs to a different team"})
		return
	}

	// Use coordination service for attach
	resp, err := s.sandboxVolumeService.Attach(c.Request.Context(), volumeID, &req)
	if err != nil {
		s.logger.Error("Failed to attach sandboxvolume",
			zap.String("sandboxvolume_id", volumeID),
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)

		// Determine appropriate status code
		switch err {
		case service.ErrSandboxNotFound:
			c.JSON(http.StatusNotFound, gin.H{"error": "sandbox not found"})
		case service.ErrVolumeNotFound:
			c.JSON(http.StatusNotFound, gin.H{"error": "sandboxvolume not found"})
		case service.ErrStorageProxyError:
			c.JSON(http.StatusBadGateway, gin.H{"error": "storage service error"})
		case service.ErrMountFailed:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "mount failed"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "attach failed"})
		}
		return
	}

	c.JSON(http.StatusOK, resp)
}

// detachSandboxVolume detaches a sandboxvolume from a sandbox
// This uses the coordination service to:
// 1. Call Procd to unmount first
// 2. Call Storage Proxy to detach record
func (s *Server) detachSandboxVolume(c *gin.Context) {
	volumeID := c.Param("id")
	if volumeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandboxvolume_id is required"})
		return
	}

	var req service.DetachRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// Verify sandbox belongs to team (if it still exists)
	authCtx := middleware.GetAuthContext(c)
	sandbox, err := s.repo.GetSandbox(c.Request.Context(), req.SandboxID)
	if err == nil && sandbox.TeamID != authCtx.TeamID {
		c.JSON(http.StatusForbidden, gin.H{"error": "sandbox belongs to a different team"})
		return
	}

	// Use coordination service for detach
	resp, err := s.sandboxVolumeService.Detach(c.Request.Context(), volumeID, &req)
	if err != nil {
		s.logger.Error("Failed to detach sandboxvolume",
			zap.String("sandboxvolume_id", volumeID),
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)

		switch err {
		case service.ErrStorageProxyError:
			c.JSON(http.StatusBadGateway, gin.H{"error": "storage service error"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		}
		return
	}

	c.JSON(http.StatusOK, resp)
}

// createSnapshot creates a snapshot of a sandboxvolume
func (s *Server) createSnapshot(c *gin.Context) {
	volumeID := c.Param("id")
	if volumeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandboxvolume_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/sandboxvolumes/" + volumeID + "/snapshots"
	s.router_proxy.ProxyToStorageProxy()(c)
}

// restoreSnapshot restores a sandboxvolume from a snapshot
func (s *Server) restoreSnapshot(c *gin.Context) {
	volumeID := c.Param("id")
	if volumeID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandboxvolume_id is required"})
		return
	}

	authCtx := middleware.GetAuthContext(c)
	c.Request.Header.Set("X-Team-ID", authCtx.TeamID)

	c.Request.URL.Path = "/api/v1/sandboxvolumes/" + volumeID + "/restore"
	s.router_proxy.ProxyToStorageProxy()(c)
}
