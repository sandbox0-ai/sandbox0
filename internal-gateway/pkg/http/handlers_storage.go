package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
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
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	// Set headers
	c.Request.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)

	// Forward to storage-proxy
	s.proxy2sp.ProxyToTarget(c)
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
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToStorageProxy(c)
}

// deleteSandboxVolume deletes a sandbox volume
func (s *Server) deleteSandboxVolume(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToStorageProxy(c)
}

// forkSandboxVolume forks a sandbox volume
func (s *Server) forkSandboxVolume(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "id is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/fork"
	s.proxyToStorageProxy(c)
}

// createSandboxVolumeSnapshot creates a snapshot of a volume
func (s *Server) createSandboxVolumeSnapshot(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots"
	s.proxyToStorageProxy(c)
}

// listSandboxVolumeSnapshots lists snapshots of a volume
func (s *Server) listSandboxVolumeSnapshots(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots"
	s.proxyToStorageProxy(c)
}

// getSandboxVolumeSnapshot gets a snapshot by ID
func (s *Server) getSandboxVolumeSnapshot(c *gin.Context) {
	id := c.Param("id")
	snapshotID := c.Param("snapshot_id")
	if id == "" || snapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id and snapshot id are required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots/" + snapshotID
	s.proxyToStorageProxy(c)
}

// restoreSandboxVolumeSnapshot restores a volume to a snapshot
func (s *Server) restoreSandboxVolumeSnapshot(c *gin.Context) {
	id := c.Param("id")
	snapshotID := c.Param("snapshot_id")
	if id == "" || snapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id and snapshot id are required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots/" + snapshotID + "/restore"
	s.proxyToStorageProxy(c)
}

// deleteSandboxVolumeSnapshot deletes a snapshot
func (s *Server) deleteSandboxVolumeSnapshot(c *gin.Context) {
	id := c.Param("id")
	snapshotID := c.Param("snapshot_id")
	if id == "" || snapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id and snapshot id are required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots/" + snapshotID
	s.proxyToStorageProxy(c)
}
