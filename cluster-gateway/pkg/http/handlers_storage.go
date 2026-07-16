package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// === Sandbox Volume Management Handlers (→ Manager Storage) ===

// proxyToManagerStorage forwards a volume request to manager storage.
func (s *Server) proxyToManagerStorage(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)

	internalToken, err := s.internalAuthGen.Generate(internalauth.ServiceManagerStorage, authCtx.TeamID, authCtx.UserID, internalauth.GenerateOptions{})
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager storage",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	// Set headers
	c.Request.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)

	// Forward to manager storage.
	s.proxy2ManagerStorage.ProxyToTarget(c)
}

func requireVolumeID(c *gin.Context) (string, bool) {
	id := c.Param("id")
	if id == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return "", false
	}
	return id, true
}

// createSandboxVolume creates a new sandbox volume
func (s *Server) createSandboxVolume(c *gin.Context) {
	c.Request.URL.Path = "/sandboxvolumes"
	s.proxyToManagerStorage(c)
}

// listSandboxVolumes lists sandbox volumes for the authenticated team
func (s *Server) listSandboxVolumes(c *gin.Context) {
	c.Request.URL.Path = "/sandboxvolumes"
	s.proxyToManagerStorage(c)
}

// getSandboxVolume gets a sandbox volume by ID
func (s *Server) getSandboxVolume(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToManagerStorage(c)
}

// deleteSandboxVolume deletes a sandbox volume
func (s *Server) deleteSandboxVolume(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToManagerStorage(c)
}

// forkSandboxVolume forks a sandbox volume
func (s *Server) forkSandboxVolume(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/fork"
	s.proxyToManagerStorage(c)
}

// createSandboxVolumeSnapshot creates a snapshot of a volume
func (s *Server) createSandboxVolumeSnapshot(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots"
	s.proxyToManagerStorage(c)
}

// listSandboxVolumeSnapshots lists snapshots of a volume
func (s *Server) listSandboxVolumeSnapshots(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots"
	s.proxyToManagerStorage(c)
}

// getSandboxVolumeSnapshot gets a snapshot by ID
func (s *Server) getSandboxVolumeSnapshot(c *gin.Context) {
	id, ok := requireVolumeID(c)
	snapshotID := c.Param("snapshot_id")
	if !ok {
		return
	}
	if snapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id and snapshot id are required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots/" + snapshotID
	s.proxyToManagerStorage(c)
}

// restoreSandboxVolumeSnapshot restores a volume to a snapshot
func (s *Server) restoreSandboxVolumeSnapshot(c *gin.Context) {
	id, ok := requireVolumeID(c)
	snapshotID := c.Param("snapshot_id")
	if !ok {
		return
	}
	if snapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id and snapshot id are required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots/" + snapshotID + "/restore"
	s.proxyToManagerStorage(c)
}

// deleteSandboxVolumeSnapshot deletes a snapshot
func (s *Server) deleteSandboxVolumeSnapshot(c *gin.Context) {
	id, ok := requireVolumeID(c)
	snapshotID := c.Param("snapshot_id")
	if !ok {
		return
	}
	if snapshotID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id and snapshot id are required")
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots/" + snapshotID
	s.proxyToManagerStorage(c)
}
