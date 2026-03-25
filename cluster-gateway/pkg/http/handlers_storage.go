package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
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

func requireVolumeID(c *gin.Context) (string, bool) {
	id := c.Param("id")
	if id == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return "", false
	}
	return id, true
}

func requireReplicaID(c *gin.Context) (string, bool) {
	replicaID := c.Param("replica_id")
	if replicaID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "replica id is required")
		return "", false
	}
	return replicaID, true
}

func requireConflictID(c *gin.Context) (string, bool) {
	conflictID := c.Param("conflict_id")
	if conflictID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "conflict id is required")
		return "", false
	}
	return conflictID, true
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
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToStorageProxy(c)
}

// deleteSandboxVolume deletes a sandbox volume
func (s *Server) deleteSandboxVolume(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id
	s.proxyToStorageProxy(c)
}

// forkSandboxVolume forks a sandbox volume
func (s *Server) forkSandboxVolume(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/fork"
	s.proxyToStorageProxy(c)
}

// createSandboxVolumeSnapshot creates a snapshot of a volume
func (s *Server) createSandboxVolumeSnapshot(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots"
	s.proxyToStorageProxy(c)
}

// listSandboxVolumeSnapshots lists snapshots of a volume
func (s *Server) listSandboxVolumeSnapshots(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/snapshots"
	s.proxyToStorageProxy(c)
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
	s.proxyToStorageProxy(c)
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
	s.proxyToStorageProxy(c)
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
	s.proxyToStorageProxy(c)
}

func (s *Server) upsertSyncReplica(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	replicaID, ok := requireReplicaID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/replicas/" + replicaID
	s.proxyToStorageProxy(c)
}

func (s *Server) getSyncReplica(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	replicaID, ok := requireReplicaID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/replicas/" + replicaID
	s.proxyToStorageProxy(c)
}

func (s *Server) appendSyncReplicaChanges(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	replicaID, ok := requireReplicaID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/replicas/" + replicaID + "/changes"
	s.proxyToStorageProxy(c)
}

func (s *Server) updateSyncReplicaCursor(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	replicaID, ok := requireReplicaID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/replicas/" + replicaID + "/cursor"
	s.proxyToStorageProxy(c)
}

func (s *Server) createSyncBootstrap(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/bootstrap"
	s.proxyToStorageProxy(c)
}

func (s *Server) downloadSyncBootstrapArchive(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/bootstrap/archive"
	s.proxyToStorageProxy(c)
}

func (s *Server) listSyncChanges(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/changes"
	s.proxyToStorageProxy(c)
}

func (s *Server) listSyncConflicts(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/conflicts"
	s.proxyToStorageProxy(c)
}

func (s *Server) resolveSyncConflict(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	conflictID, ok := requireConflictID(c)
	if !ok {
		return
	}
	c.Request.URL.Path = "/sandboxvolumes/" + id + "/sync/conflicts/" + conflictID
	s.proxyToStorageProxy(c)
}
