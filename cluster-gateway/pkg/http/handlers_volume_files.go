package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// === Volume File System Handlers (→ Storage Proxy) ===

// handleVolumeFileOperation handles volume file operations (GET, POST, DELETE).
// Route: /api/v1/sandboxvolumes/:id/files
func (s *Server) handleVolumeFileOperation(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files"
	s.proxyToStorageProxy(c)
}

// handleVolumeFileWatch handles volume file watch websocket proxying.
// Route: WS /api/v1/sandboxvolumes/:id/files/watch
func (s *Server) handleVolumeFileWatch(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/watch"
	s.proxyToStorageProxy(c)
}

// handleVolumeFileMove handles volume file move operations.
// Route: /api/v1/sandboxvolumes/:id/files/move
func (s *Server) handleVolumeFileMove(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/move"
	s.proxyToStorageProxy(c)
}

// handleVolumeFileClone handles volume file clone operations.
// Route: /api/v1/sandboxvolumes/:id/files/clone
func (s *Server) handleVolumeFileClone(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/clone"
	s.proxyToStorageProxy(c)
}

// handleVolumeFileStat handles volume file stat operations.
// Route: /api/v1/sandboxvolumes/:id/files/stat
func (s *Server) handleVolumeFileStat(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/stat"
	s.proxyToStorageProxy(c)
}

// handleVolumeFileList handles volume directory listing operations.
// Route: /api/v1/sandboxvolumes/:id/files/list
func (s *Server) handleVolumeFileList(c *gin.Context) {
	id, ok := requireVolumeID(c)
	if !ok {
		return
	}
	if c.Query("path") == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	c.Request.URL.Path = "/sandboxvolumes/" + id + "/files/list"
	s.proxyToStorageProxy(c)
}
